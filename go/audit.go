package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/rs/zerolog"
	kafka_go "github.com/segmentio/kafka-go"
)

// AuditEvent represents an audit event to be published
type AuditEvent struct {
	Timestamp     time.Time              `json:"timestamp"`
	UserID        string                 `json:"user_id"`
	SessionID     string                 `json:"session_id,omitempty"`
	SourceService string                 `json:"source_service"`
	Action        string                 `json:"action"`
	Details       map[string]interface{} `json:"details"`
	Result        Result                 `json:"result"` // success, failed, pending
	IP            string                 `json:"ip,omitempty"`
	Server        string                 `json:"server,omitempty"`
	TraceID       string                 `json:"trace_id,omitempty"`
}

type Result string

const (
	Success Result = "success"
	Failed  Result = "failed"
	Pending Result = "pending"
)

// Emitter handles publishing audit events to Kafka
type Emitter struct {
	brokers       []string
	topic         string
	sourceService string
	logger        zerolog.Logger
	writer        *kafka_go.Writer
	buffer        chan AuditEvent
	batchSize     int
	flushInterval time.Duration
	done          chan struct{}
	ctx           context.Context
	cancel        context.CancelFunc
}

// EmitterConfig holds configuration for the emitter
type EmitterConfig struct {
	BatchSize     int           // Maximum number of events to batch (default: 100)
	FlushInterval time.Duration // How often to flush the buffer (default: 500ms)
	BufferSize    int           // Size of the internal buffer channel (default: 1000)
}

// DefaultEmitterConfig returns default configuration
func DefaultEmitterConfig() EmitterConfig {
	return EmitterConfig{
		BatchSize:     100,
		FlushInterval: 500 * time.Millisecond,
		BufferSize:    1000,
	}
}

// recoverPanic is a reusable panic recovery function that logs stack traces
// Usage: defer recoverPanic(logger, "operation_name")
func recoverPanic(logger zerolog.Logger, operation string) {
	if r := recover(); r != nil {
		stack := debug.Stack()
		logger.Error().
			Str("operation", operation).
			Str("panic", fmt.Sprintf("%v", r)).
			Str("stack_trace", string(stack)).
			Msg("Panic recovered")
	}
}

// NewEmitter creates a new audit event emitter with default config
func NewEmitter(brokers []string, topic string, sourceService string, logger zerolog.Logger) *Emitter {
	return NewEmitterWithConfig(brokers, topic, sourceService, logger, DefaultEmitterConfig())
}

// NewEmitterWithConfig creates a new audit event emitter with custom config
func NewEmitterWithConfig(brokers []string, topic string, sourceService string, logger zerolog.Logger, config EmitterConfig) *Emitter {
	writer := &kafka_go.Writer{
		Addr:         kafka_go.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka_go.LeastBytes{},
		RequiredAcks: kafka_go.RequireAll,
		Async:        false,
		WriteTimeout: 10 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())

	emitter := &Emitter{
		brokers:       brokers,
		topic:         topic,
		sourceService: sourceService,
		logger:        logger,
		writer:        writer,
		buffer:        make(chan AuditEvent, config.BufferSize),
		batchSize:     config.BatchSize,
		flushInterval: config.FlushInterval,
		done:          make(chan struct{}),
		ctx:           ctx,
		cancel:        cancel,
	}

	// Start background worker
	go emitter.startBatchWorker()

	return emitter
}

// startBatchWorker runs in the background and batches events
func (e *Emitter) startBatchWorker() {
	defer recoverPanic(e.logger, "startBatchWorker")
	defer close(e.done)

	ticker := time.NewTicker(e.flushInterval)
	defer ticker.Stop()

	batch := make([]AuditEvent, 0, e.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		e.logger.Debug().
			Int("batch_size", len(batch)).
			Msg("Flushing batch")

		if err := e.EmitBatch(e.ctx, batch); err != nil {
			e.logger.Error().
				Err(err).
				Int("batch_size", len(batch)).
				Msg("Failed to flush batch")
		}

		// Clear batch
		batch = batch[:0]
	}

	for {
		select {
		case <-e.ctx.Done():
			// Flush remaining events before shutdown
			flush()
			return

		case event := <-e.buffer:
			batch = append(batch, event)

			// Flush if batch is full
			if len(batch) >= e.batchSize {
				flush()
			}

		case <-ticker.C:
			// Periodic flush
			flush()
		}
	}
}

// Close closes the underlying Kafka writer and stops the background worker
func (e *Emitter) Close() error {
	// Signal shutdown
	e.cancel()

	// Wait for worker to finish
	<-e.done

	// Close writer
	return e.writer.Close()
}

// Emit publishes an audit event to Kafka synchronously
func (e *Emitter) Emit(ctx context.Context, event AuditEvent) error {
	// Log transaction start
	e.logger.Debug().
		Str("action", event.Action).
		Str("user_id", event.UserID).
		Str("source_service", event.SourceService).
		Str("result", string(event.Result)).
		Msg("Starting audit transaction write")

	// Set timestamp if not provided
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Marshal event to JSON
	data, err := json.Marshal(event)
	if err != nil {
		e.logger.Error().
			Err(err).
			Str("action", event.Action).
			Str("user_id", event.UserID).
			Msg("Failed to marshal audit event")
		return err
	}

	e.logger.Debug().
		Int("payload_size", len(data)).
		Str("action", event.Action).
		Msg("Audit event marshaled successfully")

	// Write message
	msg := kafka_go.Message{
		Value: data,
		Time:  event.Timestamp,
		Headers: []kafka_go.Header{
			{Key: "source_service", Value: []byte(event.SourceService)},
			{Key: "action", Value: []byte(event.Action)},
		},
	}

	e.logger.Debug().
		Str("topic", e.topic).
		Str("brokers", fmt.Sprintf("%v", e.brokers)).
		Msg("Writing message to Kafka")

	if err := e.writer.WriteMessages(ctx, msg); err != nil {
		e.logger.Error().
			Err(err).
			Str("topic", e.topic).
			Str("source_service", event.SourceService).
			Str("action", event.Action).
			Str("user_id", event.UserID).
			Msg("Failed to emit audit event to Kafka")
		return err
	}

	e.logger.Debug().
		Str("topic", e.topic).
		Str("user_id", event.UserID).
		Str("action", event.Action).
		Str("source_service", event.SourceService).
		Str("result", string(event.Result)).
		Msg("Audit transaction written successfully")

	return nil
}

// EmitBatch publishes multiple audit events to Kafka in a single batch
func (e *Emitter) EmitBatch(ctx context.Context, events []AuditEvent) error {
	if len(events) == 0 {
		return nil
	}

	msgs := make([]kafka_go.Message, 0, len(events))
	for _, event := range events {
		// Set timestamp if not provided
		if event.Timestamp.IsZero() {
			event.Timestamp = time.Now()
		}

		// Marshal event to JSON
		data, err := json.Marshal(event)
		if err != nil {
			e.logger.Error().
				Err(err).
				Str("action", event.Action).
				Str("user_id", event.UserID).
				Msg("Failed to marshal audit event in batch")
			continue // Skip invalid events but try to send the rest
		}

		msgs = append(msgs, kafka_go.Message{
			Value: data,
			Time:  event.Timestamp,
			Headers: []kafka_go.Header{
				{Key: "source_service", Value: []byte(event.SourceService)},
				{Key: "action", Value: []byte(event.Action)},
			},
		})
	}

	if len(msgs) == 0 {
		return nil
	}

	e.logger.Debug().
		Str("topic", e.topic).
		Int("batch_size", len(msgs)).
		Msg("Writing batch messages to Kafka")

	if err := e.writer.WriteMessages(ctx, msgs...); err != nil {
		e.logger.Error().
			Err(err).
			Str("topic", e.topic).
			Int("batch_size", len(msgs)).
			Msg("Failed to emit batch audit events to Kafka")
		return err
	}

	e.logger.Debug().
		Str("topic", e.topic).
		Int("batch_size", len(msgs)).
		Msg("Batch audit transaction written successfully")

	return nil
}

// EmitSync is an alias for Emit (for backward compatibility)
func (e *Emitter) EmitSync(ctx context.Context, event AuditEvent) error {
	return e.Emit(ctx, event)
}

// EmitAsync publishes an audit event asynchronously using the buffer
// Events are batched and sent periodically or when the batch is full
func (e *Emitter) EmitAsync(ctx context.Context, event AuditEvent) {
	// Set timestamp if not provided
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	select {
	case e.buffer <- event:
		e.logger.Debug().
			Str("action", event.Action).
			Str("user_id", event.UserID).
			Msg("Event added to buffer")
	default:
		// Buffer is full, log warning
		e.logger.Warn().
			Str("action", event.Action).
			Str("user_id", event.UserID).
			Msg("Buffer is full, event dropped")
	}
}
