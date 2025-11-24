package _go

import (
	"context"
	"encoding/json"
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
}

// NewEmitter creates a new audit event emitter
func NewEmitter(brokers []string, topic string, sourceService string, logger zerolog.Logger) *Emitter {
	return &Emitter{
		brokers:       brokers,
		topic:         topic,
		sourceService: sourceService,
		logger:        logger,
	}
}

// Emit publishes an audit event to Kafka
func (e *Emitter) Emit(ctx context.Context, event AuditEvent) error {
	// Set timestamp if not provided
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Marshal event to JSON
	data, err := json.Marshal(event)
	if err != nil {
		e.logger.Error().Err(err).Msg("Failed to marshal audit event")
		return err
	}

	// Create Kafka writer
	writer := &kafka_go.Writer{
		Addr:         kafka_go.TCP(e.brokers...),
		Topic:        e.topic,
		Balancer:     &kafka_go.LeastBytes{},
		RequiredAcks: kafka_go.RequireAll,
		Async:        false,
		WriteTimeout: 10 * time.Second,
	}
	defer writer.Close()

	// Write message
	msg := kafka_go.Message{
		Value: data,
		Time:  event.Timestamp,
		Headers: []kafka_go.Header{
			{Key: "source_service", Value: []byte(event.SourceService)},
			{Key: "action", Value: []byte(event.Action)},
		},
	}

	if err := writer.WriteMessages(ctx, msg); err != nil {
		e.logger.Error().
			Err(err).
			Str("topic", e.topic).
			Str("source_service", event.SourceService).
			Str("action", event.Action).
			Msg("Failed to emit audit event")
		return err
	}

	e.logger.Debug().
		Str("topic", e.topic).
		Str("user_id", event.UserID).
		Str("action", event.Action).
		Str("source_service", event.SourceService).
		Msg("Audit event emitted")

	return nil
}

// EmitSync is an alias for Emit (for backward compatibility)
func (e *Emitter) EmitSync(ctx context.Context, event AuditEvent) error {
	return e.Emit(ctx, event)
}

// EmitAsync publishes an audit event asynchronously (fire and forget)
func (e *Emitter) EmitAsync(ctx context.Context, event AuditEvent) {
	go func() {
		if err := e.Emit(ctx, event); err != nil {
			e.logger.Warn().Err(err).Msg("Failed to emit audit event asynchronously")
		}
	}()
}
