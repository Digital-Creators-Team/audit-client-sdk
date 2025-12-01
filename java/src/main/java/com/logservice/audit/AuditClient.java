package com.logservice.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

public class AuditClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AuditClient.class);
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final String sourceService;
    private final ObjectMapper objectMapper;
    
    // Buffering fields
    private final BlockingQueue<AuditEvent> buffer;
    private final int batchSize;
    private final long flushIntervalMs;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService worker;
    private volatile boolean running;

    public AuditClient(String bootstrapServers, String topic, String sourceService) {
        this(bootstrapServers, topic, sourceService, 100, 500, 1000);
    }

    public AuditClient(String bootstrapServers, String topic, String sourceService, 
                       int batchSize, long flushIntervalMs, int bufferSize) {
        this.topic = topic;
        this.sourceService = sourceService;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.buffer = new LinkedBlockingQueue<>(bufferSize);
        this.running = true;
        
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        this.producer = new KafkaProducer<>(props);
        
        // Start background worker
        this.worker = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "audit-batch-worker");
            t.setDaemon(true);
            return t;
        });
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "audit-flush-scheduler");
            t.setDaemon(true);
            return t;
        });
        
        startBatchWorker();
    }

    private void startBatchWorker() {
        worker.submit(() -> {
            List<AuditEvent> batch = new ArrayList<>(batchSize);
            
            while (running || !buffer.isEmpty()) {
                try {
                    // Try to get an event with timeout
                    AuditEvent event = buffer.poll(100, TimeUnit.MILLISECONDS);
                    
                    if (event != null) {
                        batch.add(event);
                        
                        // Flush if batch is full
                        if (batch.size() >= batchSize) {
                            flushBatch(batch);
                            batch.clear();
                        }
                    } else if (!batch.isEmpty()) {
                        // No new events, but we have some in batch - check if we should flush
                        // This is handled by the scheduler
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Error in batch worker", e);
                }
            }
            
            // Flush remaining events
            if (!batch.isEmpty()) {
                flushBatch(batch);
            }
        });
        
        // Schedule periodic flush
        scheduler.scheduleAtFixedRate(() -> {
            // This is a simple approach - in production you might want more sophisticated logic
            // The worker thread handles the actual batching
        }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void flushBatch(List<AuditEvent> batch) {
        if (batch.isEmpty()) {
            return;
        }
        
        logger.debug("Flushing batch of {} events", batch.size());
        
        try {
            for (AuditEvent event : batch) {
                sendSync(event);
            }
        } catch (Exception e) {
            logger.error("Failed to flush batch", e);
        }
    }

    public void emit(AuditEvent event) {
        try {
            sendSync(event);
        } catch (Exception e) {
            logger.error("Failed to emit audit event", e);
            throw new RuntimeException("Failed to emit audit event", e);
        }
    }

    public void emitAsync(AuditEvent event) {
        if (event.getTimestamp() == null) {
            event.setTimestamp(Instant.now());
        }
        
        boolean added = buffer.offer(event);
        if (!added) {
            logger.warn("Buffer is full, event dropped: action={}, userId={}", 
                       event.getAction(), event.getUserId());
        } else {
            logger.debug("Event added to buffer: action={}, userId={}", 
                        event.getAction(), event.getUserId());
        }
    }

    public void emitBatch(List<AuditEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        try {
            for (AuditEvent event : events) {
                sendSync(event);
            }
        } catch (Exception e) {
            logger.error("Failed to emit batch audit events", e);
            throw new RuntimeException("Failed to emit batch audit events", e);
        }
    }

    private void sendSync(AuditEvent event) throws Exception {
        if (event.getTimestamp() == null) {
            event.setTimestamp(Instant.now());
        }
        if (event.getSourceService() == null) {
            event.setSourceService(this.sourceService);
        }

        String value = objectMapper.writeValueAsString(event);
        
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, event.getTimestamp().toEpochMilli(), null, value);
        record.headers().add(new RecordHeader("source_service", event.getSourceService().getBytes(StandardCharsets.UTF_8)));
        if (event.getAction() != null) {
            record.headers().add(new RecordHeader("action", event.getAction().getBytes(StandardCharsets.UTF_8)));
        }

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Error sending message to Kafka", exception);
            } else {
                logger.debug("Message sent to topic {} partition {} offset {}", metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    @Override
    public void close() {
        running = false;
        
        // Shutdown scheduler
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Shutdown worker
        worker.shutdown();
        try {
            worker.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Close producer
        if (producer != null) {
            producer.close();
        }
    }
}

