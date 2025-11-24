package com.logservice.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Emitter handles publishing audit events to Kafka
 */
public class AuditEmitter implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(AuditEmitter.class);
    private static final int DEFAULT_TIMEOUT_SECONDS = 10;
    
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final String sourceService;
    private final ObjectMapper objectMapper;

    /**
     * Creates a new AuditEmitter
     * 
     * @param brokers Comma-separated list of Kafka brokers (e.g., "localhost:9092,localhost:9093")
     * @param topic Kafka topic to publish to
     * @param sourceService Name of the source service
     */
    public AuditEmitter(String brokers, String topic, String sourceService) {
        this(createDefaultProperties(brokers), topic, sourceService);
    }

    /**
     * Creates a new AuditEmitter with custom Kafka properties
     * 
     * @param kafkaProperties Custom Kafka producer properties
     * @param topic Kafka topic to publish to
     * @param sourceService Name of the source service
     */
    public AuditEmitter(Properties kafkaProperties, String topic, String sourceService) {
        this.topic = topic;
        this.sourceService = sourceService;
        this.producer = new KafkaProducer<>(kafkaProperties);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    /**
     * Creates default Kafka producer properties
     */
    private static Properties createDefaultProperties(String brokers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        return props;
    }

    /**
     * Emits an audit event synchronously
     * 
     * @param event The audit event to emit
     * @throws AuditException if the event cannot be emitted
     */
    public void emit(AuditEvent event) throws AuditException {
        try {
            emitAsync(event).get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AuditException("Interrupted while emitting audit event", e);
        } catch (ExecutionException e) {
            throw new AuditException("Failed to emit audit event", e.getCause());
        } catch (TimeoutException e) {
            throw new AuditException("Timeout while emitting audit event", e);
        }
    }

    /**
     * Emits an audit event synchronously (alias for backward compatibility)
     * 
     * @param event The audit event to emit
     * @throws AuditException if the event cannot be emitted
     */
    public void emitSync(AuditEvent event) throws AuditException {
        emit(event);
    }

    /**
     * Emits an audit event asynchronously
     * 
     * @param event The audit event to emit
     * @return CompletableFuture that completes when the event is sent
     */
    public CompletableFuture<RecordMetadata> emitAsync(AuditEvent event) {
        // Set timestamp if not provided
        if (event.getTimestamp() == null) {
            event.setTimestamp(Instant.now());
        }

        // Set source service if not provided
        if (event.getSourceService() == null || event.getSourceService().isEmpty()) {
            event.setSourceService(sourceService);
        }

        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();

        try {
            // Serialize event to JSON
            String jsonValue = objectMapper.writeValueAsString(event);

            // Create headers
            List<Header> headers = new ArrayList<>();
            headers.add(new RecordHeader("source_service", 
                event.getSourceService().getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("action", 
                event.getAction().getBytes(StandardCharsets.UTF_8)));

            // Create Kafka record
            ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,
                null,
                event.getTimestamp().toEpochMilli(),
                null,
                jsonValue,
                headers
            );

            // Send to Kafka
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to emit audit event - topic: {}, source_service: {}, action: {}", 
                        topic, event.getSourceService(), event.getAction(), exception);
                    future.completeExceptionally(exception);
                } else {
                    logger.debug("Audit event emitted - topic: {}, user_id: {}, action: {}, source_service: {}", 
                        topic, event.getUserId(), event.getAction(), event.getSourceService());
                    future.complete(metadata);
                }
            });

        } catch (Exception e) {
            logger.error("Failed to serialize audit event", e);
            future.completeExceptionally(e);
        }

        return future;
    }

    /**
     * Emits an audit event asynchronously without waiting for result (fire and forget)
     * 
     * @param event The audit event to emit
     */
    public void emitFireAndForget(AuditEvent event) {
        emitAsync(event).whenComplete((metadata, exception) -> {
            if (exception != null) {
                logger.warn("Failed to emit audit event asynchronously", exception);
            }
        });
    }

    /**
     * Closes the Kafka producer and releases resources
     */
    @Override
    public void close() {
        if (producer != null) {
            try {
                producer.flush();
                producer.close(5, TimeUnit.SECONDS);
                logger.info("AuditEmitter closed successfully");
            } catch (Exception e) {
                logger.error("Error closing AuditEmitter", e);
            }
        }
    }

    /**
     * Gets the configured topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Gets the source service name
     */
    public String getSourceService() {
        return sourceService;
    }
}
