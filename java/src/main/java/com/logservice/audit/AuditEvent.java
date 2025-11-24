package com.logservice.audit;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an audit event to be published to Kafka
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AuditEvent {
    
    @JsonProperty("timestamp")
    @JsonSerialize(using = InstantSerializer.class)
    private Instant timestamp;
    
    @JsonProperty("user_id")
    private String userId;
    
    @JsonProperty("session_id")
    private String sessionId;
    
    @JsonProperty("source_service")
    private String sourceService;
    
    @JsonProperty("action")
    private String action;
    
    @JsonProperty("details")
    private Map<String, Object> details;
    
    @JsonProperty("result")
    private Result result;
    
    @JsonProperty("ip")
    private String ip;
    
    @JsonProperty("server")
    private String server;
    
    @JsonProperty("trace_id")
    private String traceId;

    public AuditEvent() {
        this.timestamp = Instant.now();
        this.details = new HashMap<>();
    }

    // Builder pattern for easy construction
    public static class Builder {
        private final AuditEvent event;

        public Builder() {
            this.event = new AuditEvent();
        }

        public Builder timestamp(Instant timestamp) {
            event.timestamp = timestamp;
            return this;
        }

        public Builder userId(String userId) {
            event.userId = userId;
            return this;
        }

        public Builder sessionId(String sessionId) {
            event.sessionId = sessionId;
            return this;
        }

        public Builder sourceService(String sourceService) {
            event.sourceService = sourceService;
            return this;
        }

        public Builder action(String action) {
            event.action = action;
            return this;
        }

        public Builder details(Map<String, Object> details) {
            event.details = details;
            return this;
        }

        public Builder addDetail(String key, Object value) {
            event.details.put(key, value);
            return this;
        }

        public Builder result(Result result) {
            event.result = result;
            return this;
        }

        public Builder ip(String ip) {
            event.ip = ip;
            return this;
        }

        public Builder server(String server) {
            event.server = server;
            return this;
        }

        public Builder traceId(String traceId) {
            event.traceId = traceId;
            return this;
        }

        public AuditEvent build() {
            if (event.timestamp == null) {
                event.timestamp = Instant.now();
            }
            return event;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    // Getters and Setters
    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getSourceService() {
        return sourceService;
    }

    public void setSourceService(String sourceService) {
        this.sourceService = sourceService;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Map<String, Object> getDetails() {
        return details;
    }

    public void setDetails(Map<String, Object> details) {
        this.details = details;
    }

    public Result getResult() {
        return result;
    }

    public void setResult(Result result) {
        this.result = result;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }
}
