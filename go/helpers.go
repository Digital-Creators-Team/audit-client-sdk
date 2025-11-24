package sdk

import (
	"context"
	"net/http"
	"time"
)

// EmitFromHTTPRequest creates and emits an audit event from HTTP request context
func (e *Emitter) EmitFromHTTPRequest(ctx context.Context, req *http.Request, action string, details map[string]interface{}, result Result) error {
	userID := e.extractUserID(ctx, req)
	sessionID := e.extractSessionID(ctx, req)
	traceID := e.extractTraceID(ctx, req)
	ip := e.extractIP(req)

	e.logger.Debug().
		Str("action", action).
		Str("user_id", userID).
		Str("ip", ip).
		Str("trace_id", traceID).
		Msg("Creating audit event from HTTP request")

	event := AuditEvent{
		Timestamp:     time.Now(),
		UserID:        userID,
		SessionID:     sessionID,
		SourceService: e.sourceService,
		Action:        action,
		Details:       details,
		Result:        Result(result),
		IP:            ip,
		TraceID:       traceID,
	}

	return e.Emit(ctx, event)
}

// Helper methods to extract information from context/request
func (e *Emitter) extractUserID(ctx context.Context, req *http.Request) string {
	// Try to get from context first (if your auth middleware sets it)
	if userID, ok := ctx.Value("user_id").(string); ok && userID != "" {
		return userID
	}
	// Fallback to header
	if userID := req.Header.Get("X-User-ID"); userID != "" {
		return userID
	}
	return ""
}

func (e *Emitter) extractSessionID(ctx context.Context, req *http.Request) string {
	if sessionID, ok := ctx.Value("session_id").(string); ok && sessionID != "" {
		return sessionID
	}
	if sessionID := req.Header.Get("X-Session-ID"); sessionID != "" {
		return sessionID
	}
	return ""
}

func (e *Emitter) extractTraceID(ctx context.Context, req *http.Request) string {
	if traceID, ok := ctx.Value("trace_id").(string); ok && traceID != "" {
		return traceID
	}
	if traceID := req.Header.Get("X-Trace-ID"); traceID != "" {
		return traceID
	}
	return ""
}

func (e *Emitter) extractIP(req *http.Request) string {
	// Check X-Forwarded-For header first (for proxies/load balancers)
	if forwarded := req.Header.Get("X-Forwarded-For"); forwarded != "" {
		return forwarded
	}
	// Fallback to RemoteAddr
	if req.RemoteAddr != "" {
		return req.RemoteAddr
	}
	return ""
}

func (e *Emitter) getSourceService() string {
	if e.sourceService != "" {
		return e.sourceService
	}
	return "unknown"
}

// SetSourceService sets the source service name for this emitter
func (e *Emitter) SetSourceService(serviceName string) {
	e.sourceService = serviceName
}
