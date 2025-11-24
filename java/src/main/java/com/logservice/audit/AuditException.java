package com.logservice.audit;

/**
 * Exception thrown when audit event emission fails
 */
public class AuditException extends Exception {
    
    public AuditException(String message) {
        super(message);
    }

    public AuditException(String message, Throwable cause) {
        super(message, cause);
    }

    public AuditException(Throwable cause) {
        super(cause);
    }
}
