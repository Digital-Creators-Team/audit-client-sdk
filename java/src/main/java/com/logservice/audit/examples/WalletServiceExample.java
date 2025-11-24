package com.logservice.audit.examples;

import com.logservice.audit.AuditEmitter;
import com.logservice.audit.AuditEvent;
import com.logservice.audit.AuditException;
import com.logservice.audit.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example demonstrating how to use AuditEmitter in a wallet service
 */
public class WalletServiceExample {
    
    private static final Logger logger = LoggerFactory.getLogger(WalletServiceExample.class);
    private final AuditEmitter auditEmitter;

    public WalletServiceExample(String kafkaBrokers) {
        this.auditEmitter = new AuditEmitter(
            kafkaBrokers,
            "wallet.audit",
            "wallet-service"
        );
    }

    /**
     * Deposit money and emit audit event
     */
    public void deposit(String userId, long amount, String currency) {
        // ... deposit logic ...

        // Create and emit audit event
        AuditEvent event = AuditEvent.builder()
            .userId(userId)
            .sourceService("wallet")
            .action("deposit")
            .addDetail("amount", amount)
            .addDetail("currency", currency)
            .result(Result.SUCCESS)
            .build();

        // Emit async to not block
        auditEmitter.emitFireAndForget(event);
        
        logger.info("Deposit completed for user: {}, amount: {} {}", userId, amount, currency);
    }

    /**
     * Withdraw money and emit audit event
     */
    public void withdraw(String userId, long amount, String currency) {
        // ... withdraw logic ...

        AuditEvent event = AuditEvent.builder()
            .userId(userId)
            .sourceService("wallet")
            .action("withdraw")
            .addDetail("amount", amount)
            .addDetail("currency", currency)
            .result(Result.SUCCESS)
            .build();

        auditEmitter.emitFireAndForget(event);
        
        logger.info("Withdrawal completed for user: {}, amount: {} {}", userId, amount, currency);
    }

    /**
     * Transfer money between users and emit audit event
     */
    public void transfer(String fromUserId, String toUserId, long amount, String currency) {
        // ... transfer logic ...

        AuditEvent event = AuditEvent.builder()
            .userId(fromUserId)
            .sourceService("wallet")
            .action("transfer")
            .addDetail("amount", amount)
            .addDetail("currency", currency)
            .addDetail("from", fromUserId)
            .addDetail("to", toUserId)
            .result(Result.SUCCESS)
            .build();

        auditEmitter.emitFireAndForget(event);
        
        logger.info("Transfer completed from {} to {}, amount: {} {}", 
            fromUserId, toUserId, amount, currency);
    }

    /**
     * Example of synchronous emission with error handling
     */
    public void depositWithSyncAudit(String userId, long amount, String currency) throws AuditException {
        // ... deposit logic ...

        AuditEvent event = AuditEvent.builder()
            .userId(userId)
            .sourceService("wallet")
            .action("deposit")
            .addDetail("amount", amount)
            .addDetail("currency", currency)
            .result(Result.SUCCESS)
            .build();

        // Emit synchronously - will throw exception if fails
        auditEmitter.emitSync(event);
        
        logger.info("Deposit with sync audit completed for user: {}", userId);
    }

    /**
     * Close the audit emitter when done
     */
    public void close() {
        auditEmitter.close();
    }

    /**
     * Main method for testing
     */
    public static void main(String[] args) {
        WalletServiceExample service = new WalletServiceExample("localhost:9092");
        
        try {
            // Example usage
            service.deposit("user123", 10000, "USD");
            service.withdraw("user123", 5000, "USD");
            service.transfer("user123", "user456", 2000, "USD");
            
            // Wait a bit for async messages to be sent
            Thread.sleep(2000);
            
        } catch (Exception e) {
            logger.error("Error in wallet service example", e);
        } finally {
            service.close();
        }
    }
}
