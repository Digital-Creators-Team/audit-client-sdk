package com.logservice.audit.examples;

import com.logservice.audit.AuditEmitter;
import com.logservice.audit.AuditEvent;
import com.logservice.audit.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example demonstrating how to use AuditEmitter in a game service
 */
public class GameServiceExample {
    
    private static final Logger logger = LoggerFactory.getLogger(GameServiceExample.class);
    private final AuditEmitter auditEmitter;

    public GameServiceExample(String kafkaBrokers) {
        this.auditEmitter = new AuditEmitter(
            kafkaBrokers,
            "game.audit",
            "game-service"
        );
    }

    /**
     * Emit audit event when match starts
     */
    public void matchStart(String matchId, String userId) {
        AuditEvent event = AuditEvent.builder()
            .userId(userId)
            .sourceService("game")
            .action("match_start")
            .addDetail("match_id", matchId)
            .result(Result.SUCCESS)
            .build();

        auditEmitter.emitFireAndForget(event);
        
        logger.info("Match started - matchId: {}, userId: {}", matchId, userId);
    }

    /**
     * Emit audit event when match ends
     */
    public void matchEnd(String matchId, String userId, String matchResult, long reward) {
        AuditEvent event = AuditEvent.builder()
            .userId(userId)
            .sourceService("game")
            .action("match_end")
            .addDetail("match_id", matchId)
            .addDetail("result", matchResult)
            .addDetail("reward", reward)
            .result(Result.SUCCESS)
            .build();

        auditEmitter.emitFireAndForget(event);
        
        logger.info("Match ended - matchId: {}, userId: {}, result: {}, reward: {}", 
            matchId, userId, matchResult, reward);
    }

    /**
     * Emit audit event for item purchase
     */
    public void purchaseItem(String userId, String itemId, long price) {
        AuditEvent event = AuditEvent.builder()
            .userId(userId)
            .sourceService("game")
            .action("purchase_item")
            .addDetail("item_id", itemId)
            .addDetail("price", price)
            .result(Result.SUCCESS)
            .build();

        auditEmitter.emitFireAndForget(event);
        
        logger.info("Item purchased - userId: {}, itemId: {}, price: {}", 
            userId, itemId, price);
    }

    /**
     * Emit audit event for level completion
     */
    public void levelComplete(String userId, int level, int score, long experienceGained) {
        AuditEvent event = AuditEvent.builder()
            .userId(userId)
            .sourceService("game")
            .action("level_complete")
            .addDetail("level", level)
            .addDetail("score", score)
            .addDetail("experience_gained", experienceGained)
            .result(Result.SUCCESS)
            .build();

        auditEmitter.emitFireAndForget(event);
        
        logger.info("Level completed - userId: {}, level: {}, score: {}", 
            userId, level, score);
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
        GameServiceExample service = new GameServiceExample("localhost:9092");
        
        try {
            // Example usage
            service.matchStart("match-001", "player123");
            service.matchEnd("match-001", "player123", "victory", 1000);
            service.purchaseItem("player123", "sword-legendary", 5000);
            service.levelComplete("player123", 10, 95000, 2500);
            
            // Wait a bit for async messages to be sent
            Thread.sleep(2000);
            
        } catch (Exception e) {
            logger.error("Error in game service example", e);
        } finally {
            service.close();
        }
    }
}
