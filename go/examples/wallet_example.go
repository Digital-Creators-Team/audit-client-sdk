package examples

import (
	sdk "audit-client-sdk"
	"context"

	"github.com/rs/zerolog"
)

// WalletServiceExample demonstrates how to use audit emitter in wallet service
type WalletServiceExample struct {
	auditEmitter *sdk.Emitter
}

func NewWalletServiceExample(kafkaBrokers []string, logger zerolog.Logger) *WalletServiceExample {
	emitter := sdk.NewEmitter(
		kafkaBrokers,
		"wallet.audit",
		"wallet-service",
		logger,
	)

	return &WalletServiceExample{
		auditEmitter: emitter,
	}
}

// Deposit emits audit event for deposit action
func (s *WalletServiceExample) Deposit(ctx context.Context, userID string, amount int64, currency string) error {
	// ... deposit logic ...

	// Emit audit event
	event := sdk.AuditEvent{
		UserID:        userID,
		SourceService: "wallet",
		Action:        "deposit",
		Details: map[string]interface{}{
			"amount":   amount,
			"currency": currency,
		},
		Result: sdk.Success,
	}

	// Emit async to not block
	s.auditEmitter.EmitAsync(ctx, event)

	return nil
}

// Withdraw emits audit event for withdraw action
func (s *WalletServiceExample) Withdraw(ctx context.Context, userID string, amount int64, currency string) error {
	// ... withdraw logic ...

	event := sdk.AuditEvent{
		UserID:        userID,
		SourceService: "wallet",
		Action:        "withdraw",
		Details: map[string]interface{}{
			"amount":   amount,
			"currency": currency,
		},
		Result: sdk.Success,
	}

	s.auditEmitter.EmitAsync(ctx, event)

	return nil
}

// Transfer emits audit event for transfer action
func (s *WalletServiceExample) Transfer(ctx context.Context, fromUserID, toUserID string, amount int64, currency string) error {
	// ... transfer logic ...

	event := sdk.AuditEvent{
		UserID:        fromUserID,
		SourceService: "wallet",
		Action:        "transfer",
		Details: map[string]interface{}{
			"amount":   amount,
			"currency": currency,
			"from":     fromUserID,
			"to":       toUserID,
		},
		Result: sdk.Success,
	}

	s.auditEmitter.EmitAsync(ctx, event)

	return nil
}

// GameServiceExample demonstrates how to use audit emitter in game service
type GameServiceExample struct {
	auditEmitter *sdk.Emitter
}

func NewGameServiceExample(kafkaBrokers []string, logger zerolog.Logger) *GameServiceExample {
	emitter := sdk.NewEmitter(
		kafkaBrokers,
		"game.audit",
		"game-service",
		logger,
	)

	return &GameServiceExample{
		auditEmitter: emitter,
	}
}

// MatchStart emits audit event when match starts
func (s *GameServiceExample) MatchStart(ctx context.Context, matchID, userID string) error {
	event := sdk.AuditEvent{
		UserID:        userID,
		SourceService: "game",
		Action:        "match_start",
		Details: map[string]interface{}{
			"match_id": matchID,
		},
		Result: sdk.Success,
	}

	s.auditEmitter.EmitAsync(ctx, event)
	return nil
}

// MatchEnd emits audit event when match ends
func (s *GameServiceExample) MatchEnd(ctx context.Context, matchID, userID string, result string, reward int64) error {
	event := sdk.AuditEvent{
		UserID:        userID,
		SourceService: "game",
		Action:        "match_end",
		Details: map[string]interface{}{
			"match_id": matchID,
			"result":   result,
			"reward":   reward,
		},
		Result: sdk.Success,
	}

	s.auditEmitter.EmitAsync(ctx, event)
	return nil
}

// PurchaseItem emits audit event for item purchase
func (s *GameServiceExample) PurchaseItem(ctx context.Context, userID, itemID string, price int64) error {
	event := sdk.AuditEvent{
		UserID:        userID,
		SourceService: "game",
		Action:        "purchase_item",
		Details: map[string]interface{}{
			"item_id": itemID,
			"price":   price,
		},
		Result: sdk.Success,
	}

	s.auditEmitter.EmitAsync(ctx, event)
	return nil
}
