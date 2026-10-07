package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/google/uuid"

	"wager-wallet-service/internal/money"
)

type Kind string

const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

// External reports whether providers may send this kind.
func (k Kind) External() bool {
	switch k {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return true
	}
	return false
}

// IsReversal: REFUND and ROLLBACK require a reference and undo it fully.
func (k Kind) IsReversal() bool { return k == KindRefund || k == KindRollback }

const maxFieldLen = 128

// Request is a provider operation, identical whether it came by HTTP or SQS.
type Request struct {
	ProviderID          string
	ExternalID          string
	IdempotencyKey      string
	PlayerID            uuid.UUID
	WalletID            uuid.UUID
	RoundID             string
	GameID              string
	Kind                Kind
	Money               money.Money
	ReferenceExternalID string
}

func (r Request) Validate() error {
	for name, v := range map[string]string{
		"providerId": r.ProviderID, "externalTransactionId": r.ExternalID, "idempotencyKey": r.IdempotencyKey,
		"roundId": r.RoundID, "gameId": r.GameID,
	} {
		if v == "" || len(v) > maxFieldLen {
			return invalid("%s is required (max %d chars)", name, maxFieldLen)
		}
	}
	if len(r.ReferenceExternalID) > maxFieldLen {
		return invalid("referenceExternalTransactionId too long")
	}
	if r.PlayerID == uuid.Nil || r.WalletID == uuid.Nil {
		return invalid("playerId and walletId are required")
	}
	if r.Kind == KindOpening {
		return invalid("OPENING is reserved for wallet opening")
	}
	if !r.Kind.External() {
		return invalid("unknown kind %q", r.Kind)
	}
	if !r.Money.Valid() || r.Money.IsNegative() {
		return invalid("money is required and must not be negative")
	}
	if r.Kind == KindLoss && !r.Money.IsZero() {
		return invalid("LOSS amount must be 0.00")
	}
	if r.Kind != KindLoss && !r.Money.IsPositive() {
		return invalid("%s amount must be greater than zero", r.Kind)
	}
	if r.Kind.IsReversal() && r.ReferenceExternalID == "" {
		return invalid("%s requires referenceExternalTransactionId", r.Kind)
	}
	if r.ReferenceExternalID == r.ExternalID {
		return invalid("an operation cannot reference itself")
	}
	return nil
}

// Hash is the SHA-256 (hex) of the canonical JSON of the business fields.
// encoding/json sorts map keys, which gives the canonical ordering. The
// idempotency key and transport metadata are excluded; UUIDs are lower-case
// and the amount is already canonical (the parser accepts no other form).
func (r Request) Hash() string {
	fields := map[string]any{
		"providerId":            r.ProviderID,
		"externalTransactionId": r.ExternalID,
		"playerId":              r.PlayerID.String(),
		"walletId":              r.WalletID.String(),
		"roundId":               r.RoundID,
		"gameId":                r.GameID,
		"kind":                  string(r.Kind),
		"money":                 map[string]string{"amount": r.Money.Amount(), "currency": r.Money.Currency()},
	}
	if r.ReferenceExternalID != "" {
		fields["referenceExternalTransactionId"] = r.ReferenceExternalID
	}
	b, _ := json.Marshal(fields)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
