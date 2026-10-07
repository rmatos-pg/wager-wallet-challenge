// Package service holds the use cases. HTTP and SQS both call Wagers.Process,
// so they share validation, idempotency and the transaction boundary.
package service

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"wager-wallet-service/internal/domain"
	"wager-wallet-service/internal/money"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrWalletNotFound      = errors.New("wallet not found")
	ErrWalletExists        = errors.New("wallet already exists for this player and currency")
	ErrWalletMismatch      = errors.New("player or currency does not match the wallet")
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different payload")
	ErrExternalIDReused    = errors.New("externalTransactionId already used with another idempotency key")
)

func newID() uuid.UUID { return uuid.Must(uuid.NewV7()) }

// WagerInput is the JSON of an operation, shared by the HTTP body and the
// SQS message "data" field.
type WagerInput struct {
	ProviderID                     string     `json:"providerId"`
	ExternalTransactionID          string     `json:"externalTransactionId"`
	IdempotencyKey                 string     `json:"idempotencyKey,omitempty"` // SQS only; HTTP uses the header
	PlayerID                       string     `json:"playerId"`
	WalletID                       string     `json:"walletId"`
	RoundID                        string     `json:"roundId"`
	GameID                         string     `json:"gameId"`
	Kind                           string     `json:"kind"`
	Money                          money.JSON `json:"money"`
	ReferenceExternalTransactionID string     `json:"referenceExternalTransactionId,omitempty"`
}

// Request converts and validates the input. UUIDs are normalized to their
// canonical lower-case form before hashing.
func (in WagerInput) Request(idempotencyKey string) (domain.Request, error) {
	player, err := uuid.Parse(in.PlayerID)
	if err != nil {
		return domain.Request{}, invalidf("playerId must be a UUID")
	}
	wallet, err := uuid.Parse(in.WalletID)
	if err != nil {
		return domain.Request{}, invalidf("walletId must be a UUID")
	}
	m, err := money.ParseExternal(in.Money.Amount, in.Money.Currency)
	if err != nil {
		return domain.Request{}, invalidf("money: %v", err)
	}
	r := domain.Request{
		ProviderID: in.ProviderID, ExternalID: in.ExternalTransactionID, IdempotencyKey: idempotencyKey,
		PlayerID: player, WalletID: wallet, RoundID: in.RoundID, GameID: in.GameID,
		Kind: domain.Kind(in.Kind), Money: m, ReferenceExternalID: in.ReferenceExternalTransactionID,
	}
	return r, r.Validate()
}

// MessageHash identifies the content of an SQS message for the inbox: the
// business hash plus the idempotency key carried in the message.
func MessageHash(r domain.Request) string {
	sum := sha256.Sum256([]byte(r.Hash() + "\n" + r.IdempotencyKey))
	return hex.EncodeToString(sum[:])
}

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", domain.ErrInvalid, fmt.Sprintf(format, args...))
}
