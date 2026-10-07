package domain

import (
	"time"

	"github.com/google/uuid"

	"wager-wallet-service/internal/money"
)

type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

func (s Status) Terminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

type Origin string

const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)

// TxState is a read-only snapshot of a transaction (also the rehydration
// input). Changing a copy does not change the transaction.
type TxState struct {
	ID       uuid.UUID
	Origin   Origin
	Kind     Kind
	Status   Status
	WalletID uuid.UUID
	PlayerID uuid.UUID
	Money    money.Money

	// External operations only.
	ProviderID          string
	ExternalID          string
	IdempotencyKey      string
	PayloadHash         string
	RoundID             string
	GameID              string
	ReferenceExternalID string
	ReferenceID         uuid.UUID // resolved reference, once processed

	FailureCode   FailureCode
	ResultBalance money.Money // balance seen when processed; replays return it

	Attempts      int
	NextAttemptAt time.Time
	ExpiresAt     time.Time

	CreatedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt time.Time
}

// Transaction state machine (terminal states accept nothing):
//
//	PENDING -> PROCESSED | REJECTED | FAILED | PENDING_REFERENCE
//	PENDING_REFERENCE -> PROCESSED | REJECTED | FAILED | PENDING_REFERENCE (retry)
type Transaction struct{ s TxState }

// NewExternal accepts a provider operation in PENDING.
func NewExternal(id uuid.UUID, r Request, now time.Time) (*Transaction, error) {
	if id == uuid.Nil || now.IsZero() {
		return nil, invalid("transaction id and timestamp are required")
	}
	if err := r.Validate(); err != nil {
		return nil, err
	}
	now = now.UTC()
	return &Transaction{TxState{
		ID: id, Origin: OriginExternal, Kind: r.Kind, Status: StatusPending,
		WalletID: r.WalletID, PlayerID: r.PlayerID, Money: r.Money,
		ProviderID: r.ProviderID, ExternalID: r.ExternalID, IdempotencyKey: r.IdempotencyKey,
		PayloadHash: r.Hash(), RoundID: r.RoundID, GameID: r.GameID, ReferenceExternalID: r.ReferenceExternalID,
		CreatedAt: now, UpdatedAt: now,
	}}, nil
}

// NewOpening is the internal initial credit; it is born PROCESSED and has
// none of the external metadata.
func NewOpening(id, walletID, playerID uuid.UUID, amount money.Money, now time.Time) (*Transaction, error) {
	if id == uuid.Nil || walletID == uuid.Nil || playerID == uuid.Nil || now.IsZero() {
		return nil, invalid("opening ids and timestamp are required")
	}
	if !amount.Valid() || !amount.IsPositive() {
		return nil, invalid("opening amount must be positive")
	}
	now = now.UTC()
	return &Transaction{TxState{
		ID: id, Origin: OriginInternal, Kind: KindOpening, Status: StatusProcessed,
		WalletID: walletID, PlayerID: playerID, Money: amount, ResultBalance: amount,
		CreatedAt: now, UpdatedAt: now, CompletedAt: now,
	}}, nil
}

// Rehydrate rebuilds a stored transaction without replaying anything.
func Rehydrate(s TxState) (*Transaction, error) {
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || !s.Money.Valid() {
		return nil, invalid("stored transaction is incomplete")
	}
	if s.Status == StatusPending {
		return nil, invalid("PENDING is never stored")
	}
	return &Transaction{s}, nil
}

func (t *Transaction) State() TxState     { return t.s }
func (t *Transaction) ID() uuid.UUID      { return t.s.ID }
func (t *Transaction) Kind() Kind         { return t.s.Kind }
func (t *Transaction) Status() Status     { return t.s.Status }
func (t *Transaction) Money() money.Money { return t.s.Money }

// Process completes the operation with the wallet balance after it.
func (t *Transaction) Process(balanceAfter money.Money, referenceID uuid.UUID, now time.Time) error {
	if err := t.requireOpen(); err != nil {
		return err
	}
	if !balanceAfter.Valid() || balanceAfter.IsNegative() || balanceAfter.Currency() != t.s.Money.Currency() {
		return invalid("resulting balance is invalid")
	}
	if (t.s.ReferenceExternalID != "") != (referenceID != uuid.Nil) {
		return invalid("resolved reference does not match the request")
	}
	t.s.Status = StatusProcessed
	t.s.ResultBalance = balanceAfter
	t.s.ReferenceID = referenceID
	t.finish(now)
	return nil
}

func (t *Transaction) Reject(code FailureCode, now time.Time) error {
	if err := t.requireOpen(); err != nil {
		return err
	}
	if code == "" || code == FailureInfrastructure {
		return invalid("%q is not a rejection code", code)
	}
	t.s.Status = StatusRejected
	t.s.FailureCode = code
	t.finish(now)
	return nil
}

// Fail records a permanent infrastructure failure for audit.
func (t *Transaction) Fail(now time.Time) error {
	if err := t.requireOpen(); err != nil {
		return err
	}
	t.s.Status = StatusFailed
	t.s.FailureCode = FailureInfrastructure
	t.finish(now)
	return nil
}

// AwaitReference parks a PENDING operation until its reference shows up.
func (t *Transaction) AwaitReference(next, expires, now time.Time) error {
	if t.s.Status != StatusPending {
		return ErrInvalidTransition
	}
	if t.s.ReferenceExternalID == "" || next.IsZero() || expires.Before(next) {
		return invalid("cannot wait for a reference")
	}
	t.s.Status = StatusPendingReference
	t.s.NextAttemptAt = next.UTC()
	t.s.ExpiresAt = expires.UTC()
	t.touch(now)
	return nil
}

// Reschedule records one more failed lookup.
func (t *Transaction) Reschedule(next, now time.Time) error {
	if t.s.Status != StatusPendingReference {
		return ErrInvalidTransition
	}
	t.s.Attempts++
	t.s.NextAttemptAt = next.UTC()
	t.touch(now)
	return nil
}

func (t *Transaction) requireOpen() error {
	if t.s.Status.Terminal() {
		return ErrInvalidTransition
	}
	return nil
}

func (t *Transaction) touch(now time.Time) {
	if now = now.UTC(); now.After(t.s.UpdatedAt) {
		t.s.UpdatedAt = now
	}
}

func (t *Transaction) finish(now time.Time) {
	t.touch(now)
	t.s.CompletedAt = t.s.UpdatedAt
	t.s.NextAttemptAt = time.Time{}
}
