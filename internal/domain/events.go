package domain

import (
	"time"

	"github.com/google/uuid"

	"wager-wallet-service/internal/money"
)

const (
	EventTransactionProcessed = "WagerTransactionProcessed"
	EventTransactionRejected  = "WagerTransactionRejected"
	EventBalanceChanged       = "WalletBalanceChanged"
	EventPendingReference     = "WagerTransactionPendingReference"
)

// Event is the integration envelope stored in the outbox and published as is.
// Type and version are set by the constructors below, never by callers.
type Event struct {
	EventID       uuid.UUID `json:"eventId"`
	EventType     string    `json:"eventType"`
	AggregateID   uuid.UUID `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    time.Time `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          any       `json:"data"`
}

// EventMeta carries tracing ids: correlation is the idempotency key (or the
// opening transaction id), causation is the HTTP request or SQS message id.
type EventMeta struct {
	CorrelationID string
	CausationID   string
	At            time.Time
}

func newEvent(id, aggregate uuid.UUID, typ string, m EventMeta, data any) Event {
	return Event{EventID: id, EventType: typ, AggregateID: aggregate, CorrelationID: m.CorrelationID,
		CausationID: m.CausationID, OccurredAt: m.At.UTC(), Version: 1, Data: data}
}

// TransactionRef is the part every transaction event shares.
type TransactionRef struct {
	TransactionID         uuid.UUID   `json:"transactionId"`
	WalletID              uuid.UUID   `json:"walletId"`
	PlayerID              uuid.UUID   `json:"playerId"`
	Kind                  Kind        `json:"kind"`
	Money                 money.Money `json:"money"`
	ProviderID            string      `json:"providerId,omitempty"`
	ExternalTransactionID string      `json:"externalTransactionId,omitempty"`
	RoundID               string      `json:"roundId,omitempty"`
	GameID                string      `json:"gameId,omitempty"`
}

func refOf(t *Transaction) TransactionRef {
	s := t.s
	return TransactionRef{TransactionID: s.ID, WalletID: s.WalletID, PlayerID: s.PlayerID, Kind: s.Kind, Money: s.Money,
		ProviderID: s.ProviderID, ExternalTransactionID: s.ExternalID, RoundID: s.RoundID, GameID: s.GameID}
}

type TransactionProcessed struct {
	TransactionRef
	Balance                money.Money `json:"balance"`
	ReferenceTransactionID *uuid.UUID  `json:"referenceTransactionId,omitempty"`
}

type TransactionRejected struct {
	TransactionRef
	FailureCode FailureCode `json:"failureCode"`
}

type PendingReference struct {
	TransactionRef
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId"`
	ExpiresAt                      time.Time `json:"expiresAt"`
}

type BalanceChanged struct {
	WalletID      uuid.UUID   `json:"walletId"`
	TransactionID uuid.UUID   `json:"transactionId"`
	Direction     Direction   `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	WalletVersion int64       `json:"walletVersion"`
}

func NewProcessedEvent(id uuid.UUID, m EventMeta, t *Transaction) Event {
	d := TransactionProcessed{TransactionRef: refOf(t), Balance: t.s.ResultBalance}
	if t.s.ReferenceID != uuid.Nil {
		ref := t.s.ReferenceID
		d.ReferenceTransactionID = &ref
	}
	return newEvent(id, t.s.ID, EventTransactionProcessed, m, d)
}

func NewRejectedEvent(id uuid.UUID, m EventMeta, t *Transaction) Event {
	return newEvent(id, t.s.ID, EventTransactionRejected, m, TransactionRejected{TransactionRef: refOf(t), FailureCode: t.s.FailureCode})
}

func NewPendingReferenceEvent(id uuid.UUID, m EventMeta, t *Transaction) Event {
	return newEvent(id, t.s.ID, EventPendingReference, m, PendingReference{
		TransactionRef: refOf(t), ReferenceExternalTransactionID: t.s.ReferenceExternalID, ExpiresAt: t.s.ExpiresAt})
}

func NewBalanceChangedEvent(id uuid.UUID, m EventMeta, e LedgerEntry) Event {
	return newEvent(id, e.walletID, EventBalanceChanged, m, BalanceChanged{
		WalletID: e.walletID, TransactionID: e.transactionID, Direction: e.direction, Money: e.amount,
		BalanceBefore: e.before, BalanceAfter: e.after, WalletVersion: e.walletVersion})
}

// EventsFor returns the events a settled transaction produces: one per
// status, plus WalletBalanceChanged when the balance moved.
func EventsFor(newID func() uuid.UUID, m EventMeta, t *Transaction, entry *LedgerEntry) []Event {
	var out []Event
	switch t.s.Status {
	case StatusProcessed:
		out = append(out, NewProcessedEvent(newID(), m, t))
	case StatusRejected:
		out = append(out, NewRejectedEvent(newID(), m, t))
	case StatusPendingReference:
		out = append(out, NewPendingReferenceEvent(newID(), m, t))
	}
	if entry != nil {
		out = append(out, NewBalanceChangedEvent(newID(), m, *entry))
	}
	return out
}
