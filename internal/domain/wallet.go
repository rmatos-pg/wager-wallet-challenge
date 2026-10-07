package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"wager-wallet-service/internal/money"
)

type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

// Wallet is the financial aggregate root. Balance only changes through
// Debit/Credit, and every change returns the ledger entry that must be saved
// in the same SQL transaction.
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

type OpenWallet struct {
	WalletID    uuid.UUID
	PlayerID    uuid.UUID
	Initial     money.Money
	OpeningTxID uuid.UUID // used only when Initial > 0
	EntryID     uuid.UUID // used only when Initial > 0
	Now         time.Time
}

// Open creates a wallet at version 1. A positive initial balance comes with
// its OPENING credit entry; zero produces no entry.
func Open(p OpenWallet) (*Wallet, *LedgerEntry, error) {
	switch {
	case p.WalletID == uuid.Nil || p.PlayerID == uuid.Nil:
		return nil, nil, invalid("wallet and player ids are required")
	case !p.Initial.Valid():
		return nil, nil, invalid("initial balance is required")
	case p.Initial.IsNegative():
		return nil, nil, invalid("initial balance must not be negative")
	case p.Now.IsZero():
		return nil, nil, invalid("timestamp is required")
	}
	now := p.Now.UTC()
	w := &Wallet{id: p.WalletID, playerID: p.PlayerID, balance: p.Initial, version: 1, createdAt: now, updatedAt: now}
	if p.Initial.IsZero() {
		return w, nil, nil
	}
	zero, _ := money.Zero(p.Initial.Currency())
	e, err := NewLedgerEntry(p.EntryID, w.id, p.OpeningTxID, Credit, p.Initial, zero, p.Initial, 1, now)
	if err != nil {
		return nil, nil, err
	}
	return w, &e, nil
}

// RehydrateWallet rebuilds a stored wallet without side effects.
func RehydrateWallet(id, playerID uuid.UUID, balance money.Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	switch {
	case id == uuid.Nil || playerID == uuid.Nil:
		return nil, invalid("wallet and player ids are required")
	case !balance.Valid() || balance.IsNegative():
		return nil, invalid("stored balance is invalid")
	case version < 1:
		return nil, invalid("stored version must be >= 1")
	}
	return &Wallet{id: id, playerID: playerID, balance: balance, version: version, createdAt: createdAt, updatedAt: updatedAt}, nil
}

func (w *Wallet) ID() uuid.UUID        { return w.id }
func (w *Wallet) PlayerID() uuid.UUID  { return w.playerID }
func (w *Wallet) Currency() string     { return w.balance.Currency() }
func (w *Wallet) Balance() money.Money { return w.balance }
func (w *Wallet) Version() int64       { return w.version }
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }

func (w *Wallet) Debit(entryID, txID uuid.UUID, amount money.Money, now time.Time) (LedgerEntry, error) {
	return w.move(entryID, txID, Debit, amount, now)
}

func (w *Wallet) Credit(entryID, txID uuid.UUID, amount money.Money, now time.Time) (LedgerEntry, error) {
	return w.move(entryID, txID, Credit, amount, now)
}

// move changes nothing unless every check passes.
func (w *Wallet) move(entryID, txID uuid.UUID, dir Direction, amount money.Money, now time.Time) (LedgerEntry, error) {
	if !amount.Valid() || !amount.IsPositive() {
		return LedgerEntry{}, invalid("movement amount must be positive")
	}
	if amount.Currency() != w.Currency() {
		return LedgerEntry{}, fmt.Errorf("%w: wallet %s, movement %s", money.ErrCurrencyMismatch, w.Currency(), amount.Currency())
	}
	var after money.Money
	var err error
	if dir == Debit {
		after, err = w.balance.Sub(amount)
		if err == nil && after.IsNegative() {
			return LedgerEntry{}, ErrInsufficientFunds
		}
	} else {
		after, err = w.balance.Add(amount)
	}
	if err != nil {
		return LedgerEntry{}, err
	}
	e, err := NewLedgerEntry(entryID, w.id, txID, dir, amount, w.balance, after, w.version+1, now)
	if err != nil {
		return LedgerEntry{}, err
	}
	w.balance = after
	w.version++
	if now.After(w.updatedAt) {
		w.updatedAt = now.UTC()
	}
	return e, nil
}

// LedgerEntry is immutable once built.
type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	before        money.Money
	after         money.Money
	walletVersion int64
	createdAt     time.Time
}

// NewLedgerEntry enforces after = before ± amount.
func NewLedgerEntry(id, walletID, txID uuid.UUID, dir Direction, amount, before, after money.Money, walletVersion int64, at time.Time) (LedgerEntry, error) {
	if id == uuid.Nil || walletID == uuid.Nil || txID == uuid.Nil {
		return LedgerEntry{}, invalid("ledger entry ids are required")
	}
	if !amount.IsPositive() || before.IsNegative() || after.IsNegative() || at.IsZero() || walletVersion < 1 {
		return LedgerEntry{}, invalid("ledger entry values are invalid")
	}
	var want money.Money
	var err error
	switch dir {
	case Credit:
		want, err = before.Add(amount)
	case Debit:
		want, err = before.Sub(amount)
	default:
		return LedgerEntry{}, invalid("unknown direction %q", dir)
	}
	if err != nil {
		return LedgerEntry{}, err
	}
	if !want.Equal(after) {
		return LedgerEntry{}, invalid("balance after %s does not match %s %s %s", after, before, dir, amount)
	}
	return LedgerEntry{id: id, walletID: walletID, transactionID: txID, direction: dir, amount: amount,
		before: before, after: after, walletVersion: walletVersion, createdAt: at.UTC()}, nil
}

func (e LedgerEntry) ID() uuid.UUID              { return e.id }
func (e LedgerEntry) WalletID() uuid.UUID        { return e.walletID }
func (e LedgerEntry) TransactionID() uuid.UUID   { return e.transactionID }
func (e LedgerEntry) Direction() Direction       { return e.direction }
func (e LedgerEntry) Amount() money.Money        { return e.amount }
func (e LedgerEntry) BalanceBefore() money.Money { return e.before }
func (e LedgerEntry) BalanceAfter() money.Money  { return e.after }
func (e LedgerEntry) WalletVersion() int64       { return e.walletVersion }
func (e LedgerEntry) CreatedAt() time.Time       { return e.createdAt }
