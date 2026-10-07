package service

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"wager-wallet-service/internal/domain"
	"wager-wallet-service/internal/money"
	"wager-wallet-service/internal/obs"
	"wager-wallet-service/internal/store"
)

type Wallets struct {
	st  *store.Store
	log *slog.Logger
}

func NewWallets(st *store.Store, log *slog.Logger) *Wallets { return &Wallets{st: st, log: log} }

// Open creates the wallet and, for a positive balance, the OPENING
// transaction, its credit entry and its events, all in one commit.
func (s *Wallets) Open(ctx context.Context, playerID uuid.UUID, initial money.Money) (*domain.Wallet, error) {
	now := time.Now().UTC()
	openingID := newID()
	w, entry, err := domain.Open(domain.OpenWallet{WalletID: newID(), PlayerID: playerID, Initial: initial,
		OpeningTxID: openingID, EntryID: newID(), Now: now})
	if err != nil {
		return nil, err
	}
	err = s.st.InTx(ctx, func(tx *store.Tx) error {
		if err := tx.InsertWallet(ctx, w); err != nil {
			if errors.Is(err, store.ErrDuplicate) {
				return ErrWalletExists
			}
			return err
		}
		if entry == nil {
			return nil
		}
		opening, err := domain.NewOpening(openingID, w.ID(), playerID, initial, now)
		if err != nil {
			return err
		}
		if err := tx.InsertTx(ctx, opening); err != nil {
			return err
		}
		if err := tx.InsertEntry(ctx, w, *entry); err != nil {
			return err
		}
		meta := domain.EventMeta{CorrelationID: openingID.String(), At: now}
		return tx.InsertEvents(ctx, domain.EventsFor(newID, meta, opening, entry))
	})
	if err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "wallet opened", "walletId", w.ID(), "playerId", playerID)
	return w, nil
}

func (s *Wallets) Get(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	w, err := s.st.GetWallet(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrWalletNotFound
	}
	return w, err
}

const (
	DefaultLedgerLimit = 50
	MaxLedgerLimit     = 200
)

// Ledger pages through entries in commit order. The cursor is opaque to
// clients (base64 of the last seq).
func (s *Wallets) Ledger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) ([]domain.LedgerEntry, string, error) {
	if _, err := s.Get(ctx, walletID); err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = DefaultLedgerLimit
	}
	if limit > MaxLedgerLimit {
		return nil, "", invalidf("limit must be <= %d", MaxLedgerLimit)
	}
	after, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	items, err := s.st.ListLedger(ctx, walletID, after, limit+1)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(items) > limit {
		items = items[:limit]
		next = encodeCursor(items[len(items)-1].Seq)
	}
	out := make([]domain.LedgerEntry, len(items))
	for i, it := range items {
		out[i] = it.Entry
	}
	return out, next, nil
}

func encodeCursor(seq int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte("seq:" + strconv.FormatInt(seq, 10)))
}

func decodeCursor(c string) (int64, error) {
	if c == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err == nil {
		if v, ok := strings.CutPrefix(string(raw), "seq:"); ok {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
				return n, nil
			}
		}
	}
	return 0, invalidf("invalid cursor")
}

type Reconciliation struct {
	WalletID   uuid.UUID
	Stored     money.Money
	Calculated money.Money
	Difference money.Money
	Consistent bool
	Checked    int
}

// Reconcile rebuilds the balance from the ledger (opening included) and
// compares it with the stored balance in one snapshot. It never writes.
func (s *Wallets) Reconcile(ctx context.Context, walletID uuid.UUID) (Reconciliation, error) {
	var r Reconciliation
	err := s.st.InSnapshot(ctx, func(tx *store.Tx) error {
		w, err := tx.GetWallet(ctx, walletID)
		if errors.Is(err, store.ErrNotFound) {
			return ErrWalletNotFound
		}
		if err != nil {
			return err
		}
		net, n, err := tx.LedgerTotals(ctx, walletID)
		if err != nil {
			return err
		}
		calc, err := money.FromUnits(net, w.Currency())
		if err != nil {
			return err
		}
		diff, err := w.Balance().Sub(calc)
		if err != nil {
			return err
		}
		r = Reconciliation{WalletID: walletID, Stored: w.Balance(), Calculated: calc, Difference: diff,
			Consistent: diff.IsZero(), Checked: n}
		return nil
	})
	if err != nil {
		return Reconciliation{}, err
	}
	if r.Consistent {
		obs.Reconciliations.WithLabelValues("consistent").Inc()
	} else {
		obs.Reconciliations.WithLabelValues("divergent").Inc()
		s.log.ErrorContext(ctx, "wallet balance diverges from ledger", "walletId", walletID,
			"stored", r.Stored.Amount(), "calculated", r.Calculated.Amount(), "difference", r.Difference.Amount())
	}
	return r, nil
}
