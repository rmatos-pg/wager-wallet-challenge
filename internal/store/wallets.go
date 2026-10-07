package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"wager-wallet-service/internal/domain"
	"wager-wallet-service/internal/money"
)

func (q Queries) InsertWallet(ctx context.Context, w *domain.Wallet) error {
	_, err := q.q.Exec(ctx,
		`INSERT INTO wallets (id, player_id, currency, balance_units, version, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), w.Currency(), w.Balance().Units(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	return translate(err)
}

const walletCols = `id, player_id, currency, balance_units, version, created_at, updated_at`

func (q Queries) GetWallet(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	return scanWallet(q.q.QueryRow(ctx, `SELECT `+walletCols+` FROM wallets WHERE id = $1`, id))
}

// LockWallet takes the row lock that serializes every balance change of this
// wallet. Other wallets are not affected.
func (q Queries) LockWallet(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	return scanWallet(q.q.QueryRow(ctx, `SELECT `+walletCols+` FROM wallets WHERE id = $1 FOR UPDATE`, id))
}

// UpdateBalance is conditional on the version read under the lock. With the
// lock it always matches; without it, it still prevents a lost update.
func (q Queries) UpdateBalance(ctx context.Context, w *domain.Wallet, prevVersion int64) error {
	tag, err := q.q.Exec(ctx,
		`UPDATE wallets SET balance_units = $3, version = $4, updated_at = $5 WHERE id = $1 AND version = $2`,
		w.ID(), prevVersion, w.Balance().Units(), w.Version(), w.UpdatedAt())
	if err != nil {
		return translate(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrStaleVersion
	}
	return nil
}

func scanWallet(row pgxRow) (*domain.Wallet, error) {
	var (
		id, player uuid.UUID
		currency   string
		units, ver int64
		created    time.Time
		updated    time.Time
	)
	if err := row.Scan(&id, &player, &currency, &units, &ver, &created, &updated); err != nil {
		return nil, translate(err)
	}
	balance, err := money.FromUnits(units, currency)
	if err != nil {
		return nil, err
	}
	return domain.RehydrateWallet(id, player, balance, ver, created, updated)
}

type pgxRow interface{ Scan(dest ...any) error }

func (q Queries) InsertEntry(ctx context.Context, w *domain.Wallet, e domain.LedgerEntry) error {
	_, err := q.q.Exec(ctx,
		`INSERT INTO wallet_ledger_entries (id, wallet_id, player_id, currency, transaction_id, direction,
		   amount_units, balance_before_units, balance_after_units, wallet_version, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		e.ID(), e.WalletID(), w.PlayerID(), w.Currency(), e.TransactionID(), e.Direction(),
		e.Amount().Units(), e.BalanceBefore().Units(), e.BalanceAfter().Units(), e.WalletVersion(), e.CreatedAt())
	return translate(err)
}

type LedgerItem struct {
	Seq   int64
	Entry domain.LedgerEntry
}

// ListLedger returns entries after seq in stable seq order.
func (q Queries) ListLedger(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]LedgerItem, error) {
	rows, err := q.q.Query(ctx,
		`SELECT seq, id, transaction_id, direction, currency, amount_units, balance_before_units,
		        balance_after_units, wallet_version, created_at
		   FROM wallet_ledger_entries WHERE wallet_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`,
		walletID, afterSeq, limit)
	if err != nil {
		return nil, translate(err)
	}
	defer rows.Close()
	var out []LedgerItem
	for rows.Next() {
		var (
			seq, amt, before, after, ver int64
			id, txID                     uuid.UUID
			dir, cur                     string
			at                           time.Time
		)
		if err := rows.Scan(&seq, &id, &txID, &dir, &cur, &amt, &before, &after, &ver, &at); err != nil {
			return nil, err
		}
		m := func(u int64) money.Money { v, _ := money.FromUnits(u, cur); return v }
		e, err := domain.NewLedgerEntry(id, walletID, txID, domain.Direction(dir), m(amt), m(before), m(after), ver, at)
		if err != nil {
			return nil, err
		}
		out = append(out, LedgerItem{Seq: seq, Entry: e})
	}
	return out, translate(rows.Err())
}

// LedgerTotals returns credits - debits (in cents) and the entry count.
func (q Queries) LedgerTotals(ctx context.Context, walletID uuid.UUID) (net int64, count int, err error) {
	err = q.q.QueryRow(ctx,
		`SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount_units ELSE -amount_units END), 0)::bigint, COUNT(*)
		   FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&net, &count)
	return net, count, translate(err)
}

func (q Queries) InsertEvents(ctx context.Context, events []domain.Event) error {
	for _, e := range events {
		payload, err := json.Marshal(e)
		if err != nil {
			return err
		}
		var causation *string
		if e.CausationID != "" {
			causation = &e.CausationID
		}
		_, err = q.q.Exec(ctx,
			`INSERT INTO outbox_events (id, aggregate_id, event_type, event_version, correlation_id, causation_id,
			   payload, occurred_at, next_attempt_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)`,
			e.EventID, e.AggregateID, e.EventType, e.Version, e.CorrelationID, causation, payload, e.OccurredAt)
		if err != nil {
			return translate(err)
		}
	}
	return nil
}
