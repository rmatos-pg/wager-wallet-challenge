package store

import (
	"context"
	"time"

	"github.com/google/uuid"

	"wager-wallet-service/internal/domain"
	"wager-wallet-service/internal/money"
)

const txCols = `id, origin, kind, status, wallet_id, player_id, currency, amount_units,
	provider_id, external_id, idempotency_key, payload_hash, round_id, game_id,
	reference_external_id, reference_id, failure_code, result_balance_units,
	attempts, next_attempt_at, expires_at, created_at, updated_at, completed_at`

func (q Queries) InsertTx(ctx context.Context, t *domain.Transaction) error {
	s := t.State()
	_, err := q.q.Exec(ctx, `INSERT INTO wager_transactions (`+txCols+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)`,
		s.ID, s.Origin, s.Kind, s.Status, s.WalletID, s.PlayerID, s.Money.Currency(), s.Money.Units(),
		str(s.ProviderID), str(s.ExternalID), str(s.IdempotencyKey), str(s.PayloadHash), str(s.RoundID), str(s.GameID),
		str(s.ReferenceExternalID), id(s.ReferenceID), str(string(s.FailureCode)), units(s.ResultBalance),
		s.Attempts, ts(s.NextAttemptAt), ts(s.ExpiresAt), s.CreatedAt, s.UpdatedAt, ts(s.CompletedAt))
	return translate(err)
}

// UpdateTx writes the mutable columns. The trigger rejects anything else and
// any change to a terminal row.
func (q Queries) UpdateTx(ctx context.Context, t *domain.Transaction) error {
	s := t.State()
	_, err := q.q.Exec(ctx, `UPDATE wager_transactions
		SET status = $2, reference_id = $3, failure_code = $4, result_balance_units = $5,
		    attempts = $6, next_attempt_at = $7, expires_at = $8, updated_at = $9, completed_at = $10
		WHERE id = $1`,
		s.ID, s.Status, id(s.ReferenceID), str(string(s.FailureCode)), units(s.ResultBalance),
		s.Attempts, ts(s.NextAttemptAt), ts(s.ExpiresAt), s.UpdatedAt, ts(s.CompletedAt))
	return translate(err)
}

func (q Queries) GetTx(ctx context.Context, txID uuid.UUID) (*domain.Transaction, error) {
	return scanTx(q.q.QueryRow(ctx, `SELECT `+txCols+` FROM wager_transactions WHERE id = $1`, txID))
}

func (q Queries) FindByKey(ctx context.Context, provider, key string) (*domain.Transaction, error) {
	return scanTx(q.q.QueryRow(ctx, `SELECT `+txCols+` FROM wager_transactions
		WHERE origin = 'EXTERNAL' AND provider_id = $1 AND idempotency_key = $2`, provider, key))
}

func (q Queries) FindByExternalID(ctx context.Context, provider, externalID string) (*domain.Transaction, error) {
	return scanTx(q.q.QueryRow(ctx, `SELECT `+txCols+` FROM wager_transactions
		WHERE origin = 'EXTERNAL' AND provider_id = $1 AND external_id = $2`, provider, externalID))
}

func (q Queries) HasProcessedReversal(ctx context.Context, referenceID uuid.UUID) (bool, error) {
	var ok bool
	err := q.q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM wager_transactions
		WHERE reference_id = $1 AND kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED')`, referenceID).Scan(&ok)
	return ok, translate(err)
}

// ClaimDuePending locks one due PENDING_REFERENCE row. SKIP LOCKED lets every
// instance run the worker without picking the same row. NO KEY UPDATE does
// not block the KEY SHARE lock taken by FKs pointing at this row.
func (q Queries) ClaimDuePending(ctx context.Context) (*domain.Transaction, error) {
	return scanTx(q.q.QueryRow(ctx, `SELECT `+txCols+` FROM wager_transactions
		WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= now()
		ORDER BY next_attempt_at LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED`))
}

func scanTx(row pgxRow) (*domain.Transaction, error) {
	var (
		s                                                           domain.TxState
		origin, kind, status, currency                              string
		amount                                                      int64
		provider, external, key, hash, round, game, refExt, failure *string
		refID                                                       *uuid.UUID
		result                                                      *int64
		next, expires, completed                                    *time.Time
	)
	err := row.Scan(&s.ID, &origin, &kind, &status, &s.WalletID, &s.PlayerID, &currency, &amount,
		&provider, &external, &key, &hash, &round, &game, &refExt, &refID, &failure, &result,
		&s.Attempts, &next, &expires, &s.CreatedAt, &s.UpdatedAt, &completed)
	if err != nil {
		return nil, translate(err)
	}
	s.Origin, s.Kind, s.Status = domain.Origin(origin), domain.Kind(kind), domain.Status(status)
	if s.Money, err = money.FromUnits(amount, currency); err != nil {
		return nil, err
	}
	if result != nil {
		s.ResultBalance, _ = money.FromUnits(*result, currency)
	}
	s.ProviderID, s.ExternalID, s.IdempotencyKey, s.PayloadHash = val(provider), val(external), val(key), val(hash)
	s.RoundID, s.GameID, s.ReferenceExternalID = val(round), val(game), val(refExt)
	s.FailureCode = domain.FailureCode(val(failure))
	if refID != nil {
		s.ReferenceID = *refID
	}
	s.NextAttemptAt, s.ExpiresAt, s.CompletedAt = tval(next), tval(expires), tval(completed)
	return domain.Rehydrate(s)
}

// NULL helpers: empty Go values are stored as NULL.

func str(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func id(u uuid.UUID) *uuid.UUID {
	if u == uuid.Nil {
		return nil
	}
	return &u
}

func ts(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func units(m money.Money) *int64 {
	if !m.Valid() {
		return nil
	}
	u := m.Units()
	return &u
}

func val(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func tval(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}
