package service

import (
	"context"
	"errors"
	"time"

	"wager-wallet-service/internal/domain"
	"wager-wallet-service/internal/obs"
	"wager-wallet-service/internal/store"
)

// ResumePending retries one due PENDING_REFERENCE operation. It returns true
// when it found work, so the worker loops again without sleeping.
//
// Lock order: the transaction row (SKIP LOCKED), then the wallet. The HTTP/SQS
// path only locks wallets, so the two paths cannot deadlock.
func (s *Wagers) ResumePending(ctx context.Context) (bool, error) {
	var t *domain.Transaction
	err := s.st.InTx(ctx, func(tx *store.Tx) error {
		var err error
		t, err = tx.ClaimDuePending(ctx)
		if err != nil {
			return err
		}
		st := t.State()
		w, err := tx.LockWallet(ctx, st.WalletID)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		prevVersion := w.Version()
		entry, wait, err := s.settle(ctx, tx.Queries, w, t, now)
		if err != nil {
			return err
		}
		if wait != "" {
			if s.policy.Expired(t, now) {
				err = t.Reject(wait, now)
			} else {
				err = t.Reschedule(s.policy.Next(t, now), now)
			}
			if err != nil {
				return err
			}
		}
		if err := tx.UpdateTx(ctx, t); err != nil {
			return err
		}
		if err := saveMovement(ctx, tx, w, prevVersion, entry); err != nil {
			return err
		}
		if !t.Status().Terminal() {
			return nil // still waiting, nothing to announce
		}
		meta := domain.EventMeta{CorrelationID: st.IdempotencyKey, CausationID: "reference-retry", At: now}
		return tx.InsertEvents(ctx, domain.EventsFor(newID, meta, t, entry))
	})
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	st := t.State()
	obs.ReferenceRetries.WithLabelValues(string(st.Status)).Inc()
	if st.Status.Terminal() {
		obs.Operations.WithLabelValues(string(st.Kind), string(st.Status), "false").Inc()
		s.log.InfoContext(ctx, "pending reference settled", "transactionId", st.ID, "walletId", st.WalletID,
			"providerId", st.ProviderID, "status", st.Status, "failureCode", st.FailureCode, "attempts", st.Attempts)
	}
	return true, nil
}
