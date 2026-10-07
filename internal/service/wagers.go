package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"

	"wager-wallet-service/internal/domain"
	"wager-wallet-service/internal/obs"
	"wager-wallet-service/internal/store"
)

type Wagers struct {
	st     *store.Store
	policy domain.RetryPolicy
	log    *slog.Logger
}

func NewWagers(st *store.Store, policy domain.RetryPolicy, log *slog.Logger) *Wagers {
	return &Wagers{st: st, policy: policy, log: log}
}

// InboxEntry is set when the operation came from SQS: the message is recorded
// as handled in the same SQL transaction.
type InboxEntry struct {
	Consumer   string
	MessageID  string
	Hash       string
	ReceivedAt time.Time
}

type Options struct {
	CausationID string // HTTP request id or SQS message id
	Inbox       *InboxEntry
}

type Result struct {
	Tx     *domain.Transaction
	Replay bool
}

// Process applies a provider operation exactly once. See docs/DESIGN.md
// sections 3 to 5 for the transaction boundary, locking and idempotency.
func (s *Wagers) Process(ctx context.Context, req domain.Request, opt Options) (Result, error) {
	start := time.Now()
	if err := req.Validate(); err != nil {
		return Result{}, err
	}
	ctx = obs.With(ctx, "providerId", req.ProviderID, "walletId", req.WalletID, "correlationId", req.IdempotencyKey)

	res, err := s.process(ctx, req, opt)
	if errors.Is(err, store.ErrDuplicate) || errors.Is(err, store.ErrStaleVersion) {
		// A concurrent duplicate committed first: answer with its result.
		obs.ConcurrencyConflicts.Inc()
		var found bool
		res, found, err = s.lookup(ctx, s.st.Queries, req)
		if err == nil && !found {
			err = fmt.Errorf("concurrent update, retry: %w", store.ErrStaleVersion)
		}
		if err == nil {
			err = s.recordReplayInbox(ctx, opt)
		}
	}
	if err != nil {
		return Result{}, err
	}

	st := res.Tx.State()
	obs.Operations.WithLabelValues(string(st.Kind), string(st.Status), strconv.FormatBool(res.Replay)).Inc()
	obs.ProcessingSeconds.Observe(time.Since(start).Seconds())
	s.log.InfoContext(ctx, "wager operation handled", "transactionId", st.ID, "kind", st.Kind,
		"status", st.Status, "failureCode", st.FailureCode, "idempotentReplay", res.Replay)
	return res, nil
}

func (s *Wagers) process(ctx context.Context, req domain.Request, opt Options) (Result, error) {
	if res, found, err := s.lookup(ctx, s.st.Queries, req); err != nil || found {
		if err == nil {
			err = s.recordReplayInbox(ctx, opt)
		}
		return res, err
	}

	var res Result
	err := s.st.InTx(ctx, func(tx *store.Tx) error {
		w, err := tx.LockWallet(ctx, req.WalletID)
		if errors.Is(err, store.ErrNotFound) {
			return ErrWalletNotFound
		}
		if err != nil {
			return err
		}
		if w.PlayerID() != req.PlayerID || w.Currency() != req.Money.Currency() {
			return ErrWalletMismatch
		}

		// Look again under the lock: a duplicate may have committed while we waited.
		if r, found, err := s.lookup(ctx, tx.Queries, req); err != nil || found {
			res = r
			if err == nil && opt.Inbox != nil {
				err = tx.InsertInbox(ctx, opt.Inbox.Consumer, opt.Inbox.MessageID, opt.Inbox.Hash, opt.Inbox.ReceivedAt)
			}
			return err
		}

		now := time.Now().UTC()
		t, err := domain.NewExternal(newID(), req, now)
		if err != nil {
			return err
		}
		prevVersion := w.Version()
		entry, wait, err := s.settle(ctx, tx.Queries, w, t, now)
		if err != nil {
			return err
		}
		if wait != "" {
			if err := t.AwaitReference(now.Add(s.policy.Delay(0)), now.Add(s.policy.TTL), now); err != nil {
				return err
			}
		}
		if err := tx.InsertTx(ctx, t); err != nil {
			return err
		}
		if err := saveMovement(ctx, tx, w, prevVersion, entry); err != nil {
			return err
		}
		meta := domain.EventMeta{CorrelationID: req.IdempotencyKey, CausationID: opt.CausationID, At: now}
		if err := tx.InsertEvents(ctx, domain.EventsFor(newID, meta, t, entry)); err != nil {
			return err
		}
		if opt.Inbox != nil {
			if err := tx.InsertInbox(ctx, opt.Inbox.Consumer, opt.Inbox.MessageID, opt.Inbox.Hash, opt.Inbox.ReceivedAt); err != nil {
				return err
			}
		}
		res = Result{Tx: t}
		return nil
	})
	return res, err
}

// lookup finds an earlier operation for this request. found=true with a nil
// error is a valid replay.
func (s *Wagers) lookup(ctx context.Context, q store.Queries, req domain.Request) (Result, bool, error) {
	t, err := q.FindByKey(ctx, req.ProviderID, req.IdempotencyKey)
	if errors.Is(err, store.ErrNotFound) {
		t, err = q.FindByExternalID(ctx, req.ProviderID, req.ExternalID)
		if err == nil && t.State().IdempotencyKey != req.IdempotencyKey {
			return Result{}, true, ErrExternalIDReused
		}
	}
	switch {
	case errors.Is(err, store.ErrNotFound):
		return Result{}, false, nil
	case err != nil:
		return Result{}, false, err
	case t.State().PayloadHash != req.Hash():
		return Result{}, true, ErrIdempotencyConflict
	}
	return Result{Tx: t, Replay: true}, true, nil
}

// recordReplayInbox marks a redelivered message as handled when the operation
// itself was already applied. Already recorded is fine.
func (s *Wagers) recordReplayInbox(ctx context.Context, opt Options) error {
	if opt.Inbox == nil {
		return nil
	}
	err := s.st.InsertInbox(ctx, opt.Inbox.Consumer, opt.Inbox.MessageID, opt.Inbox.Hash, opt.Inbox.ReceivedAt)
	if errors.Is(err, store.ErrDuplicate) {
		return nil
	}
	return err
}

// settle decides the outcome while the wallet is locked. If wait is not
// empty the reference is not available yet and t was left untouched; wait is
// the rejection code to use if the waiting window expires.
func (s *Wagers) settle(ctx context.Context, q store.Queries, w *domain.Wallet, t *domain.Transaction, now time.Time) (*domain.LedgerEntry, domain.FailureCode, error) {
	st := t.State()
	var ref *domain.Transaction
	if st.ReferenceExternalID != "" {
		r, err := q.FindByExternalID(ctx, st.ProviderID, st.ReferenceExternalID)
		if errors.Is(err, store.ErrNotFound) {
			return nil, domain.FailureReferenceNotFound, nil
		}
		if err != nil {
			return nil, "", err
		}
		if err := t.CheckReference(r); err != nil {
			if errors.Is(err, domain.ErrReferenceNotReady) {
				return nil, domain.FailureReferenceTimeout, nil
			}
			return nil, "", rejectOn(t, err, now)
		}
		if t.Kind().IsReversal() {
			done, err := q.HasProcessedReversal(ctx, r.ID())
			if err != nil {
				return nil, "", err
			}
			if done {
				return nil, "", t.Reject(domain.FailureAlreadyReversed, now)
			}
		}
		ref = r
	}

	var entry *domain.LedgerEntry
	var err error
	var e domain.LedgerEntry
	switch t.Effect(ref) {
	case domain.DebitEffect:
		e, err = w.Debit(newID(), t.ID(), t.Money(), now)
		entry = &e
	case domain.CreditEffect:
		e, err = w.Credit(newID(), t.ID(), t.Money(), now)
		entry = &e
	}
	if errors.Is(err, domain.ErrInsufficientFunds) {
		return nil, "", t.Reject(t.InsufficientFundsCode(), now)
	}
	if err != nil {
		return nil, "", err
	}
	refID := uuid.Nil
	if ref != nil {
		refID = ref.ID()
	}
	return entry, "", t.Process(w.Balance(), refID, now)
}

func rejectOn(t *domain.Transaction, err error, now time.Time) error {
	var r *domain.Rejection
	if errors.As(err, &r) {
		return t.Reject(r.Code, now)
	}
	return err
}

func saveMovement(ctx context.Context, tx *store.Tx, w *domain.Wallet, prevVersion int64, entry *domain.LedgerEntry) error {
	if entry == nil {
		return nil
	}
	if err := tx.UpdateBalance(ctx, w, prevVersion); err != nil {
		return err
	}
	return tx.InsertEntry(ctx, w, *entry)
}

func (s *Wagers) Get(ctx context.Context, id uuid.UUID) (*domain.Transaction, error) {
	t, err := s.st.GetTx(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrNotFound
	}
	return t, err
}

func (s *Wagers) GetByExternalID(ctx context.Context, provider, externalID string) (*domain.Transaction, error) {
	t, err := s.st.FindByExternalID(ctx, provider, externalID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrNotFound
	}
	return t, err
}
