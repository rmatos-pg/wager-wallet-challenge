package service

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"wager-wallet-service/internal/domain"
	"wager-wallet-service/internal/obs"
	"wager-wallet-service/internal/store"
)

// Disposition tells the consumer what to do with a message.
type Disposition int

const (
	Ack        Disposition = iota // outcome committed (or already handled): delete
	Retry                         // transient failure: leave it, retry with backoff
	DeadLetter                    // will never succeed: move to the DLQ now
)

const ConsumerName = "wager-transactions-consumer"

type InboundMessage struct {
	MessageID  string
	Request    domain.Request
	ReceivedAt time.Time
}

type Messages struct {
	st     *store.Store
	wagers *Wagers
	log    *slog.Logger
}

func NewMessages(st *store.Store, wagers *Wagers, log *slog.Logger) *Messages {
	return &Messages{st: st, wagers: wagers, log: log}
}

// Handle runs an input message through the same use case as HTTP. The inbox
// row is written inside the operation's SQL transaction.
func (m *Messages) Handle(ctx context.Context, msg InboundMessage) (Disposition, string, error) {
	hash := MessageHash(msg.Request)
	stored, err := m.st.FindInbox(ctx, ConsumerName, msg.MessageID)
	switch {
	case err == nil && stored == hash:
		obs.SQSMessages.WithLabelValues("duplicate").Inc()
		return Ack, "", nil
	case err == nil:
		return DeadLetter, "message_id_conflict", errors.New("messageId reused with different content")
	case !errors.Is(err, store.ErrNotFound):
		return Retry, "", err
	}

	_, err = m.wagers.Process(ctx, msg.Request, Options{
		CausationID: msg.MessageID,
		Inbox:       &InboxEntry{Consumer: ConsumerName, MessageID: msg.MessageID, Hash: hash, ReceivedAt: msg.ReceivedAt},
	})
	if err == nil {
		obs.SQSMessages.WithLabelValues("ack").Inc()
		return Ack, "", nil
	}
	if reason := permanentReason(err); reason != "" {
		return DeadLetter, reason, err
	}
	obs.SQSMessages.WithLabelValues("retry").Inc()
	return Retry, "", err
}

// permanentReason classifies errors that no retry can fix.
func permanentReason(err error) string {
	switch {
	case errors.Is(err, domain.ErrInvalid):
		return "invalid_operation"
	case errors.Is(err, ErrWalletNotFound):
		return "wallet_not_found"
	case errors.Is(err, ErrWalletMismatch):
		return "wallet_mismatch"
	case errors.Is(err, ErrIdempotencyConflict):
		return "idempotency_conflict"
	case errors.Is(err, ErrExternalIDReused):
		return "external_id_reused"
	}
	return ""
}

// --- outbox relay ---

// Publisher sends one stored event to the broker.
type Publisher interface {
	Publish(ctx context.Context, eventID, aggregateID uuid.UUID, eventType string, payload []byte) error
}

// Relay publishes committed outbox events. Every instance runs one; the
// lease + SKIP LOCKED claim keeps them from publishing the same row at the
// same time, and an expired lease hands abandoned work to someone else.
type Relay struct {
	st    *store.Store
	pub   Publisher
	owner string
	lease time.Duration
	batch int
	log   *slog.Logger
}

func NewRelay(st *store.Store, pub Publisher, owner string, lease time.Duration, batch int, log *slog.Logger) *Relay {
	return &Relay{st: st, pub: pub, owner: owner, lease: lease, batch: batch, log: log}
}

// PublishBatch returns true when it claimed something.
func (r *Relay) PublishBatch(ctx context.Context) (bool, error) {
	rows, err := r.st.ClaimOutbox(ctx, r.owner, r.lease, r.batch)
	if err != nil {
		return false, err
	}
	for _, row := range rows {
		if err := r.pub.Publish(ctx, row.ID, row.AggregateID, row.EventType, row.Payload); err != nil {
			obs.OutboxFailures.Inc()
			delay := OutboxBackoff(row.Attempts)
			r.log.WarnContext(ctx, "publish failed", "eventId", row.ID, "eventType", row.EventType,
				"attempts", row.Attempts, "retryIn", delay.String(), "error", err)
			if err := r.st.MarkFailed(ctx, row.ID, r.owner, delay, err.Error()); err != nil {
				return true, err
			}
			continue
		}
		// If we crash here the lease expires and the event is published again
		// with the same eventId; consumers deduplicate by it.
		if err := r.st.MarkPublished(ctx, row.ID); err != nil {
			return true, err
		}
		obs.OutboxPublished.Inc()
	}
	if n, age, err := r.st.OutboxBacklog(ctx); err == nil {
		obs.OutboxPending.Set(float64(n))
		obs.OutboxLag.Set(age.Seconds())
	}
	return len(rows) > 0, nil
}

// OutboxBackoff: 1s, 2s, 4s ... capped at 1 minute. No attempt limit: a
// committed event must never be dropped.
func OutboxBackoff(attempts int) time.Duration {
	d := time.Second
	for i := 1; i < attempts && d < time.Minute; i++ {
		d *= 2
	}
	return min(d, time.Minute)
}
