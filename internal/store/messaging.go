package store

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// --- inbox ---

// FindInbox returns the stored hash of a handled message.
func (q Queries) FindInbox(ctx context.Context, consumer, messageID string) (hash string, err error) {
	err = q.q.QueryRow(ctx, `SELECT payload_hash FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`,
		consumer, messageID).Scan(&hash)
	return hash, translate(err)
}

// InsertInbox records a handled message. It runs in the same SQL transaction
// as the operation, so "handled" and "applied" commit together. A second
// insert of the same message fails with ErrDuplicate.
func (q Queries) InsertInbox(ctx context.Context, consumer, messageID, hash string, receivedAt time.Time) error {
	_, err := q.q.Exec(ctx, `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at, processed_at)
		VALUES ($1, $2, $3, $4, now())`, consumer, messageID, hash, receivedAt)
	return translate(err)
}

// --- outbox ---

type OutboxRow struct {
	ID          uuid.UUID
	AggregateID uuid.UUID
	EventType   string
	Payload     []byte
	Attempts    int
}

// ClaimOutbox leases up to limit due events to owner in one statement.
// SKIP LOCKED: concurrent publishers never wait for or double-claim a row.
// An expired lease (publisher died) makes the row claimable again. Times use
// the database clock so instances agree on expiry.
func (q Queries) ClaimOutbox(ctx context.Context, owner string, lease time.Duration, limit int) ([]OutboxRow, error) {
	rows, err := q.q.Query(ctx, `
		UPDATE outbox_events o
		   SET locked_by = $2, locked_until = now() + make_interval(secs => $3), attempts = o.attempts + 1
		  FROM (SELECT id FROM outbox_events
		         WHERE published_at IS NULL AND next_attempt_at <= now()
		           AND (locked_until IS NULL OR locked_until < now())
		         ORDER BY next_attempt_at LIMIT $1
		         FOR UPDATE SKIP LOCKED) due
		 WHERE o.id = due.id
		RETURNING o.id, o.aggregate_id, o.event_type, o.payload, o.attempts`,
		limit, owner, lease.Seconds())
	if err != nil {
		return nil, translate(err)
	}
	defer rows.Close()
	var out []OutboxRow
	for rows.Next() {
		var r OutboxRow
		if err := rows.Scan(&r.ID, &r.AggregateID, &r.EventType, &r.Payload, &r.Attempts); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, translate(rows.Err())
}

// MarkPublished ignores the lease: once the broker accepted the event, it is
// published, even if the lease expired in the meantime.
func (q Queries) MarkPublished(ctx context.Context, eventID uuid.UUID) error {
	_, err := q.q.Exec(ctx, `UPDATE outbox_events SET published_at = now(), locked_by = NULL, locked_until = NULL,
		last_error = NULL WHERE id = $1 AND published_at IS NULL`, eventID)
	return translate(err)
}

// MarkFailed reschedules only while owner still holds the lease, so a slow
// publisher cannot push back a row someone else took over.
func (q Queries) MarkFailed(ctx context.Context, eventID uuid.UUID, owner string, retryIn time.Duration, reason string) error {
	if len(reason) > 500 {
		reason = reason[:500]
	}
	_, err := q.q.Exec(ctx, `UPDATE outbox_events
		SET next_attempt_at = now() + make_interval(secs => $3), locked_by = NULL, locked_until = NULL, last_error = $4
		WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`, eventID, owner, retryIn.Seconds(), reason)
	return translate(err)
}

// OutboxBacklog returns unpublished events and the age of the oldest one.
func (q Queries) OutboxBacklog(ctx context.Context) (int, time.Duration, error) {
	var n int
	var age float64
	err := q.q.QueryRow(ctx, `SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(occurred_at)), 0)::float8
		FROM outbox_events WHERE published_at IS NULL`).Scan(&n, &age)
	return n, time.Duration(age * float64(time.Second)), translate(err)
}
