//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"

	"wager-wallet-service/internal/config"
	"wager-wallet-service/internal/domain"
	"wager-wallet-service/internal/queue"
	"wager-wallet-service/internal/service"
)

type memPublisher struct {
	mu   sync.Mutex
	seen map[uuid.UUID]int
	fail bool
}

func (p *memPublisher) Publish(_ context.Context, id, _ uuid.UUID, _ string, _ []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail {
		return errors.New("broker unavailable")
	}
	if p.seen == nil {
		p.seen = map[uuid.UUID]int{}
	}
	p.seen[id]++
	return nil
}

func drainOutbox(t *testing.T, relays ...*service.Relay) {
	t.Helper()
	var wg sync.WaitGroup
	for _, r := range relays {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				more, err := r.PublishBatch(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				if !more {
					return
				}
			}
		}()
	}
	wg.Wait()
}

func backlog(t *testing.T) int {
	t.Helper()
	n, _, err := st.OutboxBacklog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestOutboxTwoRelaysPublishEachEventOnce(t *testing.T) {
	e := newEnv(fastRetry)
	drainOutbox(t, service.NewRelay(st, &memPublisher{}, "warmup", time.Minute, 100, quiet))
	for range 20 {
		w := e.open(t, "100.00")
		e.do(t, w, op{kind: domain.KindBet, ext: uniq("bet"), amount: "5.00"})
	}
	want := backlog(t)
	pub := &memPublisher{}
	drainOutbox(t, service.NewRelay(st, pub, "relay-1", time.Minute, 7, quiet), service.NewRelay(st, pub, "relay-2", time.Minute, 7, quiet))

	if backlog(t) != 0 || len(pub.seen) != want {
		t.Fatalf("backlog=%d published=%d want=%d", backlog(t), len(pub.seen), want)
	}
	for id, n := range pub.seen {
		if n != 1 {
			t.Fatalf("event %s published %d times", id, n)
		}
	}
}

func TestOutboxRecoversExpiredLease(t *testing.T) {
	e := newEnv(fastRetry)
	ctx := context.Background()
	drainOutbox(t, service.NewRelay(st, &memPublisher{}, "warmup", time.Minute, 100, quiet))
	w := e.open(t, "100.00")
	e.do(t, w, op{kind: domain.KindBet, ext: uniq("bet"), amount: "5.00"})

	// An instance claims the events, publishes them, and dies before marking.
	claimed, err := st.ClaimOutbox(ctx, "dead-instance", 300*time.Millisecond, 100)
	if err != nil || len(claimed) == 0 {
		t.Fatalf("claim: %d %v", len(claimed), err)
	}
	pub := &memPublisher{}
	for _, row := range claimed {
		_ = pub.Publish(ctx, row.ID, row.AggregateID, row.EventType, row.Payload)
	}
	relay := service.NewRelay(st, pub, "survivor", time.Minute, 100, quiet)
	if more, _ := relay.PublishBatch(ctx); more {
		t.Fatal("leased rows must not be claimed by another relay")
	}
	time.Sleep(400 * time.Millisecond)
	drainOutbox(t, relay)

	if backlog(t) != 0 {
		t.Fatal("expired lease was not recovered")
	}
	for _, row := range claimed {
		if pub.seen[row.ID] != 2 {
			t.Fatalf("event %s published %d times, want 2 (same eventId)", row.ID, pub.seen[row.ID])
		}
	}
}

func TestOutboxFailureBacksOff(t *testing.T) {
	e := newEnv(fastRetry)
	ctx := context.Background()
	drainOutbox(t, service.NewRelay(st, &memPublisher{}, "warmup", time.Minute, 100, quiet))
	e.open(t, "100.00")

	broken := service.NewRelay(st, &memPublisher{fail: true}, "relay", time.Minute, 100, quiet)
	if _, err := broken.PublishBatch(ctx); err != nil {
		t.Fatal(err)
	}
	if more, _ := broken.PublishBatch(ctx); more {
		t.Fatal("failed events must wait for their backoff")
	}
	if n := count(t, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL AND attempts = 1
		AND last_error IS NOT NULL AND locked_by IS NULL AND next_attempt_at > now()`); n != backlog(t) || n == 0 {
		t.Fatalf("failed rows = %d backlog = %d", n, backlog(t))
	}
	_, _ = pool.Exec(ctx, `UPDATE outbox_events SET next_attempt_at = now() WHERE published_at IS NULL`)
	drainOutbox(t, service.NewRelay(st, &memPublisher{}, "relay", time.Minute, 100, quiet))
	if backlog(t) != 0 {
		t.Fatal("events not published after recovery")
	}
}

func TestOutboxPublishesToSQS(t *testing.T) {
	e := newEnv(fastRetry)
	ctx := context.Background()
	c := sqsClient(t)
	events, _ := queues(t, c, 5)
	drainOutbox(t, service.NewRelay(st, &memPublisher{}, "warmup", time.Minute, 100, quiet))
	w := e.open(t, "100.00")
	bet := e.do(t, w, op{kind: domain.KindBet, ext: uniq("bet"), amount: "5.00"})

	drainOutbox(t, service.NewRelay(st, queue.NewPublisher(c, config.Config{EventsQueueURL: events}), "relay", time.Minute, 100, quiet))
	got := map[string]domain.Event{}
	processedKey := domain.EventTransactionProcessed + "/" + bet.Tx.ID().String()
	balanceKey := domain.EventBalanceChanged + "/" + w.id.String()
	deadline := time.Now().Add(10 * time.Second)
	for (got[processedKey].EventID == uuid.Nil || got[balanceKey].EventID == uuid.Nil) && time.Now().Before(deadline) {
		out, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(events), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range out.Messages {
			var ev domain.Event
			if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &ev); err != nil {
				t.Fatal(err)
			}
			got[ev.EventType+"/"+ev.AggregateID.String()] = ev
			_, _ = c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(events), ReceiptHandle: m.ReceiptHandle})
		}
	}
	ev, ok := got[processedKey]
	if !ok || ev.EventID == uuid.Nil || ev.CorrelationID == "" || ev.Version != 1 {
		t.Fatalf("processed event missing or incomplete: %v", got)
	}
	if _, ok := got[balanceKey]; !ok {
		t.Fatalf("balance event missing: %v", got)
	}
}
