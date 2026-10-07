//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"

	"wager-wallet-service/internal/config"
	"wager-wallet-service/internal/domain"
	"wager-wallet-service/internal/queue"
	"wager-wallet-service/internal/service"
	"wager-wallet-service/test/testkit"
)

func sqsClient(t *testing.T) *sqs.Client {
	t.Helper()
	for k, v := range map[string]string{"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"} {
		if os.Getenv(k) == "" {
			t.Setenv(k, v)
		}
	}
	c, err := queue.NewClient(context.Background(), config.Config{AWSRegion: "us-east-1",
		SQSEndpoint: testkit.Env("SQS_ENDPOINT", "http://localhost:4566")})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// queues creates a FIFO input queue with its own DLQ for one test, mirroring
// deploy/localstack/init-sqs.sh but with a configurable maxReceiveCount.
func queues(t *testing.T, c *sqs.Client, maxReceive int) (in, dlq string) {
	t.Helper()
	ctx := context.Background()
	name := "it-" + uuid.NewString()[:8]
	fifo := map[string]string{"FifoQueue": "true", "VisibilityTimeout": "30"}
	d, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name + "-dlq.fifo"), Attributes: fifo})
	if err != nil {
		t.Fatal(err)
	}
	arn, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: d.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatal(err)
	}
	attrs := map[string]string{"FifoQueue": "true", "VisibilityTimeout": "30",
		"RedrivePolicy": fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":"%d"}`, arn.Attributes["QueueArn"], maxReceive)}
	q, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name + ".fifo"), Attributes: attrs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = c.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: q.QueueUrl})
		_, _ = c.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: d.QueueUrl})
	})
	return *q.QueueUrl, *d.QueueUrl
}

func envelope(msgID string, data map[string]any) string {
	raw, _ := json.Marshal(map[string]any{"messageId": msgID, "type": queue.MessageType,
		"occurredAt": time.Now().UTC().Format(time.RFC3339), "data": data})
	return string(raw)
}

// wagerData is a message payload; key "" means "provider:externalId".
func (w wallet) wagerData(kind, ext, amount, key string) map[string]any {
	d := testkit.Wager("provider-a", ext, w.player.String(), w.id.String(), kind, amount, "")
	if key == "" {
		key = "provider-a:" + ext
	}
	d["idempotencyKey"] = key
	return d
}

func send(t *testing.T, c *sqs.Client, url, group, body string) {
	t.Helper()
	_, err := c.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: aws.String(url), MessageBody: aws.String(body),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(uuid.NewString())})
	if err != nil {
		t.Fatal(err)
	}
}

// pollUntil drives the consumer until cond holds.
func pollUntil(t *testing.T, c *queue.Consumer, timeout time.Duration, cond func() bool) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached while polling")
		}
		if _, err := c.Poll(ctx, ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func queueDepth(t *testing.T, c *sqs.Client, url string) int {
	t.Helper()
	out, err := c.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{QueueUrl: aws.String(url),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible}})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])
	b, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
	return a + b
}

func deadLetters(t *testing.T, c *sqs.Client, dlq string, want int) map[string]string {
	t.Helper()
	got := map[string]string{} // messageId -> reason
	deadline := time.Now().Add(15 * time.Second)
	for len(got) < want && time.Now().Before(deadline) {
		out, err := c.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: aws.String(dlq),
			MaxNumberOfMessages: 10, WaitTimeSeconds: 1, MessageAttributeNames: []string{"All"}})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range out.Messages {
			var env struct {
				MessageID string `json:"messageId"`
			}
			_ = json.Unmarshal([]byte(aws.ToString(m.Body)), &env)
			reason := "redrive"
			if a, ok := m.MessageAttributes["deadLetterReason"]; ok {
				reason = aws.ToString(a.StringValue)
			}
			got[env.MessageID] = reason
			_, _ = c.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(dlq), ReceiptHandle: m.ReceiptHandle})
		}
	}
	return got
}

func TestSQSConsumesAndSurvivesRedelivery(t *testing.T) {
	e := newEnv(fastRetry)
	c := sqsClient(t)
	in, dlq := queues(t, c, 5)
	consumer := queue.NewConsumer(c, in, dlq, e.messages, time.Second, quiet)
	w := e.open(t, "100.00")

	send(t, c, in, w.id.String(), envelope(uniq("m"), w.wagerData("BET", uniq("bet"), "10.00", "")))
	pollUntil(t, consumer, 15*time.Second, func() bool { return e.balance(t, w) == "90.00" })

	// Commit happened but the instance died before DeleteMessage: SQS redelivers.
	msgID, ext := uniq("m"), uniq("bet")
	body := envelope(msgID, w.wagerData("BET", ext, "20.00", ""))
	_, decoded, err := queue.Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	decoded.ReceivedAt = time.Now()
	if d, _, err := e.messages.Handle(context.Background(), decoded); d != service.Ack || err != nil {
		t.Fatalf("first delivery: %v %v", d, err)
	}
	send(t, c, in, w.id.String(), body)
	pollUntil(t, consumer, 15*time.Second, func() bool { return queueDepth(t, c, in) == 0 })

	if debits(t, w) != 2 {
		t.Fatalf("debits = %d", debits(t, w))
	}
	if n := count(t, `SELECT count(*) FROM inbox_messages WHERE message_id = $1`, msgID); n != 1 {
		t.Fatalf("inbox rows = %d", n)
	}
	e.consistent(t, w, "70.00")
}

func TestSQSDeadLetters(t *testing.T) {
	e := newEnv(fastRetry)
	c := sqsClient(t)
	in, dlq := queues(t, c, 5)
	consumer := queue.NewConsumer(c, in, dlq, e.messages, time.Second, quiet)
	w := e.open(t, "100.00")

	reused := uniq("m")
	e.messages.Handle(context.Background(), service.InboundMessage{MessageID: reused, ReceivedAt: time.Now(),
		Request: w.req(t, op{kind: domain.KindBet, ext: uniq("bet"), amount: "1.00"})})

	ghost := wallet{uuid.New(), uuid.New()}
	cases := map[string]struct{ id, body, reason string }{
		"malformed": {"", "{not json", "invalid_message"},
		"opening":   {uniq("m"), "", "invalid_message"},
		"unknown":   {uniq("m"), "", "wallet_not_found"},
		"reused-id": {reused, "", "message_id_conflict"},
	}
	opening := cases["opening"]
	opening.body = envelope(opening.id, w.wagerData("OPENING", uniq("x"), "1.00", ""))
	cases["opening"] = opening
	unknown := cases["unknown"]
	unknown.body = envelope(unknown.id, ghost.wagerData("BET", uniq("x"), "1.00", ""))
	cases["unknown"] = unknown
	r := cases["reused-id"]
	r.body = envelope(reused, w.wagerData("BET", uniq("other"), "2.00", ""))
	cases["reused-id"] = r

	for name, cs := range cases {
		send(t, c, in, name, cs.body)
	}
	pollUntil(t, consumer, 20*time.Second, func() bool { return queueDepth(t, c, in) == 0 })

	got := deadLetters(t, c, dlq, len(cases))
	for name, cs := range cases {
		if cs.id == "" {
			continue // no messageId to match; counted below
		}
		if got[cs.id] != cs.reason {
			t.Errorf("%s: reason %q, want %q", name, got[cs.id], cs.reason)
		}
	}
	if got[""] != "invalid_message" || len(got) != len(cases) {
		t.Errorf("dead letters = %v", got)
	}
	e.consistent(t, w, "99.00")
}

func TestSQSAndHTTPSameOperation(t *testing.T) {
	e := newEnv(fastRetry)
	c := sqsClient(t)
	in, dlq := queues(t, c, 5)
	consumer := queue.NewConsumer(c, in, dlq, e.messages, time.Second, quiet)
	api := newAPI(t, e)
	prov := testkit.Token(t, "provider-a")

	for range 5 {
		w := e.open(t, "100.00")
		ext := uniq("bet")
		data := w.wagerData("BET", ext, "60.00", "")
		send(t, c, in, w.id.String(), envelope(uniq("m"), data))

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			body := testkit.Wager("provider-a", ext, w.player.String(), w.id.String(), "BET", "60.00", "")
			if r := api.Do(t, "POST", "/wagering/transactions", prov, body, "Idempotency-Key", "provider-a:"+ext); r.Status >= 300 {
				t.Errorf("http: %d %s", r.Status, r.Raw)
			}
		}()
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for range 10 {
				got, err := consumer.Poll(ctx, ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if got {
					return
				}
			}
		}()
		wg.Wait()
		pollUntil(t, consumer, 15*time.Second, func() bool { return queueDepth(t, c, in) == 0 })
		if debits(t, w) != 1 {
			t.Fatalf("debits = %d", debits(t, w))
		}
		e.consistent(t, w, "40.00")
	}
}

type alwaysRetry struct{ calls atomic.Int32 }

func (h *alwaysRetry) Handle(context.Context, service.InboundMessage) (service.Disposition, string, error) {
	h.calls.Add(1)
	return service.Retry, "", errors.New("database down")
}

func TestSQSRetriesThenRedrivesToDLQ(t *testing.T) {
	e := newEnv(fastRetry)
	c := sqsClient(t)
	in, dlq := queues(t, c, 2)
	h := &alwaysRetry{}
	consumer := queue.NewConsumer(c, in, dlq, h, time.Second, quiet)
	w := e.open(t, "100.00")
	msgID := uniq("m")
	send(t, c, in, w.id.String(), envelope(msgID, w.wagerData("BET", uniq("bet"), "1.00", "")))

	// Backoff by receive count (2s, 4s); the redrive policy does the rest.
	pollUntil(t, consumer, 30*time.Second, func() bool { return queueDepth(t, c, in) == 0 })
	if got := deadLetters(t, c, dlq, 1); got[msgID] != "redrive" {
		t.Fatalf("dlq = %v", got)
	}
	if h.calls.Load() != 2 {
		t.Fatalf("handler calls = %d", h.calls.Load())
	}
}
