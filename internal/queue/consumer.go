package queue

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"wager-wallet-service/internal/obs"
	"wager-wallet-service/internal/service"
)

type Handler interface {
	Handle(ctx context.Context, msg service.InboundMessage) (service.Disposition, string, error)
}

// Consumer reads wager-transactions.fifo.
//
//   - delete only after the outcome is committed (inbox in the same SQL tx);
//   - transient failure: keep the message, raise its visibility (backoff by
//     receive count); after maxReceiveCount=5 the redrive policy moves it to
//     the DLQ;
//   - permanent failure: copy to the DLQ with the reason, then delete.
type Consumer struct {
	client   *sqs.Client
	queueURL string
	dlqURL   string
	handler  Handler
	wait     time.Duration
	log      *slog.Logger
}

func NewConsumer(c *sqs.Client, queueURL, dlqURL string, h Handler, wait time.Duration, log *slog.Logger) *Consumer {
	return &Consumer{client: c, queueURL: queueURL, dlqURL: dlqURL, handler: h, wait: wait, log: log.With("component", "sqs-consumer")}
}

// Poll receives one batch with pollCtx (cancelled as soon as shutdown starts)
// and handles it with workCtx (which lives until the shutdown deadline).
// Messages received but not started when shutdown begins are released with
// visibility 0 so another instance picks them up right away.
func (c *Consumer) Poll(pollCtx, workCtx context.Context) (bool, error) {
	out, err := c.client.ReceiveMessage(pollCtx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.queueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     int32(c.wait / time.Second),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
			types.MessageSystemAttributeNameMessageGroupId,
		},
	})
	if err != nil {
		if pollCtx.Err() != nil {
			return false, nil
		}
		return false, err
	}

	// FIFO: if a message of a group is not settled, later messages of the same
	// group in this batch must wait, or the wallet order would break.
	blocked := map[string]bool{}
	for i, m := range out.Messages {
		if pollCtx.Err() != nil {
			c.release(workCtx, out.Messages[i:])
			break
		}
		group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
		if blocked[group] {
			c.release(workCtx, []types.Message{m})
			continue
		}
		if !c.handle(workCtx, m) {
			blocked[group] = true
		}
	}
	return len(out.Messages) > 0, nil
}

// handle returns true when the message left the queue.
func (c *Consumer) handle(ctx context.Context, m types.Message) bool {
	count, _ := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	env, msg, err := Decode(aws.ToString(m.Body))
	ctx = obs.With(ctx, "messageId", env.MessageID, "receiveCount", count)
	if err != nil {
		return c.deadLetter(ctx, m, "invalid_message", err)
	}
	msg.ReceivedAt = time.Now().UTC()

	d, reason, err := c.handler.Handle(ctx, msg)
	switch d {
	case service.Ack:
		return c.delete(ctx, m)
	case service.DeadLetter:
		return c.deadLetter(ctx, m, reason, err)
	}
	delay := min(time.Duration(1<<min(count, 8))*time.Second, 5*time.Minute)
	c.log.WarnContext(ctx, "message failed, will retry", "error", err, "retryIn", delay.String())
	c.setVisibility(ctx, m, delay)
	return false
}

func (c *Consumer) delete(ctx context.Context, m types.Message) bool {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(c.queueURL), ReceiptHandle: m.ReceiptHandle})
	if err != nil {
		// Outcome already committed: the redelivery will hit the inbox.
		c.log.WarnContext(ctx, "delete failed", "error", err)
		return false
	}
	return true
}

// deadLetter copies the message to the DLQ and deletes the original. The SQS
// MessageId is the DLQ dedup id, so retrying this step does not duplicate it.
func (c *Consumer) deadLetter(ctx context.Context, m types.Message, reason string, cause error) bool {
	detail := "-"
	if cause != nil {
		detail = cause.Error()
		if len(detail) > 500 {
			detail = detail[:500]
		}
	}
	group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	if group == "" {
		group = "unknown"
	}
	_, err := c.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(c.dlqURL),
		MessageBody:            m.Body,
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: m.MessageId,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"deadLetterReason": {DataType: aws.String("String"), StringValue: aws.String(reason)},
			"deadLetterDetail": {DataType: aws.String("String"), StringValue: aws.String(detail)},
		},
	})
	if err != nil {
		c.log.ErrorContext(ctx, "dead-letter send failed", "error", err)
		return false
	}
	obs.SQSMessages.WithLabelValues("dlq").Inc()
	obs.DeadLetters.WithLabelValues(reason).Inc()
	c.log.ErrorContext(ctx, "message moved to DLQ", "reason", reason, "error", detail)
	return c.delete(ctx, m)
}

func (c *Consumer) release(ctx context.Context, msgs []types.Message) {
	for _, m := range msgs {
		c.setVisibility(ctx, m, 0)
	}
}

func (c *Consumer) setVisibility(ctx context.Context, m types.Message, d time.Duration) {
	_, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.queueURL), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: int32(d / time.Second),
	})
	if err != nil {
		c.log.WarnContext(ctx, "change visibility failed", "error", err)
	}
}
