// Package queue is the SQS adapter: input consumer and event publisher.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"

	"wager-wallet-service/internal/config"
	"wager-wallet-service/internal/service"
)

// NewClient uses the standard AWS credential chain (env vars locally, IAM
// role in AWS). SQS_ENDPOINT points at LocalStack.
func NewClient(ctx context.Context, cfg config.Config) (*sqs.Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.AWSRegion))
	if err != nil {
		return nil, err
	}
	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.SQSEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.SQSEndpoint)
		}
	}), nil
}

// Ping is the readiness check: the input queue must be reachable.
func Ping(ctx context.Context, c *sqs.Client, queueURL string) error {
	_, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	return err
}

// Publisher sends outbox events to the events FIFO queue.
// MessageGroupId = aggregateId keeps per-aggregate order; MessageDeduplicationId
// = eventId makes a republish inside the 5-minute window a no-op for SQS, and
// consumers deduplicate by eventId after that.
type Publisher struct {
	client   *sqs.Client
	queueURL string
}

func NewPublisher(c *sqs.Client, cfg config.Config) *Publisher {
	return &Publisher{client: c, queueURL: cfg.EventsQueueURL}
}

var _ service.Publisher = (*Publisher)(nil)

func (p *Publisher) Publish(ctx context.Context, eventID, aggregateID uuid.UUID, eventType string, payload []byte) error {
	_, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.queueURL),
		MessageBody:            aws.String(string(payload)),
		MessageGroupId:         aws.String(aggregateID.String()),
		MessageDeduplicationId: aws.String(eventID.String()),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventType": {DataType: aws.String("String"), StringValue: aws.String(eventType)},
		},
	})
	return err
}

const MessageType = "WagerTransactionRequested"

// Envelope is the input message body.
type Envelope struct {
	MessageID  string             `json:"messageId"`
	Type       string             `json:"type"`
	OccurredAt string             `json:"occurredAt"`
	Data       service.WagerInput `json:"data"`
}

// Decode parses and validates a message body into the shared request type.
// The idempotency key comes from data.idempotencyKey.
func Decode(body string) (Envelope, service.InboundMessage, error) {
	var e Envelope
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		return e, service.InboundMessage{}, fmt.Errorf("malformed JSON: %w", err)
	}
	if e.MessageID == "" || len(e.MessageID) > 128 {
		return e, service.InboundMessage{}, errors.New("messageId is required (max 128 chars)")
	}
	if e.Type != MessageType {
		return e, service.InboundMessage{}, fmt.Errorf("unsupported type %q", e.Type)
	}
	req, err := e.Data.Request(e.Data.IdempotencyKey)
	if err != nil {
		return e, service.InboundMessage{}, err
	}
	return e, service.InboundMessage{MessageID: e.MessageID, Request: req}, nil
}
