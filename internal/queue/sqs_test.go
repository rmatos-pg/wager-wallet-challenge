package queue

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"wager-wallet-service/internal/domain"
	"wager-wallet-service/internal/service"
)

const body = `{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}`

func TestDecodeMatchesHTTPHash(t *testing.T) {
	_, msg, err := Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	if msg.MessageID != "msg-123" || msg.Request.IdempotencyKey != "provider-a:transaction-123" {
		t.Fatalf("decoded %+v", msg)
	}

	// The same operation as an HTTP body + Idempotency-Key header.
	var env struct {
		Data service.WagerInput `json:"data"`
	}
	_ = json.Unmarshal([]byte(body), &env)
	env.Data.IdempotencyKey = ""
	httpReq, err := env.Data.Request("provider-a:transaction-123")
	if err != nil {
		t.Fatal(err)
	}
	if httpReq.Hash() != msg.Request.Hash() {
		t.Fatal("HTTP and SQS must produce the same payload hash")
	}
}

func TestDecodeRejects(t *testing.T) {
	cases := map[string]string{
		"malformed":    `{"messageId":`,
		"no id":        `{"type":"WagerTransactionRequested","data":{}}`,
		"wrong type":   `{"messageId":"m","type":"Other","data":{}}`,
		"opening":      replace(body, `"BET"`, `"OPENING"`),
		"float amount": replace(body, `"25.00"`, `25.0`),
		"no key":       replace(body, `"provider-a:transaction-123"`, `""`),
	}
	for name, b := range cases {
		if _, _, err := Decode(b); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, _, err := Decode(replace(body, `"BET"`, `"OPENING"`)); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("OPENING must be a validation error: %v", err)
	}
}

func replace(s, old, new string) string { return strings.Replace(s, old, new, 1) }
