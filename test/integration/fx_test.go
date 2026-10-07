//go:build integration

package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"wager-wallet-service/internal/app"
	"wager-wallet-service/internal/config"
	"wager-wallet-service/test/testkit"
)

func appConfig(t *testing.T, in, dlq, events string) config.Config {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return config.Config{
		InstanceID: "it-" + uuid.NewString()[:4], HTTPAddr: addr, LogLevel: "error", ShutdownTimeout: 5 * time.Second,
		DatabaseURL: dbURL, DBMaxConns: 5,
		OIDCIssuer:   testkit.Env("OIDC_ISSUER", "http://localhost:8180/realms/wager"),
		OIDCJWKSURL:  testkit.KeycloakURL() + "/realms/wager/protocol/openid-connect/certs",
		OIDCAudience: "wager-wallet-api",
		AWSRegion:    "us-east-1", SQSEndpoint: testkit.Env("SQS_ENDPOINT", "http://localhost:4566"),
		InputQueueURL: in, DLQURL: dlq, EventsQueueURL: events,
		ReferenceRetryBase: 50 * time.Millisecond, ReferenceRetryMax: 200 * time.Millisecond, ReferenceTTL: time.Minute,
		OutboxLease: 5 * time.Second, OutboxBatch: 50, WorkerInterval: 50 * time.Millisecond,
	}
}

func startApp(t *testing.T, cfg config.Config) (*fx.App, *pgxpool.Pool) {
	t.Helper()
	var p *pgxpool.Pool
	a := fx.New(app.Options(cfg), fx.Populate(&p))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return a, p
}

func stopApp(t *testing.T, a *fx.App) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := a.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAppLifecycleAndRestart(t *testing.T) {
	c := sqsClient(t)
	in, dlq := queues(t, c, 5)
	events, _ := queues(t, c, 5)
	cfg := appConfig(t, in, dlq, events)
	op, prov := testkit.Token(t, "wallet-internal"), testkit.Token(t, "provider-a")

	first, firstPool := startApp(t, cfg)
	api := testkit.NewClient("http://" + cfg.HTTPAddr)
	if r := api.Get(t, "/health/ready", ""); r.Status != 200 || r.Str("status") != "UP" {
		t.Fatalf("ready: %d %s", r.Status, r.Raw)
	}
	walletID, playerID := api.OpenWallet(t, op, "100.00")
	bet := testkit.Wager("provider-a", uniq("bet"), playerID, walletID, "BET", "40.00", "")
	betKey := uniq("k")
	betRes := api.Do(t, "POST", "/wagering/transactions", prov, bet, "Idempotency-Key", betKey)
	refundExt := uniq("refund")
	future := uniq("win")
	pending := api.Do(t, "POST", "/wagering/transactions", prov,
		testkit.Wager("provider-a", refundExt, playerID, walletID, "ROLLBACK", "10.00", future), "Idempotency-Key", uniq("k"))
	if betRes.Status != 201 || pending.Status != 202 {
		t.Fatalf("bet %d, pending %d", betRes.Status, pending.Status)
	}
	stopApp(t, first)
	if err := firstPool.Ping(context.Background()); err == nil {
		t.Fatal("pool still open after stop")
	}

	second, _ := startApp(t, cfg)
	defer stopApp(t, second)
	if r := api.Do(t, "POST", "/wagering/transactions", prov, bet, "Idempotency-Key", betKey); r.Status != 200 ||
		r.Str("transactionId") != betRes.Str("transactionId") || r.Str("balance", "amount") != "60.00" {
		t.Fatalf("replay after restart: %d %s", r.Status, r.Raw)
	}
	api.Do(t, "POST", "/wagering/transactions", prov,
		testkit.Wager("provider-a", future, playerID, walletID, "WIN", "10.00", ""), "Idempotency-Key", uniq("k"))
	deadline := time.Now().Add(10 * time.Second)
	for api.Get(t, "/wagering/transactions/"+pending.Str("transactionId"), op).Str("status") != "PROCESSED" {
		if time.Now().After(deadline) {
			t.Fatal("second instance did not settle the pending rollback")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if r := api.Do(t, "POST", "/wallets/"+walletID+"/reconciliation", op, nil); !r.Bool("consistent") ||
		r.Str("storedBalance", "amount") != "60.00" {
		t.Fatalf("reconciliation: %s", r.Raw)
	}
}
