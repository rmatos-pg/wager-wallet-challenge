//go:build integration

// Package integration runs against real PostgreSQL, Keycloak and SQS
// (LocalStack). The suite uses a throwaway database built from the
// migrations, so the running compose instances never touch test data.
package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	neturl "net/url"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"wager-wallet-service/internal/domain"
	"wager-wallet-service/internal/money"
	"wager-wallet-service/internal/service"
	"wager-wallet-service/internal/store"
	"wager-wallet-service/test/testkit"
)

var (
	pool  *pgxpool.Pool
	st    *store.Store
	dbURL string
	quiet = slog.New(slog.NewJSONHandler(io.Discard, nil))
)

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	ctx := context.Background()
	admin := testkit.Env("DATABASE_URL", "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable")
	url, drop, err := createDB(ctx, admin)
	if err != nil {
		fmt.Println("postgres unavailable:", err)
		return 1
	}
	defer drop()
	dbURL = url
	if err := migrate(ctx, url, "up"); err != nil {
		fmt.Println("migrate:", err)
		return 1
	}
	cfg, _ := pgxpool.ParseConfig(url)
	cfg.MaxConns = 60
	if pool, err = pgxpool.NewWithConfig(ctx, cfg); err != nil {
		fmt.Println(err)
		return 1
	}
	defer pool.Close()
	st = store.New(pool)
	return m.Run()
}

// createDB creates an empty database next to adminURL's and returns its URL
// and a drop function.
func createDB(ctx context.Context, adminURL string) (string, func(), error) {
	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return "", nil, err
	}
	name := "wallet_it_" + uuid.NewString()[:8]
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		return "", nil, err
	}
	u, _ := neturl.Parse(adminURL)
	u.Path = "/" + name
	return u.String(), func() {
		_, _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		conn.Close(context.Background())
	}, nil
}

// migrate runs the same SQL files golang-migrate runs in compose.
func migrate(ctx context.Context, url, direction string) error {
	files, _ := filepath.Glob(filepath.Join("..", "..", "migrations", "*."+direction+".sql"))
	if len(files) == 0 {
		return fmt.Errorf("no %s migrations", direction)
	}
	sort.Strings(files)
	if direction == "down" {
		sort.Sort(sort.Reverse(sort.StringSlice(files)))
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	for _, f := range files {
		sql, _ := os.ReadFile(f)
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(f), err)
		}
	}
	return nil
}

// --- helpers ---

var fastRetry = domain.RetryPolicy{Base: 50 * time.Millisecond, Max: 200 * time.Millisecond, TTL: time.Minute}

type env struct {
	wallets  *service.Wallets
	wagers   *service.Wagers
	messages *service.Messages
}

func newEnv(p domain.RetryPolicy) env {
	wagers := service.NewWagers(st, p, quiet)
	return env{wallets: service.NewWallets(st, quiet), wagers: wagers, messages: service.NewMessages(st, wagers, quiet)}
}

func brl(t testing.TB, s string) money.Money {
	t.Helper()
	m, err := money.Parse(s, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

type wallet struct{ id, player uuid.UUID }

func (e env) open(t testing.TB, amount string) wallet {
	t.Helper()
	w, err := e.wallets.Open(context.Background(), uuid.New(), brl(t, amount))
	if err != nil {
		t.Fatal(err)
	}
	return wallet{w.ID(), w.PlayerID()}
}

// op describes a provider operation; empty fields get defaults.
type op struct {
	kind     domain.Kind
	ext      string
	amount   string
	ref      string
	key      string
	provider string
}

func (w wallet) req(t testing.TB, o op) domain.Request {
	if o.provider == "" {
		o.provider = "provider-a"
	}
	if o.key == "" {
		o.key = o.provider + ":" + o.ext
	}
	return domain.Request{ProviderID: o.provider, ExternalID: o.ext, IdempotencyKey: o.key, PlayerID: w.player,
		WalletID: w.id, RoundID: "round-1", GameID: "fortune-chimp", Kind: o.kind, Money: brl(t, o.amount),
		ReferenceExternalID: o.ref}
}

func (e env) do(t testing.TB, w wallet, o op) service.Result {
	t.Helper()
	res, err := e.wagers.Process(context.Background(), w.req(t, o), service.Options{})
	if err != nil {
		t.Fatalf("%s %s: %v", o.kind, o.ext, err)
	}
	return res
}

func (e env) balance(t testing.TB, w wallet) string {
	t.Helper()
	got, err := e.wallets.Get(context.Background(), w.id)
	if err != nil {
		t.Fatal(err)
	}
	return got.Balance().Amount()
}

// consistent asserts stored balance == credits - debits of the ledger.
func (e env) consistent(t testing.TB, w wallet, want string) {
	t.Helper()
	r, err := e.wallets.Reconcile(context.Background(), w.id)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Consistent || r.Stored.Amount() != want {
		t.Fatalf("reconciliation: stored %s calculated %s (want %s)", r.Stored, r.Calculated, want)
	}
}

func count(t testing.TB, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func debits(t testing.TB, w wallet) int {
	return count(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.id)
}

func events(t testing.TB, aggregate uuid.UUID, typ string) int {
	return count(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = $2`, aggregate, typ)
}

// settlePending runs the pending-reference worker until cond holds.
func (e env) settlePending(t testing.TB, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("pending reference not settled in time")
		}
		if _, err := e.wagers.ResumePending(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func txState(t testing.TB, e env, txID uuid.UUID) domain.TxState {
	t.Helper()
	tx, err := e.wagers.Get(context.Background(), txID)
	if err != nil {
		t.Fatal(err)
	}
	return tx.State()
}

func uniq(p string) string { return testkit.Uniq(p) }
