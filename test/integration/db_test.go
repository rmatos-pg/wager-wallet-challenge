//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"wager-wallet-service/internal/domain"
	"wager-wallet-service/test/testkit"
)

func TestMigrationsDownAndUp(t *testing.T) {
	ctx := context.Background()
	admin := testkit.Env("DATABASE_URL", "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable")
	url, drop, err := createDB(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	defer drop()
	tables := func() int {
		conn, err := pgx.Connect(ctx, url)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)
		var n int
		_ = conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'`).Scan(&n)
		return n
	}
	for _, step := range []struct {
		dir  string
		want int
	}{{"up", 5}, {"down", 0}, {"up", 5}} {
		if err := migrate(ctx, url, step.dir); err != nil {
			t.Fatalf("%s: %v", step.dir, err)
		}
		if got := tables(); got != step.want {
			t.Fatalf("after %s: %d tables, want %d", step.dir, got, step.want)
		}
	}
}

func TestDatabaseGuards(t *testing.T) {
	e := newEnv(fastRetry)
	ctx := context.Background()
	w := e.open(t, "100.00")
	bet := e.do(t, w, op{kind: domain.KindBet, ext: uniq("bet"), amount: "10.00"})

	mustFail := func(name, sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err == nil {
			t.Errorf("%s: database accepted it", name)
		}
	}
	mustFail("edit ledger", `UPDATE wallet_ledger_entries SET amount_units = 1 WHERE wallet_id = $1`, w.id)
	mustFail("delete ledger", `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, w.id)
	mustFail("truncate ledger", `TRUNCATE wallet_ledger_entries CASCADE`)
	mustFail("negative balance", `UPDATE wallets SET balance_units = -1 WHERE id = $1`, w.id)
	mustFail("change terminal transaction", `UPDATE wager_transactions SET failure_code = 'X' WHERE id = $1`, bet.Tx.ID())
	mustFail("delete transaction", `DELETE FROM wager_transactions WHERE id = $1`, bet.Tx.ID())
	mustFail("second opening", `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, currency,
		amount_units, result_balance_units, created_at, updated_at, completed_at)
		VALUES ($1, 'INTERNAL', 'OPENING', 'PROCESSED', $2, $3, 'BRL', 100, 100, now(), now(), now())`, uuid.New(), w.id, w.player)
	mustFail("wrong ledger math", `INSERT INTO wallet_ledger_entries (id, wallet_id, player_id, currency, transaction_id,
		direction, amount_units, balance_before_units, balance_after_units, wallet_version, created_at)
		VALUES ($1, $2, $3, 'BRL', $4, 'DEBIT', 100, 1000, 950, 9, now())`, uuid.New(), w.id, w.player, bet.Tx.ID())
	mustFail("ledger in other currency", `INSERT INTO wallet_ledger_entries (id, wallet_id, player_id, currency, transaction_id,
		direction, amount_units, balance_before_units, balance_after_units, wallet_version, created_at)
		VALUES ($1, $2, $3, 'USD', $4, 'CREDIT', 100, 0, 100, 9, now())`, uuid.New(), w.id, w.player, bet.Tx.ID())
	e.consistent(t, w, "90.00")
}
