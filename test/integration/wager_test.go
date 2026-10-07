//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"wager-wallet-service/internal/domain"
	"wager-wallet-service/internal/service"
)

func TestOpenWallet(t *testing.T) {
	e := newEnv(fastRetry)
	ctx := context.Background()

	w := e.open(t, "100.00")
	got, _ := e.wallets.Get(ctx, w.id)
	if got.Version() != 1 || got.Balance().Amount() != "100.00" {
		t.Fatalf("version=%d balance=%s", got.Version(), got.Balance())
	}
	if n := count(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING' AND status = 'PROCESSED'`, w.id); n != 1 {
		t.Fatalf("opening transactions = %d", n)
	}
	if n := count(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'CREDIT'`, w.id); n != 1 {
		t.Fatalf("opening entries = %d", n)
	}
	if events(t, w.id, domain.EventBalanceChanged) != 1 {
		t.Fatal("opening must produce WalletBalanceChanged")
	}
	if n := count(t, `SELECT count(*) FROM outbox_events o JOIN wager_transactions x ON x.id = o.aggregate_id
		WHERE x.wallet_id = $1 AND o.event_type = 'WagerTransactionProcessed'`, w.id); n != 1 {
		t.Fatalf("opening processed events = %d", n)
	}
	e.consistent(t, w, "100.00")

	zero := e.open(t, "0.00")
	if n := count(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1`, zero.id); n != 0 {
		t.Fatal("zero opening must not create OPENING")
	}
	if n := count(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1`, zero.id); n != 0 {
		t.Fatal("zero opening must not create events")
	}

	if _, err := e.wallets.Open(ctx, w.player, brl(t, "1.00")); !errors.Is(err, service.ErrWalletExists) {
		t.Fatalf("second wallet for same player/currency: %v", err)
	}
}

// Mandatory: 100.00 balance, two different 80.00 bets at the same time.
func TestTwoConcurrentBetsOf80(t *testing.T) {
	e := newEnv(fastRetry)
	w := e.open(t, "100.00")
	a, b := uniq("bet-a"), uniq("bet-b")

	// Each bet is also delivered three times (at-least-once).
	var wg sync.WaitGroup
	results := make(chan service.Result, 6)
	for _, ext := range []string{a, b, a, b, a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- e.do(t, w, op{kind: domain.KindBet, ext: ext, amount: "80.00"})
		}()
	}
	wg.Wait()
	close(results)

	byTx := map[uuid.UUID]domain.TxState{}
	fresh := 0
	for r := range results {
		byTx[r.Tx.ID()] = r.Tx.State()
		if !r.Replay {
			fresh++
		}
	}
	if len(byTx) != 2 || fresh != 2 {
		t.Fatalf("transactions=%d fresh=%d", len(byTx), fresh)
	}
	statuses := map[domain.Status]int{}
	for _, s := range byTx {
		statuses[s.Status]++
		if s.Status == domain.StatusRejected && s.FailureCode != domain.FailureInsufficientFunds {
			t.Fatalf("failure code = %s", s.FailureCode)
		}
	}
	if statuses[domain.StatusProcessed] != 1 || statuses[domain.StatusRejected] != 1 {
		t.Fatalf("statuses = %v", statuses)
	}
	if debits(t, w) != 1 {
		t.Fatalf("debits = %d", debits(t, w))
	}
	e.consistent(t, w, "20.00")

	// Resending does not change anything.
	e.do(t, w, op{kind: domain.KindBet, ext: a, amount: "80.00"})
	e.do(t, w, op{kind: domain.KindBet, ext: b, amount: "80.00"})
	if debits(t, w) != 1 {
		t.Fatal("resend changed the ledger")
	}
	e.consistent(t, w, "20.00")
}

// Mandatory: the same bet 50 times in parallel produces one debit.
func TestSameBetFiftyTimes(t *testing.T) {
	e := newEnv(fastRetry)
	w := e.open(t, "1000.00")
	ext := uniq("bet")

	var wg sync.WaitGroup
	var mu sync.Mutex
	ids := map[uuid.UUID]bool{}
	fresh, replays := 0, 0
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := e.do(t, w, op{kind: domain.KindBet, ext: ext, amount: "25.00"})
			mu.Lock()
			defer mu.Unlock()
			ids[r.Tx.ID()] = true
			if r.Replay {
				replays++
			} else {
				fresh++
			}
			if b := r.Tx.State().ResultBalance; b.Amount() != "975.00" {
				t.Errorf("balance in result = %s", b)
			}
		}()
	}
	wg.Wait()
	if fresh != 1 || replays != 49 || len(ids) != 1 {
		t.Fatalf("fresh=%d replays=%d ids=%d", fresh, replays, len(ids))
	}
	if debits(t, w) != 1 {
		t.Fatalf("debits = %d", debits(t, w))
	}
	e.consistent(t, w, "975.00")
}

func TestDistinctWalletsInParallel(t *testing.T) {
	e := newEnv(fastRetry)
	wallets := make([]wallet, 10)
	for i := range wallets {
		wallets[i] = e.open(t, "50.00")
	}
	var wg sync.WaitGroup
	for _, w := range wallets {
		for range 5 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if r := e.do(t, w, op{kind: domain.KindBet, ext: uniq("bet"), amount: "10.00"}); r.Tx.Status() != domain.StatusProcessed {
					t.Errorf("bet status %s", r.Tx.Status())
				}
			}()
		}
	}
	wg.Wait()
	for _, w := range wallets {
		e.consistent(t, w, "0.00")
	}
}

func TestReplayReturnsOriginalBalance(t *testing.T) {
	e := newEnv(fastRetry)
	w := e.open(t, "100.00")
	bet := op{kind: domain.KindBet, ext: uniq("bet"), amount: "25.00"}
	first := e.do(t, w, bet)
	e.do(t, w, op{kind: domain.KindWin, ext: uniq("win"), amount: "10.00"})

	again := e.do(t, w, bet)
	if !again.Replay || again.Tx.ID() != first.Tx.ID() || again.Tx.State().ResultBalance.Amount() != "75.00" {
		t.Fatalf("replay = %+v", again.Tx.State())
	}
	if e.balance(t, w) != "85.00" {
		t.Fatalf("balance = %s", e.balance(t, w))
	}
}

func TestIdempotencyConflicts(t *testing.T) {
	e := newEnv(fastRetry)
	ctx := context.Background()
	w := e.open(t, "100.00")
	ext := uniq("bet")
	e.do(t, w, op{kind: domain.KindBet, ext: ext, amount: "10.00"})

	_, err := e.wagers.Process(ctx, w.req(t, op{kind: domain.KindBet, ext: ext, amount: "11.00"}), service.Options{})
	if !errors.Is(err, service.ErrIdempotencyConflict) {
		t.Fatalf("same key, different payload: %v", err)
	}
	_, err = e.wagers.Process(ctx, w.req(t, op{kind: domain.KindBet, ext: ext, amount: "10.00", key: "other-key"}), service.Options{})
	if !errors.Is(err, service.ErrExternalIDReused) {
		t.Fatalf("same externalId, other key: %v", err)
	}
	other := e.open(t, "100.00")
	_, err = e.wagers.Process(ctx, wallet{w.id, other.player}.req(t, op{kind: domain.KindBet, ext: uniq("x"), amount: "1.00"}), service.Options{})
	if !errors.Is(err, service.ErrWalletMismatch) {
		t.Fatalf("wrong player: %v", err)
	}
	if debits(t, w) != 1 {
		t.Fatal("conflicts must not move money")
	}
}

func TestLossHasNoLedgerEntry(t *testing.T) {
	e := newEnv(fastRetry)
	w := e.open(t, "100.00")
	r := e.do(t, w, op{kind: domain.KindLoss, ext: uniq("loss"), amount: "0.00"})
	if r.Tx.Status() != domain.StatusProcessed {
		t.Fatal(r.Tx.Status())
	}
	got, _ := e.wallets.Get(context.Background(), w.id)
	if got.Version() != 1 {
		t.Fatalf("LOSS changed the version to %d", got.Version())
	}
	if events(t, r.Tx.ID(), domain.EventTransactionProcessed) != 1 {
		t.Fatal("LOSS must produce WagerTransactionProcessed")
	}
	if n := count(t, `SELECT count(*) FROM wallet_ledger_entries WHERE transaction_id = $1`, r.Tx.ID()); n != 0 {
		t.Fatal("LOSS must not create a ledger entry")
	}
}

func TestReversals(t *testing.T) {
	e := newEnv(fastRetry)
	w := e.open(t, "100.00")

	bet := uniq("bet")
	e.do(t, w, op{kind: domain.KindBet, ext: bet, amount: "30.00"})
	wrong := e.do(t, w, op{kind: domain.KindRefund, ext: uniq("refund"), amount: "10.00", ref: bet})
	if wrong.Tx.State().FailureCode != domain.FailureReferenceAmountMismatch {
		t.Fatalf("partial refund = %s", wrong.Tx.State().FailureCode)
	}
	refund := e.do(t, w, op{kind: domain.KindRefund, ext: uniq("refund"), amount: "30.00", ref: bet})
	if refund.Tx.Status() != domain.StatusProcessed || e.balance(t, w) != "100.00" {
		t.Fatalf("refund %s balance %s", refund.Tx.Status(), e.balance(t, w))
	}
	// One successful reversal per referenced transaction, any kind.
	for _, k := range []domain.Kind{domain.KindRefund, domain.KindRollback} {
		r := e.do(t, w, op{kind: k, ext: uniq("again"), amount: "30.00", ref: bet})
		if r.Tx.State().FailureCode != domain.FailureAlreadyReversed {
			t.Fatalf("second %s = %s %s", k, r.Tx.Status(), r.Tx.State().FailureCode)
		}
	}

	// ROLLBACK of a WIN debits; without balance it gets its own code.
	win := uniq("win")
	e.do(t, w, op{kind: domain.KindWin, ext: win, amount: "50.00"})
	e.do(t, w, op{kind: domain.KindBet, ext: uniq("bet"), amount: "150.00"})
	rb := e.do(t, w, op{kind: domain.KindRollback, ext: uniq("rb"), amount: "50.00", ref: win})
	if rb.Tx.State().FailureCode != domain.FailureReversalInsufficientFunds {
		t.Fatalf("rollback without balance = %s", rb.Tx.State().FailureCode)
	}
	if events(t, rb.Tx.ID(), domain.EventTransactionRejected) != 1 {
		t.Fatal("rejection must be auditable with an event")
	}
	e.consistent(t, w, "0.00")
}

func TestPendingReferenceResolved(t *testing.T) {
	e := newEnv(fastRetry)
	w := e.open(t, "100.00")
	bet := uniq("bet")

	refund := e.do(t, w, op{kind: domain.KindRefund, ext: uniq("refund"), amount: "40.00", ref: bet})
	if refund.Tx.Status() != domain.StatusPendingReference {
		t.Fatalf("refund before bet = %s", refund.Tx.Status())
	}
	if events(t, refund.Tx.ID(), domain.EventPendingReference) != 1 {
		t.Fatal("missing WagerTransactionPendingReference")
	}
	e.do(t, w, op{kind: domain.KindBet, ext: bet, amount: "40.00"})

	e.settlePending(t, func() bool { return txState(t, e, refund.Tx.ID()).Status == domain.StatusProcessed })
	if events(t, refund.Tx.ID(), domain.EventTransactionProcessed) != 1 {
		t.Fatal("missing processed event after resolution")
	}
	e.consistent(t, w, "100.00")
}

func TestPendingReferenceExpires(t *testing.T) {
	e := newEnv(domain.RetryPolicy{Base: 50 * time.Millisecond, Max: 100 * time.Millisecond, TTL: 300 * time.Millisecond})
	w := e.open(t, "100.00")
	r := e.do(t, w, op{kind: domain.KindRollback, ext: uniq("rb"), amount: "10.00", ref: uniq("never")})

	e.settlePending(t, func() bool { return txState(t, e, r.Tx.ID()).Status == domain.StatusRejected })
	s := txState(t, e, r.Tx.ID())
	if s.FailureCode != domain.FailureReferenceNotFound || s.Attempts == 0 {
		t.Fatalf("expired = %s after %d attempts", s.FailureCode, s.Attempts)
	}
	if events(t, r.Tx.ID(), domain.EventTransactionRejected) != 1 {
		t.Fatal("missing rejection event")
	}
	e.consistent(t, w, "100.00")
}
