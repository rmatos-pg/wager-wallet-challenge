//go:build multiinstance

// Package multiinstance runs against the three wallet instances of
// docker-compose (wallet-1..3). Requests are spread round-robin, so the
// races happen between processes, not goroutines of one process.
package multiinstance

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"wager-wallet-service/test/testkit"
)

func instances(t *testing.T) []*testkit.Client {
	t.Helper()
	var out []*testkit.Client
	for _, u := range strings.Split(testkit.Env("WALLET_URLS", "http://localhost:8081,http://localhost:8082,http://localhost:8083"), ",") {
		c := testkit.NewClient(u)
		if r := c.Get(t, "/health/ready", ""); r.Status != 200 {
			t.Fatalf("%s not ready: %d", u, r.Status)
		}
		out = append(out, c)
	}
	return out
}

func submit(t *testing.T, c *testkit.Client, token string, body map[string]any, key string) testkit.Response {
	return c.Do(t, "POST", "/wagering/transactions", token, body, "Idempotency-Key", key)
}

func balance(t *testing.T, c *testkit.Client, op, walletID string) string {
	return c.Get(t, "/wallets/"+walletID, op).Str("balance", "amount")
}

func consistent(t *testing.T, c *testkit.Client, op, walletID, want string) {
	t.Helper()
	r := c.Do(t, "POST", "/wallets/"+walletID+"/reconciliation", op, nil)
	if !r.Bool("consistent") || r.Str("storedBalance", "amount") != want {
		t.Fatalf("reconciliation: %s (want %s)", r.Raw, want)
	}
}

func TestTwoBetsOf80AcrossInstances(t *testing.T) {
	apis := instances(t)
	op, prov := testkit.Token(t, "wallet-internal"), testkit.Token(t, "provider-a")
	walletID, playerID := apis[0].OpenWallet(t, op, "100.00")

	var wg sync.WaitGroup
	var mu sync.Mutex
	statuses := map[string]int{}
	for i, ext := range []string{testkit.Uniq("a"), testkit.Uniq("b")} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := submit(t, apis[i%len(apis)], prov, testkit.Wager("provider-a", ext, playerID, walletID, "BET", "80.00", ""), "k-"+ext)
			mu.Lock()
			statuses[fmt.Sprintf("%d %s", r.Status, r.Str("failureCode"))]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if statuses["201 "] != 1 || statuses["422 INSUFFICIENT_FUNDS"] != 1 {
		t.Fatalf("statuses = %v", statuses)
	}
	consistent(t, apis[2], op, walletID, "20.00")
}

func TestSameBetFiftyTimesAcrossInstances(t *testing.T) {
	apis := instances(t)
	op, prov := testkit.Token(t, "wallet-internal"), testkit.Token(t, "provider-a")
	walletID, playerID := apis[0].OpenWallet(t, op, "100.00")
	ext := testkit.Uniq("bet")
	body := testkit.Wager("provider-a", ext, playerID, walletID, "BET", "10.00", "")

	var wg sync.WaitGroup
	var mu sync.Mutex
	ids, codes := map[string]bool{}, map[int]int{}
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := submit(t, apis[i%len(apis)], prov, body, "k-"+ext)
			mu.Lock()
			ids[r.Str("transactionId")] = true
			codes[r.Status]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(ids) != 1 || codes[201] != 1 || codes[200] != 49 {
		t.Fatalf("ids=%d codes=%v", len(ids), codes)
	}
	consistent(t, apis[1], op, walletID, "90.00")
}

func TestDistinctWalletsAcrossInstances(t *testing.T) {
	apis := instances(t)
	op, prov := testkit.Token(t, "wallet-internal"), testkit.Token(t, "provider-a")
	type w struct{ id, player string }
	wallets := make([]w, 6)
	for i := range wallets {
		wallets[i].id, wallets[i].player = apis[i%len(apis)].OpenWallet(t, op, "30.00")
	}
	var wg sync.WaitGroup
	for i, wl := range wallets {
		for j := range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ext := testkit.Uniq("bet")
				if r := submit(t, apis[(i+j)%len(apis)], prov, testkit.Wager("provider-a", ext, wl.player, wl.id, "BET", "10.00", ""), "k-"+ext); r.Status != 201 {
					t.Errorf("bet: %d %s", r.Status, r.Raw)
				}
			}()
		}
	}
	wg.Wait()
	for _, wl := range wallets {
		consistent(t, apis[0], op, wl.id, "0.00")
	}
}

func TestPendingReferenceSettledByAnyInstance(t *testing.T) {
	apis := instances(t)
	op, prov := testkit.Token(t, "wallet-internal"), testkit.Token(t, "provider-a")
	walletID, playerID := apis[0].OpenWallet(t, op, "100.00")
	bet := testkit.Uniq("bet")
	refund := testkit.Uniq("refund")

	p := submit(t, apis[1], prov, testkit.Wager("provider-a", refund, playerID, walletID, "REFUND", "25.00", bet), "k-"+refund)
	if p.Status != 202 {
		t.Fatalf("refund before bet: %d %s", p.Status, p.Raw)
	}
	if r := submit(t, apis[2], prov, testkit.Wager("provider-a", bet, playerID, walletID, "BET", "25.00", ""), "k-"+bet); r.Status != 201 {
		t.Fatalf("bet: %d", r.Status)
	}
	deadline := time.Now().Add(60 * time.Second)
	for apis[0].Get(t, "/wagering/transactions/"+p.Str("transactionId"), op).Str("status") != "PROCESSED" {
		if time.Now().After(deadline) {
			t.Fatal("pending refund not settled")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if b := balance(t, apis[1], op, walletID); b != "100.00" {
		t.Fatalf("balance = %s", b)
	}
	consistent(t, apis[2], op, walletID, "100.00")
}
