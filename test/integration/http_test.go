//go:build integration

package integration

import (
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"testing"
	"time"

	"github.com/google/uuid"

	"wager-wallet-service/internal/config"
	"wager-wallet-service/internal/httpapi"
	"wager-wallet-service/test/testkit"
)

// newAPI serves the real router with the real Keycloak verifier. The issuer
// is the public URL (http://localhost:8180) even when keys come from inside
// the compose network.
func newAPI(t *testing.T, e env) *testkit.Client {
	t.Helper()
	cfg := config.Config{
		OIDCIssuer:   testkit.Env("OIDC_ISSUER", "http://localhost:8180/realms/wager"),
		OIDCJWKSURL:  testkit.KeycloakURL() + "/realms/wager/protocol/openid-connect/certs",
		OIDCAudience: "wager-wallet-api",
	}
	srv := httptest.NewServer(httpapi.NewServer(e.wallets, e.wagers, httpapi.NewVerifier(cfg), nil, quiet).Handler())
	t.Cleanup(srv.Close)
	return testkit.NewClient(srv.URL)
}

func TestHTTPAuthentication(t *testing.T) {
	api := newAPI(t, newEnv(fastRetry))
	short := testkit.Token(t, "provider-short-lived")

	for name, tok := range map[string]string{"missing": "", "garbage": "not-a-jwt", "tampered": short[:len(short)-4] + "AAAA"} {
		if res := api.Get(t, "/wagering/transactions/"+uuid.NewString(), tok); res.Status != http.StatusUnauthorized {
			t.Errorf("%s token: %d", name, res.Status)
		}
	}
	time.Sleep(3 * time.Second) // provider-short-lived tokens live 2s
	if res := api.Get(t, "/wagering/transactions/"+uuid.NewString(), short); res.Status != http.StatusUnauthorized {
		t.Fatalf("expired token: %d", res.Status)
	}
	if res := api.Get(t, "/health/live", ""); res.Status != http.StatusOK {
		t.Fatalf("liveness must be public: %d", res.Status)
	}
}

func TestHTTPAuthorization(t *testing.T) {
	e := newEnv(fastRetry)
	api := newAPI(t, e)
	op, provA, noRole := testkit.Token(t, "wallet-internal"), testkit.Token(t, "provider-a"), testkit.Token(t, "provider-no-role")
	walletID, playerID := api.OpenWallet(t, op, "100.00")

	cases := []struct {
		name, method, path, token string
		body                      any
	}{
		{"provider opening wallet", "POST", "/wallets", provA, map[string]any{"playerId": uuid.NewString(),
			"initialBalance": map[string]string{"amount": "1.00", "currency": "BRL"}}},
		{"provider reading wallet", "GET", "/wallets/" + walletID, provA, nil},
		{"provider reading ledger", "GET", "/wallets/" + walletID + "/ledger", provA, nil},
		{"operator submitting wager", "POST", "/wagering/transactions", op,
			testkit.Wager("provider-a", uniq("bet"), playerID, walletID, "BET", "1.00", "")},
		{"client without role", "POST", "/wagering/transactions", noRole,
			testkit.Wager("provider-a", uniq("bet"), playerID, walletID, "BET", "1.00", "")},
		{"forged providerId", "POST", "/wagering/transactions", provA,
			testkit.Wager("provider-b", uniq("bet"), playerID, walletID, "BET", "1.00", "")},
	}
	for _, c := range cases {
		if res := api.Do(t, c.method, c.path, c.token, c.body, "Idempotency-Key", uniq("k")); res.Status != http.StatusForbidden {
			t.Errorf("%s: %d %s", c.name, res.Status, res.Raw)
		}
	}
	if res := api.Get(t, "/wallets/"+walletID, op); res.Str("balance", "amount") != "100.00" {
		t.Fatalf("forbidden calls moved money: %s", res.Raw)
	}
}

func TestHTTPProviderIsolation(t *testing.T) {
	e := newEnv(fastRetry)
	api := newAPI(t, e)
	op, provA, provB := testkit.Token(t, "wallet-internal"), testkit.Token(t, "provider-a"), testkit.Token(t, "provider-b")
	walletID, playerID := api.OpenWallet(t, op, "100.00")
	ext := uniq("bet")
	res := api.Do(t, "POST", "/wagering/transactions", provA,
		testkit.Wager("provider-a", ext, playerID, walletID, "BET", "10.00", ""), "Idempotency-Key", uniq("k"))
	txID := res.Str("transactionId")

	if r := api.Get(t, "/wagering/transactions/"+txID, provB); r.Status != http.StatusNotFound {
		t.Errorf("provider-b by id: %d", r.Status)
	}
	if r := api.Get(t, "/providers/provider-a/wagering/transactions/"+ext, provB); r.Status != http.StatusForbidden {
		t.Errorf("provider-b by path: %d", r.Status)
	}
	if r := api.Get(t, "/wagering/transactions/"+txID, provA); r.Status != http.StatusOK {
		t.Errorf("owner by id: %d", r.Status)
	}
	if r := api.Get(t, "/providers/provider-a/wagering/transactions/"+ext, op); r.Status != http.StatusOK || r.Str("transactionId") != txID {
		t.Errorf("operator by external id: %d", r.Status)
	}
}

func TestHTTPContract(t *testing.T) {
	e := newEnv(fastRetry)
	api := newAPI(t, e)
	op, prov := testkit.Token(t, "wallet-internal"), testkit.Token(t, "provider-a")
	walletID, playerID := api.OpenWallet(t, op, "100.00")
	submit := func(body any, key string) testkit.Response {
		return api.Do(t, "POST", "/wagering/transactions", prov, body, "Idempotency-Key", key)
	}

	bet := testkit.Wager("provider-a", uniq("bet"), playerID, walletID, "BET", "30.00", "")
	if r := submit(bet, "k1-"+walletID); r.Status != http.StatusCreated || r.Str("balance", "amount") != "70.00" {
		t.Fatalf("new bet: %d %s", r.Status, r.Raw)
	}
	if r := submit(bet, "k1-"+walletID); r.Status != http.StatusOK || !r.Bool("idempotentReplay") {
		t.Fatalf("replay: %d %s", r.Status, r.Raw)
	}
	bet["money"] = map[string]string{"amount": "31.00", "currency": "BRL"}
	if r := submit(bet, "k1-"+walletID); r.Status != http.StatusConflict || r.Str("code") != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("conflict: %d %s", r.Status, r.Raw)
	}
	big := testkit.Wager("provider-a", uniq("bet"), playerID, walletID, "BET", "500.00", "")
	if r := submit(big, uniq("k")); r.Status != http.StatusUnprocessableEntity || r.Str("failureCode") != "INSUFFICIENT_FUNDS" {
		t.Fatalf("insufficient: %d %s", r.Status, r.Raw)
	}
	early := testkit.Wager("provider-a", uniq("refund"), playerID, walletID, "REFUND", "5.00", uniq("future"))
	if r := submit(early, uniq("k")); r.Status != http.StatusAccepted || r.Str("status") != "PENDING_REFERENCE" {
		t.Fatalf("pending: %d %s", r.Status, r.Raw)
	}

	for name, body := range map[string]any{
		"malformed":        "{",
		"three decimals":   testkit.Wager("provider-a", uniq("x"), playerID, walletID, "BET", "1.001", ""),
		"negative":         testkit.Wager("provider-a", uniq("x"), playerID, walletID, "BET", "-1.00", ""),
		"loss with amount": testkit.Wager("provider-a", uniq("x"), playerID, walletID, "LOSS", "1.00", ""),
		"opening":          testkit.Wager("provider-a", uniq("x"), playerID, walletID, "OPENING", "1.00", ""),
	} {
		if r := submit(body, uniq("k")); r.Status != http.StatusBadRequest {
			t.Errorf("%s: %d %s", name, r.Status, r.Raw)
		}
	}
	if r := api.Do(t, "POST", "/wagering/transactions", prov, bet); r.Status != http.StatusBadRequest {
		t.Errorf("missing Idempotency-Key: %d", r.Status)
	}
	if r := api.Get(t, "/wallets/"+uuid.NewString(), op); r.Status != http.StatusNotFound {
		t.Errorf("unknown wallet: %d", r.Status)
	}
	unknown := testkit.Wager("provider-a", uniq("x"), playerID, uuid.NewString(), "BET", "1.00", "")
	if r := submit(unknown, uniq("k")); r.Status != http.StatusNotFound {
		t.Errorf("bet on unknown wallet: %d %s", r.Status, r.Raw)
	}

	// Ledger: opening credit + bet debit, paged one by one.
	first := api.Get(t, "/wallets/"+walletID+"/ledger?limit=1", op)
	cursor := first.Str("nextCursor")
	second := api.Get(t, "/wallets/"+walletID+"/ledger?limit=1&cursor="+neturl.QueryEscape(cursor), op)
	items, _ := second.Body["items"].([]any)
	if first.Status != 200 || cursor == "" || len(items) != 1 || items[0].(map[string]any)["direction"] != "DEBIT" {
		t.Fatalf("ledger paging: %s / %s", first.Raw, second.Raw)
	}
	if r := api.Get(t, "/wallets/"+walletID+"/ledger?cursor=bogus", op); r.Status != http.StatusBadRequest {
		t.Errorf("bad cursor: %d", r.Status)
	}

	rec := api.Do(t, "POST", "/wallets/"+walletID+"/reconciliation", op, nil)
	if rec.Status != 200 || !rec.Bool("consistent") || rec.Str("storedBalance", "amount") != "70.00" ||
		rec.Str("difference", "amount") != "0.00" {
		t.Fatalf("reconciliation: %s", rec.Raw)
	}
}
