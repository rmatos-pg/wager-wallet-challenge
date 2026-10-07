// Package testkit: real Keycloak tokens and a tiny JSON HTTP client, shared by
// the integration and multi-instance tests.
package testkit

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// KeycloakURL is where tokens are requested (http://keycloak:8180 inside the
// compose network). The issuer in the token is always http://localhost:8180.
func KeycloakURL() string { return Env("KEYCLOAK_URL", "http://localhost:8180") }

func Token(t testing.TB, client string) string {
	t.Helper()
	resp, err := http.PostForm(KeycloakURL()+"/realms/wager/protocol/openid-connect/token", url.Values{
		"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {client + "-secret"},
	})
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&body) != nil {
		t.Fatalf("token for %s: %s", client, resp.Status)
	}
	return body.AccessToken
}

type Client struct {
	Base string
	HTTP *http.Client
}

func NewClient(base string) *Client {
	return &Client{Base: strings.TrimRight(base, "/"), HTTP: &http.Client{Timeout: 30 * time.Second}}
}

type Response struct {
	Status int
	Body   map[string]any
	Raw    string
}

// Str reads a nested string field: r.Str("balance", "amount").
func (r Response) Str(path ...string) string {
	var cur any = r.Body
	for _, p := range path {
		m, _ := cur.(map[string]any)
		cur = m[p]
	}
	s, _ := cur.(string)
	return s
}

func (r Response) Bool(key string) bool { b, _ := r.Body[key].(bool); return b }

// Do sends JSON. headers are name/value pairs.
func (c *Client) Do(t testing.TB, method, path, token string, body any, headers ...string) Response {
	t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		raw, _ := json.Marshal(b)
		rd = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, c.Base+path, rd)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := Response{Status: resp.StatusCode, Raw: string(raw)}
	_ = json.Unmarshal(raw, &out.Body)
	return out
}

func (c *Client) Get(t testing.TB, path, token string) Response {
	return c.Do(t, "GET", path, token, nil)
}

// OpenWallet opens a BRL wallet for a new player.
func (c *Client) OpenWallet(t testing.TB, token, amount string) (walletID, playerID string) {
	t.Helper()
	playerID = uuid.NewString()
	res := c.Do(t, "POST", "/wallets", token, map[string]any{
		"playerId": playerID, "initialBalance": map[string]string{"amount": amount, "currency": "BRL"}})
	if res.Status != http.StatusCreated {
		t.Fatalf("open wallet: %d %s", res.Status, res.Raw)
	}
	return res.Str("id"), playerID
}

// Wager is the POST /wagering/transactions body.
func Wager(provider, ext, playerID, walletID, kind, amount, ref string) map[string]any {
	b := map[string]any{
		"providerId": provider, "externalTransactionId": ext, "playerId": playerID, "walletId": walletID,
		"roundId": "round-1", "gameId": "fortune-chimp", "kind": kind,
		"money": map[string]string{"amount": amount, "currency": "BRL"},
	}
	if ref != "" {
		b["referenceExternalTransactionId"] = ref
	}
	return b
}

func Uniq(prefix string) string { return prefix + "-" + uuid.NewString()[:8] }
