package httpapi

import (
	"context"
	"net/http"
	"slices"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"

	"wager-wallet-service/internal/config"
)

const (
	RoleProvider = "provider"        // submits and reads its own operations
	RoleOperator = "wallet-operator" // internal service: wallets, ledger, reconciliation, any transaction
)

// Principal is the authenticated caller. ProviderID comes only from the
// provider_id claim set by the IdP, never from the request.
type Principal struct {
	ClientID   string
	ProviderID string
	Roles      []string
}

func (p Principal) Is(role string) bool { return slices.Contains(p.Roles, role) }

func (p Principal) IsProvider() bool { return p.Is(RoleProvider) && p.ProviderID != "" }

// Verifier checks RS256 signature (JWKS fetched from Keycloak and cached),
// issuer, audience and expiry of access tokens.
type Verifier struct{ v *oidc.IDTokenVerifier }

func NewVerifier(cfg config.Config) *Verifier {
	keys := oidc.NewRemoteKeySet(context.Background(), cfg.OIDCJWKSURL)
	return &Verifier{v: oidc.NewVerifier(cfg.OIDCIssuer, keys, &oidc.Config{
		ClientID: cfg.OIDCAudience, SupportedSigningAlgs: []string{oidc.RS256},
	})}
}

func (v *Verifier) Verify(ctx context.Context, raw string) (Principal, bool) {
	tok, err := v.v.Verify(ctx, raw)
	if err != nil {
		return Principal{}, false
	}
	var c struct {
		Type        string `json:"typ"`
		ClientID    string `json:"azp"`
		ProviderID  string `json:"provider_id"`
		RealmAccess struct {
			Roles []string `json:"roles"`
		} `json:"realm_access"`
	}
	// Keycloak marks access tokens with typ=Bearer; ID/refresh tokens are refused.
	if err := tok.Claims(&c); err != nil || c.Type != "Bearer" {
		return Principal{}, false
	}
	return Principal{ClientID: c.ClientID, ProviderID: c.ProviderID, Roles: c.RealmAccess.Roles}, true
}

type principalKey struct{}

func principalFrom(ctx context.Context) Principal {
	p, _ := ctx.Value(principalKey{}).(Principal)
	return p
}

// authorize wraps a handler: valid token required (401), and the caller must
// pass allow (403).
func (s *Server) authorize(allow func(Principal) bool, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || raw == "" {
			writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "missing bearer token")
			return
		}
		p, ok := s.verifier.Verify(r.Context(), raw)
		if !ok {
			writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid or expired token")
			return
		}
		if !allow(p) {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "not allowed for this client")
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
	}
}

func operator(p Principal) bool           { return p.Is(RoleOperator) }
func provider(p Principal) bool           { return p.IsProvider() }
func providerOrOperator(p Principal) bool { return p.IsProvider() || p.Is(RoleOperator) }
