package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"wager-wallet-service/internal/domain"
	"wager-wallet-service/internal/money"
	"wager-wallet-service/internal/service"
)

const maxBody = 64 << 10

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "malformed JSON body")
		return false
	}
	return true
}

func pathUUID(w http.ResponseWriter, r *http.Request, name string) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", name+" must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}

// --- wallets (internal service only) ---

type walletJSON struct {
	ID        uuid.UUID   `json:"id"`
	PlayerID  uuid.UUID   `json:"playerId"`
	Balance   money.Money `json:"balance"`
	Version   int64       `json:"version"`
	CreatedAt time.Time   `json:"createdAt"`
	UpdatedAt time.Time   `json:"updatedAt"`
}

func toWalletJSON(w *domain.Wallet) walletJSON {
	return walletJSON{ID: w.ID(), PlayerID: w.PlayerID(), Balance: w.Balance(), Version: w.Version(),
		CreatedAt: w.CreatedAt(), UpdatedAt: w.UpdatedAt()}
}

func (s *Server) openWallet(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PlayerID       string     `json:"playerId"`
		InitialBalance money.JSON `json:"initialBalance"`
	}
	if !decode(w, r, &body) {
		return
	}
	player, err := uuid.Parse(body.PlayerID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "playerId must be a UUID")
		return
	}
	initial, err := money.ParseExternal(body.InitialBalance.Amount, body.InitialBalance.Currency)
	if err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "initialBalance: "+err.Error())
		return
	}
	wallet, err := s.wallets.Open(r.Context(), player, initial)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toWalletJSON(wallet))
}

func (s *Server) getWallet(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "walletId")
	if !ok {
		return
	}
	wallet, err := s.wallets.Get(r.Context(), id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toWalletJSON(wallet))
}

type entryJSON struct {
	ID            uuid.UUID        `json:"id"`
	TransactionID uuid.UUID        `json:"transactionId"`
	Direction     domain.Direction `json:"direction"`
	Money         money.Money      `json:"money"`
	BalanceBefore money.Money      `json:"balanceBefore"`
	BalanceAfter  money.Money      `json:"balanceAfter"`
	WalletVersion int64            `json:"walletVersion"`
	CreatedAt     time.Time        `json:"createdAt"`
}

func (s *Server) ledger(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "walletId")
	if !ok {
		return
	}
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "limit must be a positive integer")
			return
		}
		limit = n
	}
	entries, next, err := s.wallets.Ledger(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	items := make([]entryJSON, len(entries))
	for i, e := range entries {
		items[i] = entryJSON{ID: e.ID(), TransactionID: e.TransactionID(), Direction: e.Direction(), Money: e.Amount(),
			BalanceBefore: e.BalanceBefore(), BalanceAfter: e.BalanceAfter(), WalletVersion: e.WalletVersion(), CreatedAt: e.CreatedAt()}
	}
	resp := map[string]any{"items": items}
	if next != "" {
		resp["nextCursor"] = next
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) reconcile(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "walletId")
	if !ok {
		return
	}
	rec, err := s.wallets.Reconcile(r.Context(), id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"walletId": rec.WalletID, "storedBalance": rec.Stored, "calculatedBalance": rec.Calculated,
		"difference": rec.Difference, "consistent": rec.Consistent, "checkedEntries": rec.Checked,
	})
}

// --- wager transactions ---

type txJSON struct {
	TransactionID                  uuid.UUID     `json:"transactionId"`
	ProviderID                     string        `json:"providerId,omitempty"`
	ExternalTransactionID          string        `json:"externalTransactionId,omitempty"`
	WalletID                       uuid.UUID     `json:"walletId"`
	Kind                           domain.Kind   `json:"kind"`
	Status                         domain.Status `json:"status"`
	Money                          money.Money   `json:"money"`
	Balance                        *money.Money  `json:"balance,omitempty"`
	FailureCode                    string        `json:"failureCode,omitempty"`
	ReferenceExternalTransactionID string        `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         *uuid.UUID    `json:"referenceTransactionId,omitempty"`
	NextAttemptAt                  *time.Time    `json:"nextAttemptAt,omitempty"`
	ExpiresAt                      *time.Time    `json:"expiresAt,omitempty"`
	IdempotentReplay               *bool         `json:"idempotentReplay,omitempty"`
	CreatedAt                      time.Time     `json:"createdAt"`
	CompletedAt                    *time.Time    `json:"completedAt,omitempty"`
}

func toTxJSON(t *domain.Transaction) txJSON {
	s := t.State()
	out := txJSON{TransactionID: s.ID, ProviderID: s.ProviderID, ExternalTransactionID: s.ExternalID, WalletID: s.WalletID,
		Kind: s.Kind, Status: s.Status, Money: s.Money, FailureCode: string(s.FailureCode),
		ReferenceExternalTransactionID: s.ReferenceExternalID, CreatedAt: s.CreatedAt}
	if s.ResultBalance.Valid() {
		b := s.ResultBalance
		out.Balance = &b
	}
	if s.ReferenceID != uuid.Nil {
		out.ReferenceTransactionID = &s.ReferenceID
	}
	if s.Status == domain.StatusPendingReference {
		out.NextAttemptAt, out.ExpiresAt = &s.NextAttemptAt, &s.ExpiresAt
	}
	if !s.CompletedAt.IsZero() {
		out.CompletedAt = &s.CompletedAt
	}
	return out
}

// submit: 201 processed, 200 replay of a processed one, 202 waiting for the
// reference, 422 rejected by a business rule (new or replayed).
func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "Idempotency-Key header is required")
		return
	}
	var in service.WagerInput
	if !decode(w, r, &in) {
		return
	}
	if in.ProviderID != p.ProviderID {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "providerId does not match the authenticated provider")
		return
	}
	req, err := in.Request(key)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	res, err := s.wagers.Process(r.Context(), req, service.Options{CausationID: w.Header().Get("X-Request-Id")})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	body := toTxJSON(res.Tx)
	body.IdempotentReplay = &res.Replay
	status := http.StatusCreated
	switch {
	case res.Tx.Status() == domain.StatusRejected:
		status = http.StatusUnprocessableEntity
	case res.Tx.Status() == domain.StatusPendingReference:
		status = http.StatusAccepted
	case res.Replay:
		status = http.StatusOK
	}
	writeJSON(w, status, body)
}

// A provider only sees its own transactions; for others it gets 404, so it
// cannot even learn that an id exists.
func (s *Server) getTransaction(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "transactionId")
	if !ok {
		return
	}
	t, err := s.wagers.Get(r.Context(), id)
	if err == nil && !principalFrom(r.Context()).Is(RoleOperator) && t.State().ProviderID != principalFrom(r.Context()).ProviderID {
		err = service.ErrNotFound
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTxJSON(t))
}

func (s *Server) getByExternalID(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	providerID := r.PathValue("providerId")
	if !p.Is(RoleOperator) && providerID != p.ProviderID {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "providers can only read their own transactions")
		return
	}
	t, err := s.wagers.GetByExternalID(r.Context(), providerID, r.PathValue("externalId"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTxJSON(t))
}
