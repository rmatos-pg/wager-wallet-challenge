package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"wager-wallet-service/internal/domain"
	"wager-wallet-service/internal/obs"
	"wager-wallet-service/internal/service"
)

// Check is a named readiness probe (postgres, sqs).
type Check struct {
	Name string
	Fn   func(context.Context) error
}

type Server struct {
	wallets  *service.Wallets
	wagers   *service.Wagers
	verifier *Verifier
	checks   []Check
	log      *slog.Logger
}

func NewServer(wallets *service.Wallets, wagers *service.Wagers, v *Verifier, checks []Check, log *slog.Logger) *Server {
	return &Server{wallets: wallets, wagers: wagers, verifier: v, checks: checks, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
	})
	mux.HandleFunc("GET /health/ready", s.ready)
	mux.Handle("GET /metrics", promhttp.Handler())

	mux.HandleFunc("POST /wallets", s.authorize(operator, s.openWallet))
	mux.HandleFunc("GET /wallets/{walletId}", s.authorize(operator, s.getWallet))
	mux.HandleFunc("GET /wallets/{walletId}/ledger", s.authorize(operator, s.ledger))
	mux.HandleFunc("POST /wallets/{walletId}/reconciliation", s.authorize(operator, s.reconcile))

	mux.HandleFunc("POST /wagering/transactions", s.authorize(provider, s.submit))
	mux.HandleFunc("GET /wagering/transactions/{transactionId}", s.authorize(providerOrOperator, s.getTransaction))
	mux.HandleFunc("GET /providers/{providerId}/wagering/transactions/{externalId}", s.authorize(providerOrOperator, s.getByExternalID))
	return s.logRequests(mux)
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	status, checks := http.StatusOK, map[string]string{}
	for _, c := range s.checks {
		checks[c.Name] = "UP"
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		err := c.Fn(ctx)
		cancel()
		if err != nil {
			checks[c.Name] = "DOWN"
			status = http.StatusServiceUnavailable
		}
	}
	overall := "UP"
	if status != http.StatusOK {
		overall = "DOWN"
	}
	writeJSON(w, status, map[string]any{"status": overall, "checks": checks})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) { w.status = code; w.ResponseWriter.WriteHeader(code) }

// logRequests sets the request id (also the event causationId) and logs one
// line per request. Bodies are never logged.
func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := r.Header.Get("X-Request-Id")
		if id == "" || len(id) > 128 {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-Id", id)
		ctx := obs.With(r.Context(), "requestId", id)
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r.WithContext(ctx))
		switch r.URL.Path {
		case "/metrics", "/health/live", "/health/ready":
			return
		}
		s.log.InfoContext(ctx, "http request", "method", r.Method, "path", r.URL.Path, "status", sw.status,
			"durationMs", time.Since(start).Milliseconds())
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{"code": code, "message": msg})
}

// fail maps service errors to the documented HTTP contract. Anything not
// recognized is treated as transient (503 + Retry-After): retrying is safe
// because every write is idempotent.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalid):
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
	case errors.Is(err, service.ErrWalletNotFound):
		writeError(w, http.StatusNotFound, "WALLET_NOT_FOUND", err.Error())
	case errors.Is(err, service.ErrNotFound):
		writeError(w, http.StatusNotFound, "NOT_FOUND", "transaction not found")
	case errors.Is(err, service.ErrWalletExists):
		writeError(w, http.StatusConflict, "WALLET_ALREADY_EXISTS", err.Error())
	case errors.Is(err, service.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "IDEMPOTENCY_CONFLICT", err.Error())
	case errors.Is(err, service.ErrExternalIDReused):
		writeError(w, http.StatusConflict, "EXTERNAL_ID_REUSED", err.Error())
	case errors.Is(err, service.ErrWalletMismatch):
		writeError(w, http.StatusUnprocessableEntity, "WALLET_MISMATCH", err.Error())
	default:
		s.log.ErrorContext(r.Context(), "request failed", "error", err)
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "temporarily unavailable, retry")
	}
}
