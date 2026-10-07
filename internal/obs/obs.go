// Package obs holds the JSON logger and the Prometheus metrics.
package obs

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func NewLogger(level, instance string) *slog.Logger {
	var l slog.Level
	_ = l.UnmarshalText([]byte(strings.ToUpper(level)))
	return slog.New(&ctxHandler{slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l})}).With("instance", instance)
}

type attrsKey struct{}

// With returns a context whose log lines carry the given attributes
// (correlationId, messageId, transactionId, walletId, providerId...).
func With(ctx context.Context, args ...any) context.Context {
	prev, _ := ctx.Value(attrsKey{}).([]any)
	return context.WithValue(ctx, attrsKey{}, append(append([]any{}, prev...), args...))
}

type ctxHandler struct{ slog.Handler }

func (h *ctxHandler) Handle(ctx context.Context, r slog.Record) error {
	if args, ok := ctx.Value(attrsKey{}).([]any); ok {
		r.Add(args...)
	}
	return h.Handler.Handle(ctx, r)
}

func (h *ctxHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return &ctxHandler{h.Handler.WithAttrs(a)}
}
func (h *ctxHandler) WithGroup(n string) slog.Handler { return &ctxHandler{h.Handler.WithGroup(n)} }

// Metrics live in the default registry and are served on GET /metrics.
var (
	Operations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_operations_total",
		Help: "Wager operations handled, by kind, final status and whether it was an idempotent replay.",
	}, []string{"kind", "status", "replay"})

	ProcessingSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "wallet_processing_seconds",
		Help:    "Time to process one wager operation.",
		Buckets: prometheus.DefBuckets,
	})

	ConcurrencyConflicts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wallet_concurrency_conflicts_total",
		Help: "Duplicate inserts or stale versions resolved by re-reading.",
	})

	ReferenceRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_reference_retries_total",
		Help: "Pending reference attempts, by resulting status.",
	}, []string{"status"})

	SQSMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_sqs_messages_total",
		Help: "Input messages by outcome (ack, duplicate, retry, dlq).",
	}, []string{"outcome"})

	DeadLetters = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_sqs_dead_letters_total",
		Help: "Messages moved to the DLQ by the consumer, by reason.",
	}, []string{"reason"})

	OutboxPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wallet_outbox_published_total", Help: "Events published."})
	OutboxFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wallet_outbox_publish_failures_total", Help: "Failed publish attempts."})
	OutboxPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wallet_outbox_pending_events", Help: "Unpublished events."})
	OutboxLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wallet_outbox_lag_seconds", Help: "Age of the oldest unpublished event."})

	Reconciliations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_reconciliations_total",
		Help: "Reconciliations by result (consistent, divergent).",
	}, []string{"result"})
)
