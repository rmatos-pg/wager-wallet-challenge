// Package app wires everything with Uber Fx.
//
// Shutdown order is the reverse of start order: the HTTP server stops
// accepting requests first, then the workers stop polling and finish (or
// release) what they hold, and the Postgres pool closes last.
package app

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"wager-wallet-service/internal/config"
	"wager-wallet-service/internal/domain"
	"wager-wallet-service/internal/httpapi"
	"wager-wallet-service/internal/obs"
	"wager-wallet-service/internal/queue"
	"wager-wallet-service/internal/service"
	"wager-wallet-service/internal/store"
)

func Options(cfg config.Config) fx.Option {
	return fx.Options(
		fx.Supply(cfg),
		fx.StopTimeout(cfg.ShutdownTimeout+5*time.Second),
		fx.WithLogger(func(l *slog.Logger) fxevent.Logger {
			fl := &fxevent.SlogLogger{Logger: l.With("component", "fx")}
			fl.UseLogLevel(slog.LevelDebug)
			return fl
		}),
		fx.Module("platform",
			fx.Provide(
				func(c config.Config) *slog.Logger { return obs.NewLogger(c.LogLevel, c.InstanceID) },
				newPool,
				newSQS,
			),
		),
		fx.Module("store", fx.Provide(store.New)),
		fx.Module("service",
			fx.Provide(
				func(c config.Config) domain.RetryPolicy {
					return domain.RetryPolicy{Base: c.ReferenceRetryBase, Max: c.ReferenceRetryMax, TTL: c.ReferenceTTL}
				},
				service.NewWallets,
				service.NewWagers,
				service.NewMessages,
				newRelay,
			),
		),
		fx.Module("queue",
			fx.Provide(
				queue.NewPublisher,
				func(c *sqs.Client, cfg config.Config, m *service.Messages, l *slog.Logger) *queue.Consumer {
					return queue.NewConsumer(c, cfg.InputQueueURL, cfg.DLQURL, m, 5*time.Second, l)
				},
			),
		),
		fx.Module("http", fx.Provide(httpapi.NewVerifier, newChecks, httpapi.NewServer)),
		// Order matters: workers are registered before HTTP so HTTP stops first.
		fx.Invoke(runWorkers, runHTTP),
	)
}

func newPool(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	pc.MaxConns = cfg.DBMaxConns
	pool, err := pgxpool.NewWithConfig(context.Background(), pc)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return pool.Ping(ctx) },
		OnStop: func(context.Context) error {
			pool.Close()
			log.Info("postgres pool closed", "component", "postgres")
			return nil
		},
	})
	return pool, nil
}

func newSQS(lc fx.Lifecycle, cfg config.Config) (*sqs.Client, error) {
	c, err := queue.NewClient(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error { return queue.Ping(ctx, c, cfg.InputQueueURL) }})
	return c, nil
}

func newRelay(st *store.Store, pub *queue.Publisher, cfg config.Config, log *slog.Logger) *service.Relay {
	return service.NewRelay(st, pub, cfg.InstanceID, cfg.OutboxLease, cfg.OutboxBatch, log.With("component", "outbox-relay"))
}

func newChecks(st *store.Store, c *sqs.Client, cfg config.Config) []httpapi.Check {
	return []httpapi.Check{
		{Name: "postgres", Fn: st.Ping},
		{Name: "sqs", Fn: func(ctx context.Context) error { return queue.Ping(ctx, c, cfg.InputQueueURL) }},
	}
}

func runHTTP(lc fx.Lifecycle, cfg config.Config, s *httpapi.Server, log *slog.Logger) {
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ln, err := net.Listen("tcp", cfg.HTTPAddr)
			if err != nil {
				return err
			}
			go func() {
				if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Error("http server failed", "error", err)
				}
			}()
			log.Info("http server started", "addr", cfg.HTTPAddr)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			err := srv.Shutdown(ctx)
			log.Info("http server stopped")
			return err
		},
	})
}
