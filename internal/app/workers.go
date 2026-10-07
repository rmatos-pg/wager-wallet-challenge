package app

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.uber.org/fx"

	"wager-wallet-service/internal/config"
	"wager-wallet-service/internal/queue"
	"wager-wallet-service/internal/service"
)

// job does one unit of work. poll is cancelled when shutdown starts (stop
// taking new work); work lives until the shutdown deadline (finish what you
// hold). It returns true when it did something, so the loop runs again
// without sleeping.
type job struct {
	name string
	run  func(poll, work context.Context) (bool, error)
}

func runWorkers(lc fx.Lifecycle, cfg config.Config, wagers *service.Wagers, relay *service.Relay, consumer *queue.Consumer, log *slog.Logger) {
	jobs := []job{
		{"sqs-consumer", consumer.Poll},
		{"pending-references", func(_, work context.Context) (bool, error) { return wagers.ResumePending(work) }},
		{"outbox-relay", func(_, work context.Context) (bool, error) { return relay.PublishBatch(work) }},
	}

	poll, stopPolling := context.WithCancel(context.Background())
	work, stopWork := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			for _, j := range jobs {
				wg.Add(1)
				go func() {
					defer wg.Done()
					loop(j, cfg.WorkerInterval, poll, work, log.With("component", "worker", "worker", j.name))
				}()
			}
			return nil
		},
		OnStop: func(ctx context.Context) error {
			stopPolling()
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			defer stopWork()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				// Deadline hit: cancel in-flight work. Uncommitted SQL rolls
				// back; SQS messages come back after their visibility timeout.
				stopWork()
				<-done
				return ctx.Err()
			}
		},
	})
}

func loop(j job, interval time.Duration, poll, work context.Context, log *slog.Logger) {
	log.Info("worker started")
	defer log.Info("worker stopped")
	for poll.Err() == nil {
		busy, err := j.run(poll, work)
		if err != nil && poll.Err() == nil {
			log.Warn("worker iteration failed", "error", err)
			busy = false
		}
		if busy {
			continue
		}
		select {
		case <-poll.Done():
		case <-time.After(interval):
		}
	}
}
