// Package config reads and validates the environment. The app refuses to
// start with an invalid configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	InstanceID      string
	HTTPAddr        string
	LogLevel        string
	ShutdownTimeout time.Duration

	DatabaseURL string
	DBMaxConns  int32

	OIDCIssuer   string
	OIDCJWKSURL  string
	OIDCAudience string

	AWSRegion      string
	SQSEndpoint    string
	InputQueueURL  string
	DLQURL         string
	EventsQueueURL string

	ReferenceRetryBase time.Duration
	ReferenceRetryMax  time.Duration
	ReferenceTTL       time.Duration

	OutboxLease    time.Duration
	OutboxBatch    int
	WorkerInterval time.Duration
}

func Load() (Config, error) {
	host, _ := os.Hostname()
	c := Config{
		InstanceID:      env("INSTANCE_ID", host),
		HTTPAddr:        env("HTTP_ADDR", ":8080"),
		LogLevel:        env("LOG_LEVEL", "info"),
		ShutdownTimeout: dur("SHUTDOWN_TIMEOUT", 20*time.Second),

		DatabaseURL: os.Getenv("DATABASE_URL"),
		DBMaxConns:  int32(num("DB_MAX_CONNS", 20)),

		OIDCIssuer:   os.Getenv("OIDC_ISSUER_URL"),
		OIDCJWKSURL:  os.Getenv("OIDC_JWKS_URL"),
		OIDCAudience: env("OIDC_AUDIENCE", "wager-wallet-api"),

		AWSRegion:      env("AWS_REGION", "us-east-1"),
		SQSEndpoint:    os.Getenv("SQS_ENDPOINT"),
		InputQueueURL:  os.Getenv("SQS_INPUT_QUEUE_URL"),
		DLQURL:         os.Getenv("SQS_DLQ_URL"),
		EventsQueueURL: os.Getenv("SQS_EVENTS_QUEUE_URL"),

		ReferenceRetryBase: dur("REFERENCE_RETRY_BASE", time.Second),
		ReferenceRetryMax:  dur("REFERENCE_RETRY_MAX", 30*time.Second),
		ReferenceTTL:       dur("REFERENCE_TTL", 5*time.Minute),

		OutboxLease:    dur("OUTBOX_LEASE", 30*time.Second),
		OutboxBatch:    num("OUTBOX_BATCH", 50),
		WorkerInterval: dur("WORKER_INTERVAL", 500*time.Millisecond),
	}
	return c, c.validate()
}

func (c Config) validate() error {
	var errs []error
	required := map[string]string{
		"DATABASE_URL": c.DatabaseURL, "OIDC_ISSUER_URL": c.OIDCIssuer, "OIDC_JWKS_URL": c.OIDCJWKSURL,
		"SQS_INPUT_QUEUE_URL": c.InputQueueURL, "SQS_DLQ_URL": c.DLQURL, "SQS_EVENTS_QUEUE_URL": c.EventsQueueURL,
	}
	for k, v := range required {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", k))
		}
	}
	if c.ReferenceRetryBase <= 0 || c.ReferenceRetryMax < c.ReferenceRetryBase || c.ReferenceTTL <= 0 {
		errs = append(errs, errors.New("reference retry settings are invalid"))
	}
	if c.OutboxLease <= 0 || c.OutboxBatch <= 0 || c.WorkerInterval <= 0 || c.ShutdownTimeout <= 0 || c.DBMaxConns <= 0 {
		errs = append(errs, errors.New("durations, batch sizes and pool size must be positive"))
	}
	return errors.Join(errs...)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func dur(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return -1 // caught by validate
	}
	return d
}

func num(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return -1
	}
	return n
}
