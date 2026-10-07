// Package store is the PostgreSQL persistence (pgx, explicit SQL).
//
// Transaction boundary: the service opens one SQL transaction with InTx and
// every query inside the callback runs on it through *Tx. Queries outside a
// transaction use *Store directly. Both share the same query methods.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound = errors.New("not found")
	// ErrDuplicate is a unique violation (idempotency races, second wallet...).
	ErrDuplicate = errors.New("duplicate")
	// ErrStaleVersion means a conditional update matched no row.
	ErrStaleVersion = errors.New("stale wallet version")
)

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Queries struct{ q querier }

type Store struct {
	Queries
	pool *pgxpool.Pool
}

type Tx struct{ Queries }

func New(pool *pgxpool.Pool) *Store { return &Store{Queries: Queries{q: pool}, pool: pool} }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// InTx runs fn in a READ COMMITTED transaction: commit on nil, rollback on
// error. Row locks (FOR UPDATE) give the isolation each operation needs.
func (s *Store) InTx(ctx context.Context, fn func(*Tx) error) error {
	return s.run(ctx, pgx.TxOptions{}, fn)
}

// InSnapshot runs fn in a read-only REPEATABLE READ transaction, so every
// query sees the same snapshot (used by reconciliation).
func (s *Store) InSnapshot(ctx context.Context, fn func(*Tx) error) error {
	return s.run(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, fn)
}

func (s *Store) run(ctx context.Context, opts pgx.TxOptions, fn func(*Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, opts)
	if err != nil {
		return translate(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // no-op after commit
	if err := fn(&Tx{Queries{q: tx}}); err != nil {
		return err
	}
	return translate(tx.Commit(ctx))
}

// translate maps driver errors to the package errors. Other errors pass
// through and are treated as transient by callers.
func translate(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == "23505" {
		return fmt.Errorf("%w: %s", ErrDuplicate, pg.ConstraintName)
	}
	return err
}
