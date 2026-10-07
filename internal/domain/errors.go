// Package domain holds the wallet, wager transactions, ledger entries and
// integration events. It knows nothing about Fx, HTTP, SQS or SQL.
package domain

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalid marks input the caller can fix and resend (HTTP 400).
	ErrInvalid           = errors.New("invalid input")
	ErrInvalidTransition = errors.New("invalid state transition")
	ErrInsufficientFunds = errors.New("insufficient funds")
	// ErrReferenceNotReady means the reference exists but is still pending.
	ErrReferenceNotReady = errors.New("reference not ready")
)

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// FailureCode is the stable, documented reason of a REJECTED (or FAILED)
// transaction.
type FailureCode string

const (
	FailureInsufficientFunds         FailureCode = "INSUFFICIENT_FUNDS"
	FailureReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	FailureReferenceNotFound         FailureCode = "REFERENCE_NOT_FOUND"
	FailureReferenceTimeout          FailureCode = "REFERENCE_TIMEOUT"
	FailureReferenceNotProcessed     FailureCode = "REFERENCE_NOT_PROCESSED"
	FailureReferenceMismatch         FailureCode = "REFERENCE_MISMATCH"
	FailureReferenceKindNotAllowed   FailureCode = "REFERENCE_KIND_NOT_ALLOWED"
	FailureReferenceAmountMismatch   FailureCode = "REFERENCE_AMOUNT_MISMATCH"
	FailureAlreadyReversed           FailureCode = "ALREADY_REVERSED"

	// FailureInfrastructure is the only FAILED code. The service does not
	// write it today (see ARCHITECTURE.md), but the domain supports it.
	FailureInfrastructure FailureCode = "INFRASTRUCTURE_FAILURE"
)

// Rejection is a definitive business refusal: it is persisted as REJECTED.
type Rejection struct {
	Code   FailureCode
	Reason string
}

func (r *Rejection) Error() string { return string(r.Code) + ": " + r.Reason }

func reject(code FailureCode, format string, args ...any) error {
	return &Rejection{Code: code, Reason: fmt.Sprintf(format, args...)}
}
