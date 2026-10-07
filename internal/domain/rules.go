package domain

import "time"

// Effect is what an operation does to the balance.
type Effect int

const (
	NoEffect Effect = iota
	DebitEffect
	CreditEffect
)

// CheckReference validates an operation against its resolved reference.
// Returns ErrReferenceNotReady while the reference is still pending, a
// *Rejection for a definitive mismatch, or nil. "Already reversed" is checked
// by the service (and enforced by a unique index).
func (t *Transaction) CheckReference(ref *Transaction) error {
	r := ref.s
	switch r.Status {
	case StatusPending, StatusPendingReference:
		return ErrReferenceNotReady
	case StatusRejected, StatusFailed:
		return reject(FailureReferenceNotProcessed, "reference %s is %s", r.ExternalID, r.Status)
	}
	if r.Origin != OriginExternal || r.ProviderID != t.s.ProviderID || r.PlayerID != t.s.PlayerID ||
		r.WalletID != t.s.WalletID || r.Money.Currency() != t.s.Money.Currency() || r.RoundID != t.s.RoundID {
		return reject(FailureReferenceMismatch, "reference must match provider, player, wallet, currency and round")
	}

	ok := false
	switch t.s.Kind {
	case KindWin, KindRefund:
		ok = r.Kind == KindBet
	case KindRollback:
		ok = r.Kind == KindBet || r.Kind == KindWin || r.Kind == KindRefund
	}
	if !ok {
		return reject(FailureReferenceKindNotAllowed, "%s cannot reference %s", t.s.Kind, r.Kind)
	}
	if t.s.Kind.IsReversal() && !r.Money.Equal(t.s.Money) {
		return reject(FailureReferenceAmountMismatch, "reversal must equal %s", r.Money)
	}
	return nil
}

// Effect returns the movement to apply. ROLLBACK mirrors its reference.
func (t *Transaction) Effect(ref *Transaction) Effect {
	switch t.s.Kind {
	case KindBet:
		return DebitEffect
	case KindWin, KindRefund, KindOpening:
		return CreditEffect
	case KindRollback:
		if ref != nil && ref.s.Kind == KindBet {
			return CreditEffect
		}
		return DebitEffect // undoing a WIN or a REFUND
	}
	return NoEffect // LOSS
}

// InsufficientFundsCode keeps "reversal cannot debit" distinct from "bet
// without balance".
func (t *Transaction) InsufficientFundsCode() FailureCode {
	if t.s.Kind.IsReversal() {
		return FailureReversalInsufficientFunds
	}
	return FailureInsufficientFunds
}

// RetryPolicy: exponential backoff for pending references, capped, until TTL.
type RetryPolicy struct {
	Base time.Duration
	Max  time.Duration
	TTL  time.Duration
}

// Delay before attempt n (0-based).
func (p RetryPolicy) Delay(n int) time.Duration {
	d := p.Base
	for i := 0; i < n && d < p.Max; i++ {
		d *= 2
	}
	return min(d, p.Max)
}

// Next is the time of the next attempt, never after the expiry so the last
// attempt always happens.
func (p RetryPolicy) Next(t *Transaction, now time.Time) time.Time {
	next := now.Add(p.Delay(t.s.Attempts + 1))
	if next.After(t.s.ExpiresAt) {
		return t.s.ExpiresAt
	}
	return next
}

func (p RetryPolicy) Expired(t *Transaction, now time.Time) bool {
	return !now.Before(t.s.ExpiresAt)
}
