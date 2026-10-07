package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"wager-wallet-service/internal/money"
)

var now = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.Parse(s, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func openWallet(t *testing.T, initial string) *Wallet {
	t.Helper()
	w, _, err := Open(OpenWallet{WalletID: uuid.New(), PlayerID: uuid.New(), Initial: brl(t, initial),
		OpeningTxID: uuid.New(), EntryID: uuid.New(), Now: now})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func request(w *Wallet, kind Kind, amount money.Money, ref string) Request {
	return Request{ProviderID: "provider-a", ExternalID: "ext-" + string(kind), IdempotencyKey: "provider-a:ext-" + string(kind),
		PlayerID: w.PlayerID(), WalletID: w.ID(), RoundID: "round-1", GameID: "game", Kind: kind, Money: amount,
		ReferenceExternalID: ref}
}

func TestOpenWallet(t *testing.T) {
	w, entry, err := Open(OpenWallet{WalletID: uuid.New(), PlayerID: uuid.New(), Initial: brl(t, "100.00"),
		OpeningTxID: uuid.New(), EntryID: uuid.New(), Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 1 || w.Balance().Amount() != "100.00" {
		t.Fatalf("version=%d balance=%s", w.Version(), w.Balance())
	}
	if entry == nil || entry.Direction() != Credit || entry.BalanceBefore().Amount() != "0.00" || entry.WalletVersion() != 1 {
		t.Fatalf("opening entry = %+v", entry)
	}

	_, entry, err = Open(OpenWallet{WalletID: uuid.New(), PlayerID: uuid.New(), Initial: brl(t, "0.00"), Now: now})
	if err != nil || entry != nil {
		t.Fatalf("zero opening must not create an entry: %v %v", entry, err)
	}
	if _, _, err := Open(OpenWallet{WalletID: uuid.New(), PlayerID: uuid.New(), Initial: brl(t, "-1.00"), Now: now}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative opening: %v", err)
	}
	if _, _, err := Open(OpenWallet{PlayerID: uuid.New(), Initial: brl(t, "1.00"), Now: now}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing id: %v", err)
	}
}

func TestOpeningTransaction(t *testing.T) {
	w := openWallet(t, "50.00")
	tx, err := NewOpening(uuid.New(), w.ID(), w.PlayerID(), w.Balance(), now)
	if err != nil {
		t.Fatal(err)
	}
	s := tx.State()
	if s.Status != StatusProcessed || s.Origin != OriginInternal || s.ProviderID != "" || s.IdempotencyKey != "" {
		t.Fatalf("opening = %+v", s)
	}
	evs := EventsFor(uuid.New, EventMeta{CorrelationID: s.ID.String(), At: now}, tx, nil)
	if len(evs) != 1 || evs[0].EventType != EventTransactionProcessed || evs[0].Version != 1 {
		t.Fatalf("events = %+v", evs)
	}
	if _, err := NewOpening(uuid.New(), w.ID(), w.PlayerID(), brl(t, "0.00"), now); !errors.Is(err, ErrInvalid) {
		t.Fatal("zero opening transaction must be rejected")
	}
}

func TestDebitCredit(t *testing.T) {
	w := openWallet(t, "100.00")
	e, err := w.Debit(uuid.New(), uuid.New(), brl(t, "80.00"), now)
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 || e.BalanceAfter().Amount() != "20.00" {
		t.Fatalf("balance=%s version=%d", w.Balance(), w.Version())
	}

	_, err = w.Debit(uuid.New(), uuid.New(), brl(t, "20.01"), now)
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("overdraft: %v", err)
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatal("failed debit must not change the wallet")
	}

	usd, _ := money.Parse("1.00", "USD")
	if _, err := w.Credit(uuid.New(), uuid.New(), usd, now); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Fatalf("currency: %v", err)
	}
	if _, err := w.Credit(uuid.New(), uuid.New(), brl(t, "0.00"), now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero credit: %v", err)
	}
}

func TestRehydrateWalletHasNoSideEffects(t *testing.T) {
	w, err := RehydrateWallet(uuid.New(), uuid.New(), brl(t, "10.00"), 7, now, now)
	if err != nil || w.Version() != 7 || w.Balance().Amount() != "10.00" {
		t.Fatalf("%v %v", w, err)
	}
	if _, err := RehydrateWallet(uuid.New(), uuid.New(), brl(t, "-1.00"), 1, now, now); err == nil {
		t.Fatal("negative stored balance must be rejected")
	}
	if _, err := RehydrateWallet(uuid.New(), uuid.New(), brl(t, "1.00"), 0, now, now); err == nil {
		t.Fatal("version 0 must be rejected")
	}
}

func TestLedgerEntryMath(t *testing.T) {
	id, w, tx := uuid.New(), uuid.New(), uuid.New()
	if _, err := NewLedgerEntry(id, w, tx, Debit, brl(t, "10.00"), brl(t, "30.00"), brl(t, "20.00"), 2, now); err != nil {
		t.Fatal(err)
	}
	if _, err := NewLedgerEntry(id, w, tx, Debit, brl(t, "10.00"), brl(t, "30.00"), brl(t, "25.00"), 2, now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong math accepted: %v", err)
	}
	if _, err := NewLedgerEntry(id, w, tx, Credit, brl(t, "10.00"), brl(t, "30.00"), brl(t, "40.00"), 2, now); err != nil {
		t.Fatal(err)
	}
}

func TestRequestValidation(t *testing.T) {
	w := openWallet(t, "10.00")
	cases := []struct {
		name string
		req  Request
		ok   bool
	}{
		{"bet", request(w, KindBet, brl(t, "1.00"), ""), true},
		{"win without ref", request(w, KindWin, brl(t, "1.00"), ""), true},
		{"loss zero", request(w, KindLoss, brl(t, "0.00"), ""), true},
		{"refund", request(w, KindRefund, brl(t, "1.00"), "bet-1"), true},
		{"rollback", request(w, KindRollback, brl(t, "1.00"), "bet-1"), true},
		{"bet zero", request(w, KindBet, brl(t, "0.00"), ""), false},
		{"win zero", request(w, KindWin, brl(t, "0.00"), ""), false},
		{"refund zero", request(w, KindRefund, brl(t, "0.00"), "bet-1"), false},
		{"rollback zero", request(w, KindRollback, brl(t, "0.00"), "bet-1"), false},
		{"loss non zero", request(w, KindLoss, brl(t, "1.00"), ""), false},
		{"refund without ref", request(w, KindRefund, brl(t, "1.00"), ""), false},
		{"rollback without ref", request(w, KindRollback, brl(t, "1.00"), ""), false},
		{"opening", request(w, KindOpening, brl(t, "1.00"), ""), false},
		{"unknown", request(w, Kind("JACKPOT"), brl(t, "1.00"), ""), false},
		{"negative", request(w, KindBet, brl(t, "-1.00"), ""), false},
		{"no money", request(w, KindBet, money.Money{}, ""), false},
	}
	for _, c := range cases {
		err := c.req.Validate()
		if c.ok && err != nil {
			t.Errorf("%s: %v", c.name, err)
		}
		if !c.ok && !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: want ErrInvalid, got %v", c.name, err)
		}
	}
}

func TestHash(t *testing.T) {
	w := openWallet(t, "10.00")
	a := request(w, KindBet, brl(t, "25.00"), "")
	b := a
	b.IdempotencyKey = "other-key"
	if a.Hash() != b.Hash() {
		t.Fatal("the idempotency key must not be part of the hash")
	}
	c := a
	c.Money = brl(t, "25.01")
	if a.Hash() == c.Hash() {
		t.Fatal("different amount must change the hash")
	}
	d := a
	d.RoundID = "round-2"
	if a.Hash() == d.Hash() {
		t.Fatal("different round must change the hash")
	}
	if len(a.Hash()) != 64 {
		t.Fatal("hash must be hex SHA-256")
	}
}

func TestHashKnownVector(t *testing.T) {
	req := Request{ProviderID: "provider-a", ExternalID: "transaction-123", IdempotencyKey: "provider-a:transaction-123",
		PlayerID: uuid.MustParse("0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"), WalletID: uuid.MustParse("0192f291-27dd-7d3f-8071-5f8685deef37"),
		RoundID: "round-987", GameID: "fortune-chimp", Kind: KindBet, Money: brl(t, "25.00")}
	// sha256 of {"externalTransactionId":"transaction-123","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"},"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","providerId":"provider-a","roundId":"round-987","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37"}
	const want = "629836932b79106b99523d06a1e7fa80689b0ea1e1c47aa3f0a5a2c87d0c4344"
	if got := req.Hash(); got != want {
		t.Fatalf("hash = %s", got)
	}
}

func TestStateMachine(t *testing.T) {
	w := openWallet(t, "10.00")
	tx, err := NewExternal(uuid.New(), request(w, KindBet, brl(t, "1.00"), ""), now)
	if err != nil || tx.Status() != StatusPending {
		t.Fatalf("%v %v", tx, err)
	}
	if err := tx.Process(brl(t, "9.00"), uuid.Nil, now); err != nil {
		t.Fatal(err)
	}
	if err := tx.Reject(FailureInsufficientFunds, now); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal("terminal transaction accepted a transition")
	}
	if err := tx.Fail(now); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal("terminal transaction accepted Fail")
	}

	ref, _ := NewExternal(uuid.New(), request(w, KindRefund, brl(t, "1.00"), "bet-1"), now)
	if err := ref.AwaitReference(now.Add(time.Second), now.Add(time.Minute), now); err != nil {
		t.Fatal(err)
	}
	if err := ref.Reschedule(now.Add(2*time.Second), now); err != nil || ref.State().Attempts != 1 {
		t.Fatalf("reschedule: %v", err)
	}
	if err := ref.Reject(FailureReferenceNotFound, now); err != nil || ref.Status() != StatusRejected {
		t.Fatalf("reject: %v", err)
	}

	bet, _ := NewExternal(uuid.New(), request(w, KindBet, brl(t, "1.00"), ""), now)
	if err := bet.AwaitReference(now, now.Add(time.Minute), now); !errors.Is(err, ErrInvalid) {
		t.Fatal("operation without reference cannot wait")
	}
	if err := bet.Reject(FailureInfrastructure, now); !errors.Is(err, ErrInvalid) {
		t.Fatal("infrastructure code is not a rejection")
	}
	if err := bet.Fail(now); err != nil || bet.State().FailureCode != FailureInfrastructure {
		t.Fatalf("fail: %v", err)
	}
}

func TestRehydrateRejectsPending(t *testing.T) {
	if _, err := Rehydrate(TxState{ID: uuid.New(), WalletID: uuid.New(), Money: brl(t, "1.00"), Status: StatusPending}); err == nil {
		t.Fatal("PENDING is never stored")
	}
}

func processed(t *testing.T, w *Wallet, kind Kind, amount, ref string) *Transaction {
	t.Helper()
	r := request(w, kind, brl(t, amount), ref)
	tx, err := NewExternal(uuid.New(), r, now)
	if err != nil {
		t.Fatal(err)
	}
	refID := uuid.Nil
	if ref != "" {
		refID = uuid.New()
	}
	if err := tx.Process(brl(t, "10.00"), refID, now); err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestCheckReference(t *testing.T) {
	w := openWallet(t, "10.00")
	bet := processed(t, w, KindBet, "5.00", "")
	win := processed(t, w, KindWin, "7.00", "")

	newOp := func(kind Kind, amount string) *Transaction {
		tx, err := NewExternal(uuid.New(), request(w, kind, brl(t, amount), bet.State().ExternalID), now)
		if err != nil {
			t.Fatal(err)
		}
		return tx
	}

	if err := newOp(KindRefund, "5.00").CheckReference(bet); err != nil {
		t.Fatalf("refund of bet: %v", err)
	}
	var rj *Rejection
	if err := newOp(KindRefund, "4.00").CheckReference(bet); !errors.As(err, &rj) || rj.Code != FailureReferenceAmountMismatch {
		t.Fatalf("partial refund: %v", err)
	}
	if err := newOp(KindRefund, "7.00").CheckReference(win); !errors.As(err, &rj) || rj.Code != FailureReferenceKindNotAllowed {
		t.Fatalf("refund of win: %v", err)
	}
	if err := newOp(KindRollback, "7.00").CheckReference(win); err != nil {
		t.Fatalf("rollback of win: %v", err)
	}

	other := openWallet(t, "10.00")
	foreign := processed(t, other, KindBet, "5.00", "")
	if err := newOp(KindRefund, "5.00").CheckReference(foreign); !errors.As(err, &rj) || rj.Code != FailureReferenceMismatch {
		t.Fatalf("other wallet: %v", err)
	}

	pending, _ := NewExternal(uuid.New(), request(w, KindRefund, brl(t, "5.00"), "x"), now)
	_ = pending.AwaitReference(now, now.Add(time.Minute), now)
	if err := newOp(KindRollback, "5.00").CheckReference(pending); !errors.Is(err, ErrReferenceNotReady) {
		t.Fatalf("pending reference: %v", err)
	}

	rejected, _ := NewExternal(uuid.New(), request(w, KindBet, brl(t, "5.00"), ""), now)
	_ = rejected.Reject(FailureInsufficientFunds, now)
	if err := newOp(KindRefund, "5.00").CheckReference(rejected); !errors.As(err, &rj) || rj.Code != FailureReferenceNotProcessed {
		t.Fatalf("rejected reference: %v", err)
	}
}

func TestEffects(t *testing.T) {
	w := openWallet(t, "10.00")
	bet := processed(t, w, KindBet, "5.00", "")
	win := processed(t, w, KindWin, "5.00", "")
	op := func(k Kind, amount string) *Transaction {
		ref := ""
		if k.IsReversal() {
			ref = "r"
		}
		tx, _ := NewExternal(uuid.New(), request(w, k, brl(t, amount), ref), now)
		return tx
	}
	cases := []struct {
		tx   *Transaction
		ref  *Transaction
		want Effect
	}{
		{op(KindBet, "1.00"), nil, DebitEffect},
		{op(KindWin, "1.00"), nil, CreditEffect},
		{op(KindLoss, "0.00"), nil, NoEffect},
		{op(KindRefund, "1.00"), bet, CreditEffect},
		{op(KindRollback, "1.00"), bet, CreditEffect},
		{op(KindRollback, "1.00"), win, DebitEffect},
	}
	for i, c := range cases {
		if got := c.tx.Effect(c.ref); got != c.want {
			t.Errorf("case %d (%s): got %v want %v", i, c.tx.Kind(), got, c.want)
		}
	}
	if op(KindRollback, "1.00").InsufficientFundsCode() != FailureReversalInsufficientFunds {
		t.Fatal("reversal must have its own insufficient funds code")
	}
	if op(KindBet, "1.00").InsufficientFundsCode() != FailureInsufficientFunds {
		t.Fatal("bet code")
	}
}

func TestEvents(t *testing.T) {
	w := openWallet(t, "100.00")
	tx, _ := NewExternal(uuid.New(), request(w, KindBet, brl(t, "25.00"), ""), now)
	entry, err := w.Debit(uuid.New(), tx.ID(), tx.Money(), now)
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Process(w.Balance(), uuid.Nil, now)
	evs := EventsFor(uuid.New, EventMeta{CorrelationID: "k", At: now}, tx, &entry)
	if len(evs) != 2 || evs[0].EventType != EventTransactionProcessed || evs[1].EventType != EventBalanceChanged {
		t.Fatalf("events = %+v", evs)
	}
	bc := evs[1].Data.(BalanceChanged)
	if bc.BalanceBefore.Amount() != "100.00" || bc.BalanceAfter.Amount() != "75.00" || bc.WalletVersion != 2 || evs[1].AggregateID != w.ID() {
		t.Fatalf("balance changed = %+v", bc)
	}

	loss, _ := NewExternal(uuid.New(), request(w, KindLoss, brl(t, "0.00"), ""), now)
	_ = loss.Process(w.Balance(), uuid.Nil, now)
	if evs := EventsFor(uuid.New, EventMeta{At: now}, loss, nil); len(evs) != 1 || evs[0].EventType != EventTransactionProcessed {
		t.Fatalf("LOSS must only produce WagerTransactionProcessed: %+v", evs)
	}
}

func TestRetryPolicy(t *testing.T) {
	p := RetryPolicy{Base: time.Second, Max: 8 * time.Second, TTL: time.Minute}
	want := []time.Duration{1, 2, 4, 8, 8}
	for i, w := range want {
		if got := p.Delay(i); got != w*time.Second {
			t.Fatalf("delay(%d) = %v", i, got)
		}
	}
}
