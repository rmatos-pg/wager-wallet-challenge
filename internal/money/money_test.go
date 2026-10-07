package money

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestParseExternal(t *testing.T) {
	ok := map[string]int64{
		"0.00":                 0,
		"0.05":                 5,
		"25.00":                2500,
		"1000.99":              100099,
		"92233720368547758.07": math.MaxInt64,
	}
	for in, want := range ok {
		m, err := ParseExternal(in, "BRL")
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if m.Units() != want || m.Amount() != in {
			t.Fatalf("%q: got %d %s", in, m.Units(), m.Amount())
		}
	}

	bad := map[string]error{
		"":                     ErrInvalidAmount,
		"NaN":                  ErrInvalidAmount,
		"Infinity":             ErrInvalidAmount,
		"1e3":                  ErrInvalidAmount,
		"+1.00":                ErrInvalidAmount,
		" 1.00":                ErrInvalidAmount,
		"01.00":                ErrInvalidAmount,
		".50":                  ErrInvalidAmount,
		"1.":                   ErrInvalidAmount,
		"1,00":                 ErrInvalidAmount,
		"10":                   ErrInvalidScale,
		"1.5":                  ErrInvalidScale,
		"1.005":                ErrInvalidScale,
		"-1.00":                ErrNegativeAmount,
		"92233720368547758.08": ErrOverflow,
	}
	for in, want := range bad {
		if _, err := ParseExternal(in, "BRL"); !errors.Is(err, want) {
			t.Fatalf("%q: got %v, want %v", in, err, want)
		}
	}
}

func TestParseInternalAllowsNegative(t *testing.T) {
	m, err := Parse("-0.05", "BRL")
	if err != nil || m.Units() != -5 || m.Amount() != "-0.05" {
		t.Fatalf("got %v %v", m, err)
	}
	if m, err := Parse("-92233720368547758.08", "BRL"); err != nil || m.Units() != math.MinInt64 {
		t.Fatalf("min: %v %v", m, err)
	}
	if _, err := Parse("-0.00", "BRL"); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("negative zero: %v", err)
	}
}

func TestCurrency(t *testing.T) {
	for _, c := range []string{"", "BR", "brl", "BRLX", "B1L"} {
		if _, err := ParseExternal("1.00", c); !errors.Is(err, ErrInvalidCurrency) {
			t.Fatalf("%q: %v", c, err)
		}
	}
}

func TestArithmetic(t *testing.T) {
	a, _ := Parse("10.50", "BRL")
	b, _ := Parse("0.75", "BRL")
	sum, _ := a.Add(b)
	diff, _ := b.Sub(a)
	neg, _ := a.Neg()
	if sum.Amount() != "11.25" || diff.Amount() != "-9.75" || neg.Amount() != "-10.50" {
		t.Fatalf("sum=%s diff=%s neg=%s", sum, diff, neg)
	}
	if c, _ := a.Cmp(b); c != 1 {
		t.Fatalf("cmp = %d", c)
	}
	z, _ := Zero("BRL")
	if !z.IsZero() || z.Amount() != "0.00" {
		t.Fatalf("zero = %s", z)
	}
}

func TestCurrencyMismatch(t *testing.T) {
	brl, _ := Parse("1.00", "BRL")
	usd, _ := Parse("1.00", "USD")
	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatal(err)
	}
	if _, err := brl.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatal(err)
	}
	if _, err := brl.Cmp(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatal(err)
	}
	if brl.Equal(usd) {
		t.Fatal("different currencies must not be equal")
	}
}

func TestOverflow(t *testing.T) {
	max, _ := FromUnits(math.MaxInt64, "BRL")
	min, _ := FromUnits(math.MinInt64, "BRL")
	one, _ := FromUnits(1, "BRL")
	if _, err := max.Add(one); !errors.Is(err, ErrOverflow) {
		t.Fatal("add")
	}
	if _, err := min.Sub(one); !errors.Is(err, ErrOverflow) {
		t.Fatal("sub")
	}
	if _, err := min.Neg(); !errors.Is(err, ErrOverflow) {
		t.Fatal("neg")
	}
}

func TestUninitialized(t *testing.T) {
	var zero Money
	one, _ := FromUnits(1, "BRL")
	if _, err := zero.Add(one); !errors.Is(err, ErrUninitialized) {
		t.Fatal(err)
	}
	if _, err := json.Marshal(zero); err == nil {
		t.Fatal("marshal of zero value must fail")
	}
}

func TestJSON(t *testing.T) {
	m, _ := Parse("25.00", "BRL")
	b, _ := json.Marshal(m)
	if string(b) != `{"amount":"25.00","currency":"BRL"}` {
		t.Fatal(string(b))
	}
	var back Money
	if err := json.Unmarshal(b, &back); err != nil || !back.Equal(m) {
		t.Fatalf("%v %v", back, err)
	}
	if err := json.Unmarshal([]byte(`{"amount":25.0,"currency":"BRL"}`), &back); err == nil {
		t.Fatal("numeric amount must be rejected")
	}
}
