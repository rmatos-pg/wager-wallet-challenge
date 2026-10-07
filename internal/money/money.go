// Package money is an immutable monetary value: int64 minor units (cents)
// plus an ISO 4217 currency. No floating point anywhere.
//
// Range with 2 decimals: about ±92 quadrillion. Every operation that could
// wrap around returns ErrOverflow instead.
package money

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
)

var (
	ErrInvalidAmount    = errors.New("invalid amount")
	ErrInvalidScale     = errors.New("amount must have exactly 2 decimal places")
	ErrNegativeAmount   = errors.New("amount must not be negative")
	ErrInvalidCurrency  = errors.New("invalid currency")
	ErrCurrencyMismatch = errors.New("currency mismatch")
	ErrOverflow         = errors.New("amount overflow")
	ErrUninitialized    = errors.New("money not initialized")
)

// Money's zero value has no currency and is rejected by every operation.
type Money struct {
	units    int64
	currency string
}

// ValidCurrency accepts three upper-case letters (ISO 4217 shape).
func ValidCurrency(c string) bool {
	if len(c) != 3 {
		return false
	}
	for i := 0; i < 3; i++ {
		if c[i] < 'A' || c[i] > 'Z' {
			return false
		}
	}
	return true
}

func Zero(currency string) (Money, error) { return FromUnits(0, currency) }

// FromUnits builds Money from cents, e.g. when reading from the database.
func FromUnits(units int64, currency string) (Money, error) {
	if !ValidCurrency(currency) {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, currency)
	}
	return Money{units: units, currency: currency}, nil
}

// Parse accepts "-12.30", "0.00", "25.00". Used for internal values.
func Parse(amount, currency string) (Money, error) { return parse(amount, currency, true) }

// ParseExternal is for provider input: same format, but no sign at all.
func ParseExternal(amount, currency string) (Money, error) { return parse(amount, currency, false) }

// parse only accepts the canonical form (0|[1-9][0-9]*).[0-9]{2}, so there is
// nothing to normalize before hashing: NaN, Infinity, "1e3", "+1.00", "01.00",
// " 1.00" and "1.5" are all rejected, never rounded.
func parse(s, currency string, allowNegative bool) (Money, error) {
	if !ValidCurrency(currency) {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, currency)
	}
	if s == "" {
		return Money{}, fmt.Errorf("%w: empty", ErrInvalidAmount)
	}
	neg := s[0] == '-'
	if neg {
		if !allowNegative {
			return Money{}, fmt.Errorf("%w: %q", ErrNegativeAmount, s)
		}
		s = s[1:]
	}

	dot := -1
	for i := range len(s) {
		if s[i] == '.' {
			dot = i
			break
		}
	}
	if dot < 0 {
		if digits(s) {
			return Money{}, fmt.Errorf("%w: %q", ErrInvalidScale, s)
		}
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	whole, frac := s[:dot], s[dot+1:]
	if !digits(whole) || !digits(frac) {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	if len(frac) != 2 {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidScale, s)
	}
	if len(whole) > 1 && whole[0] == '0' {
		return Money{}, fmt.Errorf("%w: leading zero in %q", ErrInvalidAmount, s)
	}

	limit := uint64(math.MaxInt64)
	if neg {
		limit++
	}
	var v uint64
	for _, r := range whole + frac {
		d := uint64(r - '0')
		if v > (limit-d)/10 {
			return Money{}, fmt.Errorf("%w: %q", ErrOverflow, s)
		}
		v = v*10 + d
	}
	if neg {
		if v == 0 {
			return Money{}, fmt.Errorf("%w: negative zero", ErrInvalidAmount)
		}
		return Money{units: int64(-v), currency: currency}, nil
	}
	return Money{units: int64(v), currency: currency}, nil
}

func digits(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func (m Money) Units() int64       { return m.units }
func (m Money) Currency() string   { return m.currency }
func (m Money) Valid() bool        { return m.currency != "" }
func (m Money) IsZero() bool       { return m.units == 0 }
func (m Money) IsPositive() bool   { return m.units > 0 }
func (m Money) IsNegative() bool   { return m.units < 0 }
func (m Money) Equal(o Money) bool { return m == o }
func (m Money) String() string     { return m.Amount() + " " + m.currency }

// Amount renders two decimals, e.g. "975.00" or "-0.05".
func (m Money) Amount() string {
	u, sign := uint64(m.units), ""
	if m.units < 0 {
		u, sign = uint64(-(m.units+1))+1, "-"
	}
	s := strconv.FormatUint(u, 10)
	for len(s) < 3 {
		s = "0" + s
	}
	return sign + s[:len(s)-2] + "." + s[len(s)-2:]
}

func (m Money) check(o Money) error {
	if !m.Valid() || !o.Valid() {
		return ErrUninitialized
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

func (m Money) Add(o Money) (Money, error) {
	if err := m.check(o); err != nil {
		return Money{}, err
	}
	if (o.units > 0 && m.units > math.MaxInt64-o.units) || (o.units < 0 && m.units < math.MinInt64-o.units) {
		return Money{}, ErrOverflow
	}
	return Money{units: m.units + o.units, currency: m.currency}, nil
}

func (m Money) Sub(o Money) (Money, error) {
	if err := m.check(o); err != nil {
		return Money{}, err
	}
	if (o.units < 0 && m.units > math.MaxInt64+o.units) || (o.units > 0 && m.units < math.MinInt64+o.units) {
		return Money{}, ErrOverflow
	}
	return Money{units: m.units - o.units, currency: m.currency}, nil
}

func (m Money) Neg() (Money, error) {
	if !m.Valid() {
		return Money{}, ErrUninitialized
	}
	if m.units == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{units: -m.units, currency: m.currency}, nil
}

// Cmp returns -1, 0 or 1.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.check(o); err != nil {
		return 0, err
	}
	switch {
	case m.units < o.units:
		return -1, nil
	case m.units > o.units:
		return 1, nil
	}
	return 0, nil
}

// JSON is the wire shape {"amount":"25.00","currency":"BRL"}.
type JSON struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func (m Money) MarshalJSON() ([]byte, error) {
	if !m.Valid() {
		return nil, ErrUninitialized
	}
	return json.Marshal(JSON{Amount: m.Amount(), Currency: m.currency})
}

// UnmarshalJSON accepts signed values; external input goes through
// ParseExternal at the boundary instead.
func (m *Money) UnmarshalJSON(b []byte) error {
	var j JSON
	if err := json.Unmarshal(b, &j); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAmount, err)
	}
	v, err := Parse(j.Amount, j.Currency)
	if err != nil {
		return err
	}
	*m = v
	return nil
}
