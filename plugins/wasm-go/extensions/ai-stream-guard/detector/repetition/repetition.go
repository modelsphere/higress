// Package repetition detects a choice whose stream of text chunks keeps
// repeating a group of at most MaxPeriod chunks.
//
// For one choice, let x_0..x_n be the values (field, text) of the text chunks
// seen so far. The detector fires at x_n when, for some period p <= MaxPeriod,
// the last MinRepeatedChunks values are periodic with period p
// (x_i == x_{i-p} for every i in the tail except the first p). It reports the
// smallest such p.
package repetition

import (
	"fmt"
	"strings"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-stream-guard/detector"
)

const (
	// MaxPeriodLimit caps max_period; it bounds per-chunk cost and memory.
	MaxPeriodLimit = 128
	// DefaultMaxPeriod matches LiteLLM's single-chunk check.
	DefaultMaxPeriod = 1
	// DefaultMinRepeatedChunks is the default size of the repeating tail.
	DefaultMinRepeatedChunks = 512

	// prefixBytes is how much of each chunk's text is kept for the log sample.
	prefixBytes = 64
	// SampleBytes caps the sample reported with a verdict.
	SampleBytes = 256
)

// Config is the validated configuration of one repetition detector.
type Config struct {
	MaxPeriod         int
	MinRepeatedChunks int
	Fields            detector.FieldSet
}

// Validate checks the invariants documented in the design (§10.2).
func (c Config) Validate() error {
	if c.MaxPeriod < 1 || c.MaxPeriod > MaxPeriodLimit {
		return fmt.Errorf("max_period must be in 1..%d, got %d", MaxPeriodLimit, c.MaxPeriod)
	}
	if c.MinRepeatedChunks < 2*c.MaxPeriod {
		return fmt.Errorf("min_repeated_chunks must be >= 2 * max_period (%d), got %d", 2*c.MaxPeriod, c.MinRepeatedChunks)
	}
	if c.Fields == 0 {
		return fmt.Errorf("fields must not be empty")
	}
	return nil
}

// Detector is the repetition detector. It is immutable and shared by streams.
type Detector struct {
	name   string
	action detector.Action
	cfg    Config
}

func New(name string, action detector.Action, cfg Config) (*Detector, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Detector{name: name, action: action, cfg: cfg}, nil
}

func (d *Detector) Name() string            { return d.name }
func (d *Detector) Action() detector.Action { return d.action }
func (d *Detector) Config() Config          { return d.cfg }

func (d *Detector) NewStream() detector.StreamState {
	return &streamState{d: d, choices: map[int]*choiceState{}}
}

// value is the comparable fingerprint of one text chunk. Equality compares
// field, length and a 64-bit FNV-1a hash, so memory stays fixed regardless of
// chunk size; prefix is kept only for the log sample.
type value struct {
	field  detector.Field
	n      int
	h      uint64
	prefix string
}

func (v value) equal(o value) bool { return v.field == o.field && v.n == o.n && v.h == o.h }

func newValue(f detector.Field, text string) value {
	p := text
	if len(p) > prefixBytes {
		p = p[:prefixBytes]
	}
	return value{field: f, n: len(text), h: fnv64a(text), prefix: strings.Clone(p)}
}

func fnv64a(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

type streamState struct {
	d       *Detector
	choices map[int]*choiceState
}

// choiceState holds the last MaxPeriod values in a ring and, for each period
// p, run[p] = the number of most recent consecutive values with x_i == x_{i-p}.
type choiceState struct {
	ring []value
	run  []int // index p in 1..MaxPeriod; run[0] unused
	seen int
}

func (s *streamState) OnEvent(ev *detector.Event) *detector.Verdict {
	cfg := s.d.cfg
	if ev.Text == "" || !cfg.Fields.Has(ev.Field) {
		return nil
	}
	cs := s.choices[ev.Choice]
	if cs == nil {
		cs = &choiceState{ring: make([]value, cfg.MaxPeriod), run: make([]int, cfg.MaxPeriod+1)}
		s.choices[ev.Choice] = cs
	}
	x := newValue(ev.Field, ev.Text)
	L := cfg.MaxPeriod
	fired := 0
	maxP := min(L, cs.seen)
	for p := 1; p <= maxP; p++ {
		if x.equal(cs.ring[(cs.seen-p)%L]) {
			cs.run[p]++
		} else {
			cs.run[p] = 0
		}
		if fired == 0 && cs.run[p]+p >= cfg.MinRepeatedChunks {
			fired = p
		}
	}
	cs.ring[cs.seen%L] = x
	cs.seen++
	if fired == 0 {
		return nil
	}
	p := fired
	tail := cs.run[p] + p
	return &detector.Verdict{
		Detector: s.d.name,
		Action:   s.d.action,
		Choice:   ev.Choice,
		Reason:   "repetition",
		Attrs: []detector.Attr{
			{Key: "period", Value: p},
			{Key: "repeats", Value: tail / p},
			{Key: "tail_chunks", Value: tail},
			{Key: "chunks_seen", Value: cs.seen},
			{Key: "sample", Value: cs.sample(p, L)},
		},
	}
}

// sample joins the stored prefixes of the last p values (the repeating block,
// in stream order), capped at SampleBytes and made valid UTF-8.
func (cs *choiceState) sample(p, L int) string {
	var b strings.Builder
	for i := cs.seen - p; i < cs.seen; i++ {
		b.WriteString(cs.ring[i%L].prefix)
		if b.Len() >= SampleBytes {
			break
		}
	}
	s := b.String()
	if len(s) > SampleBytes {
		s = s[:SampleBytes]
	}
	return strings.ToValidUTF8(s, "�")
}
