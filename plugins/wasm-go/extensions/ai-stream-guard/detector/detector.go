// Package detector defines the contract between the ai-stream-guard framework
// and its detectors. Detectors see normalized events only (never SSE bytes or
// JSON), keep bounded per-stream state, and report verdicts; the framework owns
// every stream action.
package detector

// Field identifies which text field of a choice an Event came from.
type Field uint8

const (
	FieldContent Field = iota + 1
	FieldReasoning
)

func (f Field) String() string {
	switch f {
	case FieldContent:
		return "content"
	case FieldReasoning:
		return "reasoning"
	default:
		return "unknown"
	}
}

// Event is one non-empty text delta of one choice.
type Event struct {
	Choice int
	Field  Field
	Text   string
}

// Action is what the framework does when a detector fires.
type Action string

// ActionCut ends the client stream with a terminal chunk and drops the rest of
// the upstream response. It is the only action in v1.
const ActionCut Action = "cut"

// Attr is one detector-specific attribute reported with a verdict. Attributes
// are ordered so logs are deterministic.
type Attr struct {
	Key   string
	Value any
}

// Verdict is a detector's decision that the stream is anomalous.
type Verdict struct {
	Detector string
	Action   Action
	Choice   int
	Reason   string
	Attrs    []Attr
}

// Detector is built once from config and shared by all streams.
type Detector interface {
	Name() string
	Action() Action
	NewStream() StreamState
}

// StreamState is the per-stream state of one detector. Implementations must
// keep memory bounded per stream and cost bounded per event.
type StreamState interface {
	// OnEvent returns nil to let the stream continue.
	OnEvent(ev *Event) *Verdict
}

// FieldSet is a small set of Fields.
type FieldSet uint8

func NewFieldSet(fields ...Field) FieldSet {
	var s FieldSet
	for _, f := range fields {
		s |= 1 << f
	}
	return s
}

func (s FieldSet) Has(f Field) bool { return s&(1<<f) != 0 }
