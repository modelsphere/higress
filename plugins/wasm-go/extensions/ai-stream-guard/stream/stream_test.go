package stream

import (
	"bytes"
	"encoding/json"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-stream-guard/detector"
)

// ---- framer ----

func feedAll(f *Framer, chunks ...string) (events []string) {
	for _, c := range chunks {
		evs, _ := f.Feed([]byte(c))
		for _, e := range evs {
			events = append(events, string(e))
		}
	}
	return events
}

func TestFramerReturnsRawEvents(t *testing.T) {
	f := NewFramer()
	got := feedAll(f, "data: a\r\n\r\ndata: b\n\n: ping\n\ndata: c")
	want := []string{"data: a\r\n\r\n", "data: b\n\n", ": ping\n\n"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %q, want %q", got, want)
	}
	if string(f.Take()) != "data: c" || f.Pending() != 0 {
		t.Fatal("incomplete tail not retained / not cleared by Take")
	}
}

func TestFramerEveryByteSplit(t *testing.T) {
	stream := "data: {\"x\":1}\r\n\r\ndata: y\n\ndata: z\r\rdata: w\n\r\n"
	var chunks []string
	for i := 0; i < len(stream); i++ {
		chunks = append(chunks, stream[i:i+1])
	}
	f := NewFramer()
	got := strings.Join(feedAll(f, chunks...), "")
	if got != stream {
		t.Fatalf("byte-split reassembly mismatch:\n got %q\nwant %q", got, stream)
	}
	// Every split point of a 2-callback delivery yields the same events.
	whole := feedAll(NewFramer(), stream)
	for i := 0; i <= len(stream); i++ {
		parts := feedAll(NewFramer(), stream[:i], stream[i:])
		if strings.Join(parts, "|") != strings.Join(whole, "|") {
			t.Fatalf("split at %d: got %q want %q", i, parts, whole)
		}
	}
}

func TestFramerPendingCR(t *testing.T) {
	f := NewFramer()
	if evs := feedAll(f, "data: a\r"); len(evs) != 0 {
		t.Fatalf("pending CR emitted %q", evs)
	}
	if evs := feedAll(f, "\n\r\n"); len(evs) != 1 || evs[0] != "data: a\r\n\r\n" {
		t.Fatalf("got %q", evs)
	}
}

func TestFramerOverflow(t *testing.T) {
	f := &Framer{max: 8}
	if _, of := f.Feed([]byte("data: 12")); of {
		t.Fatal("overflow at exactly the cap")
	}
	evs, of := f.Feed([]byte("3"))
	if !of || len(evs) != 0 {
		t.Fatalf("want overflow at cap+1, got overflow=%v events=%q", of, evs)
	}
	if string(f.Take()) != "data: 123" {
		t.Fatal("overflowed tail must be returned intact")
	}
}

func TestFramerFragmentedLargeEventIsLinear(t *testing.T) {
	// A ~1 MiB event delivered in 64-byte pieces: rescanning the whole retained
	// tail on every callback would scan ~8 GiB; resuming must stay fast.
	payload := "data: " + strings.Repeat("x", MaxIncompleteEventBytes-16)
	f := NewFramer()
	start := time.Now()
	for i := 0; i < len(payload); i += 64 {
		end := min(i+64, len(payload))
		if evs, of := f.Feed([]byte(payload[i:end])); len(evs) != 0 || of {
			t.Fatalf("unexpected events/overflow at %d", i)
		}
	}
	evs, _ := f.Feed([]byte("\r\n\r\n"))
	if len(evs) != 1 || len(evs[0]) != len(payload)+4 {
		t.Fatalf("got %d events", len(evs))
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("framing took %v: retained tail is being rescanned", d)
	}
}

func TestFramerRandomSplitsMatchWholeStream(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	alphabet := []byte("ab:\r\n\r\n")
	for iter := 0; iter < 3000; iter++ {
		stream := make([]byte, rng.IntN(80))
		for i := range stream {
			stream[i] = alphabet[rng.IntN(len(alphabet))]
		}
		whole := NewFramer()
		want := feedAll(whole, string(stream))
		wantTail := string(whole.Take())

		f := NewFramer()
		var got []string
		for rest := string(stream); len(rest) > 0; {
			k := 1 + rng.IntN(min(len(rest), 7))
			got = append(got, feedAll(f, rest[:k])...)
			rest = rest[k:]
		}
		if strings.Join(got, "|") != strings.Join(want, "|") || string(f.Take()) != wantTail {
			t.Fatalf("stream %q: split framing %q differs from whole %q", stream, got, want)
		}
	}
}

func TestFramerTailDoesNotAliasChunk(t *testing.T) {
	f := NewFramer()
	chunk := []byte("data: abc")
	f.Feed(chunk)
	copy(chunk, "XXXXXXXXX")
	if string(f.Take()) != "data: abc" {
		t.Fatal("tail aliases caller chunk")
	}
}

// ---- adapter ----

func parse(t *testing.T, meta *Meta, raw string) ParseResult {
	t.Helper()
	return ParseEvent([]byte(raw), meta)
}

func TestParseChatContentAndReasoning(t *testing.T) {
	var m Meta
	r := parse(t, &m, `data: {"id":"c1","object":"chat.completion.chunk","created":17,"model":"m","choices":[{"index":0,"delta":{"reasoning":"think","content":"say"},"finish_reason":null}]}`+"\n\n")
	want := []detector.Event{
		{Choice: 0, Field: detector.FieldReasoning, Text: "think"},
		{Choice: 0, Field: detector.FieldContent, Text: "say"},
	}
	if len(r.Events) != 2 || r.Events[0] != want[0] || r.Events[1] != want[1] {
		t.Fatalf("got %+v", r.Events)
	}
	if m.ID != "c1" || m.Object != ObjectChatChunk || m.Created != "17" || m.Model != "m" {
		t.Fatalf("meta not recorded: %+v", m)
	}
}

func TestParseReasoningContentAliasAndPrecedence(t *testing.T) {
	var m Meta
	r := parse(t, &m, `data: {"choices":[{"index":0,"delta":{"reasoning_content":"old"}}]}`)
	if len(r.Events) != 1 || r.Events[0].Field != detector.FieldReasoning || r.Events[0].Text != "old" {
		t.Fatalf("reasoning_content: got %+v", r.Events)
	}
	r = parse(t, &m, `data: {"choices":[{"index":0,"delta":{"reasoning":"new","reasoning_content":"new"}}]}`)
	if len(r.Events) != 1 || r.Events[0].Text != "new" {
		t.Fatalf("both fields must yield one event, got %+v", r.Events)
	}
}

func TestParseCompletions(t *testing.T) {
	var m Meta
	r := parse(t, &m, `data: {"id":"x","object":"text_completion","choices":[{"index":1,"text":"hi","finish_reason":null}]}`)
	if len(r.Events) != 1 || r.Events[0] != (detector.Event{Choice: 1, Field: detector.FieldContent, Text: "hi"}) {
		t.Fatalf("got %+v", r.Events)
	}
}

func TestParseInvisibleEvents(t *testing.T) {
	cases := []string{
		`data: {"choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}`,
		`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"a\""}}]}}]}`,
		`data: {"choices":[],"usage":{"prompt_tokens":1}}`,
		`data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		"data: [DONE]\n\n",
		": ping\n\n",
		"event: x\nid: 1\nretry: 5\n\n",
		`data: {"error":{"message":"boom"}}`,
	}
	for _, c := range cases {
		var m Meta
		if r := parse(t, &m, c); len(r.Events) != 0 || r.ParseError {
			t.Errorf("%q: got %+v", c, r)
		}
	}
}

func TestParseMalformedJSON(t *testing.T) {
	var m Meta
	if r := parse(t, &m, `data: {"choices":[{"delta":`); !r.ParseError {
		t.Fatal("malformed JSON not flagged")
	}
	if r := parse(t, &m, `data: not json`); r.ParseError {
		t.Fatal("non-JSON payload must not be a parse error")
	}
}

func TestParseMultiLineDataAndCRLF(t *testing.T) {
	var m Meta
	r := parse(t, &m, "data: {\"choices\":[{\"index\":0,\r\ndata: \"delta\":{\"content\":\"x\"}}]}\r\n\r\n")
	if len(r.Events) != 1 || r.Events[0].Text != "x" {
		t.Fatalf("got %+v", r)
	}
}

func TestParseTracksFinishedChoices(t *testing.T) {
	var m Meta
	parse(t, &m, `data: {"choices":[{"index":0,"delta":{"content":"a"}},{"index":2,"delta":{"content":"b"}}]}`)
	parse(t, &m, `data: {"choices":[{"index":1,"delta":{"content":"c"}}]}`)
	parse(t, &m, `data: {"choices":[{"index":2,"delta":{},"finish_reason":"stop"}]}`)
	if got := m.Unfinished(); len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("unfinished = %v, want [0 1]", got)
	}
}

// ---- terminal bytes ----

func decodeEvents(t *testing.T, b []byte) []map[string]any {
	t.Helper()
	s := string(b)
	if !strings.HasSuffix(s, doneEvent) {
		t.Fatalf("missing [DONE]: %q", s)
	}
	var out []map[string]any
	for _, ev := range strings.Split(strings.TrimSuffix(s, doneEvent), "\n\n") {
		if ev == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(ev, "data: ")), &m); err != nil {
			t.Fatalf("invalid JSON %q: %v", ev, err)
		}
		out = append(out, m)
	}
	return out
}

func TestTerminalChat(t *testing.T) {
	var m Meta
	parse(t, &m, `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1763520000,"model":"qwen3","choices":[{"index":0,"delta":{"content":"a"}}]}`)
	b := TerminalBytes(&m, Cutoff{FinishReason: "content_filter"})
	want := `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1763520000,"model":"qwen3","choices":[{"index":0,"delta":{},"finish_reason":"content_filter"}]}` + "\n\n" + doneEvent
	if string(b) != want {
		t.Fatalf("got  %q\nwant %q", b, want)
	}
}

func TestTerminalCompletionsWithMessageAndChoices(t *testing.T) {
	var m Meta
	parse(t, &m, `data: {"id":"cmpl-1","object":"text_completion","created":5,"model":"m","choices":[{"index":0,"text":"a"},{"index":1,"text":"b"}]}`)
	parse(t, &m, `data: {"object":"text_completion","choices":[{"index":1,"text":"","finish_reason":"stop"}]}`)
	evs := decodeEvents(t, TerminalBytes(&m, Cutoff{FinishReason: "length", Message: "[stopped \"x\"]"}))
	if len(evs) != 2 {
		t.Fatalf("want message + terminal for the one unfinished choice, got %d: %v", len(evs), evs)
	}
	msg := evs[0]["choices"].([]any)[0].(map[string]any)
	if msg["text"] != "[stopped \"x\"]" || msg["finish_reason"] != nil || msg["index"] != float64(0) {
		t.Fatalf("message chunk: %v", msg)
	}
	term := evs[1]["choices"].([]any)[0].(map[string]any)
	if term["finish_reason"] != "length" || term["text"] != "" || evs[1]["object"] != ObjectTextCompletion {
		t.Fatalf("terminal chunk: %v", evs[1])
	}
}

func TestTerminalAllFinishedOnlyDone(t *testing.T) {
	var m Meta
	parse(t, &m, `data: {"choices":[{"index":0,"delta":{"content":"a"},"finish_reason":"stop"}]}`)
	if got := string(TerminalBytes(&m, Cutoff{FinishReason: "content_filter", Message: "m"})); got != doneEvent {
		t.Fatalf("got %q, want only [DONE]", got)
	}
}

func TestTerminalEscapesStrings(t *testing.T) {
	m := Meta{ID: `a"b`, Model: "m\n"}
	m.choices = map[int]bool{0: false}
	evs := decodeEvents(t, TerminalBytes(&m, Cutoff{FinishReason: "stop", Message: "</script> "}))
	if evs[0]["id"] != `a"b` || evs[0]["model"] != "m\n" {
		t.Fatalf("escaping broken: %v", evs[0])
	}
	if !bytes.Contains(TerminalBytes(&m, Cutoff{FinishReason: "stop"}), []byte(`"created":0`)) {
		t.Fatal("missing created must default to 0")
	}
}
