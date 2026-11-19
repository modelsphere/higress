package stream

import (
	"bytes"
	"sort"

	"github.com/tidwall/gjson"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-stream-guard/detector"
)

const (
	ObjectChatChunk      = "chat.completion.chunk"
	ObjectTextCompletion = "text_completion"
)

// Meta is what the adapter remembers about the stream for building the
// terminal chunk: the last seen envelope fields and the state of each choice.
type Meta struct {
	ID      string
	Object  string
	Created string // raw JSON number, copied verbatim
	Model   string
	// choices maps every choice index seen to whether it already finished.
	choices map[int]bool
}

// Unfinished returns the indexes of seen choices without a finish_reason, in
// ascending order.
func (m *Meta) Unfinished() []int {
	var out []int
	for i, done := range m.choices {
		if !done {
			out = append(out, i)
		}
	}
	sort.Ints(out)
	return out
}

// ParseResult is the outcome of parsing one raw SSE event.
type ParseResult struct {
	Events []detector.Event
	// ParseError is set when the event's data looked like JSON but did not
	// parse. The event is still forwarded unchanged.
	ParseError bool
}

// ParseEvent extracts text events from one raw SSE event and updates meta.
// It never fails: anything it does not understand yields no events.
func ParseEvent(raw []byte, meta *Meta) ParseResult {
	data, ok := eventData(raw)
	if !ok {
		return ParseResult{}
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return ParseResult{} // [DONE] or non-JSON payloads
	}
	if !gjson.ValidBytes(data) {
		return ParseResult{ParseError: true}
	}
	root := gjson.ParseBytes(data)
	choices := root.Get("choices")
	if !choices.IsArray() {
		return ParseResult{} // e.g. {"error": ...}
	}
	if v := root.Get("id"); v.Type == gjson.String {
		meta.ID = v.String()
	}
	if v := root.Get("object"); v.Type == gjson.String {
		meta.Object = v.String()
	}
	if v := root.Get("created"); v.Type == gjson.Number {
		meta.Created = v.Raw
	}
	if v := root.Get("model"); v.Type == gjson.String {
		meta.Model = v.String()
	}
	if meta.choices == nil {
		meta.choices = map[int]bool{}
	}
	completion := meta.Object == ObjectTextCompletion

	var res ParseResult
	for pos, c := range choices.Array() {
		idx := pos
		if v := c.Get("index"); v.Type == gjson.Number {
			idx = int(v.Int())
		}
		if idx < 0 {
			continue
		}
		if _, seen := meta.choices[idx]; !seen {
			meta.choices[idx] = false
		}
		if fr := c.Get("finish_reason"); fr.Exists() && fr.Type != gjson.Null && fr.String() != "" {
			meta.choices[idx] = true
		}
		if completion {
			if t := c.Get("text").String(); t != "" {
				res.Events = append(res.Events, detector.Event{Choice: idx, Field: detector.FieldContent, Text: t})
			}
			continue
		}
		delta := c.Get("delta")
		reasoning := delta.Get("reasoning").String()
		if reasoning == "" {
			reasoning = delta.Get("reasoning_content").String()
		}
		if reasoning != "" {
			res.Events = append(res.Events, detector.Event{Choice: idx, Field: detector.FieldReasoning, Text: reasoning})
		}
		if t := delta.Get("content").String(); t != "" {
			res.Events = append(res.Events, detector.Event{Choice: idx, Field: detector.FieldContent, Text: t})
		}
	}
	return res
}

// eventData returns the joined data field of a raw SSE event (multiple data
// lines joined with '\n', per the SSE spec). ok is false if there is none.
func eventData(raw []byte) (data []byte, ok bool) {
	// Normalize line endings on a copy; raw is forwarded unchanged.
	norm := bytes.ReplaceAll(raw, []byte("\r\n"), []byte("\n"))
	norm = bytes.ReplaceAll(norm, []byte("\r"), []byte("\n"))
	var parts [][]byte
	for _, line := range bytes.Split(norm, []byte("\n")) {
		v, found := bytes.CutPrefix(line, []byte("data:"))
		if !found {
			continue
		}
		v = bytes.TrimPrefix(v, []byte(" "))
		parts = append(parts, v)
	}
	if len(parts) == 0 {
		return nil, false
	}
	if len(parts) == 1 {
		return parts[0], true
	}
	return bytes.Join(parts, []byte("\n")), true
}
