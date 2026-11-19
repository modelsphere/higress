package stream

import (
	"bytes"
	"encoding/json"
	"strconv"
)

// Finish reasons accepted by cutoff.finish_reason.
var FinishReasons = []string{"content_filter", "length", "stop"}

// Cutoff configures what the client receives when the stream is cut.
type Cutoff struct {
	FinishReason string
	Message      string
}

const doneEvent = "data: [DONE]\n\n"

// TerminalBytes builds the bytes appended after the last forwarded event on a
// cut: an optional message chunk and a terminal chunk per unfinished choice,
// then [DONE]. Envelope fields are copied from meta. A choice that already
// sent its finish_reason (e.g. the triggering chunk carried both the last
// token and finish_reason) gets nothing: a second finish would be invalid.
func TerminalBytes(meta *Meta, c Cutoff) []byte {
	choices := meta.Unfinished()
	var buf bytes.Buffer
	if c.Message != "" {
		for _, idx := range choices {
			writeChunk(&buf, meta, idx, c.Message, "")
		}
	}
	for _, idx := range choices {
		writeChunk(&buf, meta, idx, "", c.FinishReason)
	}
	buf.WriteString(doneEvent)
	return buf.Bytes()
}

// writeChunk writes one SSE event. A non-empty finishReason makes it a
// terminal chunk; otherwise it carries text with finish_reason null.
func writeChunk(buf *bytes.Buffer, meta *Meta, idx int, text, finishReason string) {
	object := meta.Object
	if object != ObjectTextCompletion {
		object = ObjectChatChunk
	}
	created := meta.Created
	if created == "" {
		created = "0"
	}
	buf.WriteString(`data: {"id":`)
	writeJSONString(buf, meta.ID)
	buf.WriteString(`,"object":`)
	writeJSONString(buf, object)
	buf.WriteString(`,"created":`)
	buf.WriteString(created)
	buf.WriteString(`,"model":`)
	writeJSONString(buf, meta.Model)
	buf.WriteString(`,"choices":[{"index":`)
	buf.WriteString(strconv.Itoa(idx))
	if object == ObjectTextCompletion {
		buf.WriteString(`,"text":`)
		writeJSONString(buf, text)
	} else if text == "" {
		buf.WriteString(`,"delta":{}`)
	} else {
		buf.WriteString(`,"delta":{"content":`)
		writeJSONString(buf, text)
		buf.WriteString(`}`)
	}
	buf.WriteString(`,"finish_reason":`)
	if finishReason == "" {
		buf.WriteString(`null`)
	} else {
		writeJSONString(buf, finishReason)
	}
	buf.WriteString("}]}\n\n")
}

func writeJSONString(buf *bytes.Buffer, s string) {
	b, _ := json.Marshal(s) // a string always marshals
	buf.Write(b)
}
