// Copyright (c) 2025 Alibaba Group Holding Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stream

// Derived from ai-statistics/sse_framer.go. Differences:
//   - events are returned RAW (delimiter included, no normalization), because
//     ai-stream-guard forwards them to the client unchanged;
//   - there is no RESYNC state: on overflow the caller switches the stream to
//     pass-through and takes the retained tail back with Take, so no byte is
//     ever dropped.

// MaxIncompleteEventBytes caps the bytes retained for one incomplete event.
const MaxIncompleteEventBytes = 1 * 1024 * 1024 // 1 MiB

// Framer splits a byte stream into complete SSE events. NOT safe for
// concurrent use; one instance lives for one response.
type Framer struct {
	tail []byte // incomplete-event bytes; never aliases a caller chunk
	max  int
}

func NewFramer() *Framer { return &Framer{max: MaxIncompleteEventBytes} }

// Feed scans tail‖chunk and returns the complete events found, each
// including its terminating delimiter. The incomplete suffix is retained. If
// the suffix exceeds the cap, overflow is true and the suffix is still
// retained; the caller is expected to Take it and stop framing.
func (f *Framer) Feed(chunk []byte) (events [][]byte, overflow bool) {
	w := byteWindow{a: f.tail, b: chunk}
	ends := w.scanEventEnds(resumeOffset(f.tail))
	if len(ends) == 0 {
		// Still inside one event: grow the tail in place (amortized O(len(chunk)));
		// append copies chunk, so the tail never aliases the caller's buffer.
		f.tail = append(f.tail, chunk...)
		return nil, len(f.tail) > f.max
	}
	n := w.len()
	prev := 0
	for _, end := range ends {
		if prev >= len(f.tail) {
			events = append(events, chunk[prev-len(f.tail):end-len(f.tail)])
		} else {
			events = append(events, w.sliceRange(prev, end))
		}
		prev = end
	}
	if prev == n {
		f.tail = nil
	} else {
		f.tail = w.sliceRange(prev, n)
	}
	return events, len(f.tail) > f.max
}

// Take returns the retained incomplete bytes and clears them.
func (f *Framer) Take() []byte {
	t := f.tail
	f.tail = nil
	return t
}

// Pending reports how many incomplete bytes are retained.
func (f *Framer) Pending() int { return len(f.tail) }

// byteWindow is a read-only view over the virtual concatenation a‖b.
type byteWindow struct {
	a []byte
	b []byte
}

func (w byteWindow) len() int { return len(w.a) + len(w.b) }

func (w byteWindow) at(i int) byte {
	if i < len(w.a) {
		return w.a[i]
	}
	return w.b[i-len(w.a)]
}

// lineEndingEnd classifies the line ending at offset i ('\r' or '\n') and
// returns the offset just past it: LF -> i+1, CRLF -> i+2, bare CR -> i+1.
// pending is true for a '\r' at the very end of the window, whose meaning
// depends on the next byte.
func (w byteWindow) lineEndingEnd(i int) (end int, pending bool) {
	if w.at(i) == '\n' {
		return i + 1, false
	}
	if i+1 == w.len() {
		return i, true
	}
	if w.at(i+1) == '\n' {
		return i + 2, false
	}
	return i + 1, false
}

// resumeOffset is where scanning tail‖chunk may start. The tail never holds a
// complete delimiter, so a full scan from 0 would reach the start of the
// tail's trailing CR/LF run (preceded by a non-line-ending byte, or at 0) with
// identical state; starting there keeps each byte scanned O(1) times when one
// large event arrives in many small callbacks.
func resumeOffset(tail []byte) int {
	i := len(tail)
	for i > 0 && (tail[i-1] == '\r' || tail[i-1] == '\n') {
		i--
	}
	return i
}

// scanEventEnds returns the end offset (delimiter included) of each complete
// event, scanning from offset from: two consecutive line endings in any
// LF/CRLF/CR combination.
func (w byteWindow) scanEventEnds(from int) []int {
	var ends []int
	n := w.len()
	pos := from
	for pos < n {
		c := w.at(pos)
		if c != '\n' && c != '\r' {
			pos++
			continue
		}
		firstEnd, pending := w.lineEndingEnd(pos)
		if pending {
			break
		}
		if firstEnd < n {
			if c2 := w.at(firstEnd); c2 == '\n' || c2 == '\r' {
				secondEnd, pending := w.lineEndingEnd(firstEnd)
				if pending {
					break
				}
				ends = append(ends, secondEnd)
				pos = secondEnd
				continue
			}
		}
		pos = firstEnd
	}
	return ends
}

// sliceRange returns a fresh copy of w[from:to].
func (w byteWindow) sliceRange(from, to int) []byte {
	out := make([]byte, 0, to-from)
	if from >= len(w.a) {
		return append(out, w.b[from-len(w.a):to-len(w.a)]...)
	}
	if to <= len(w.a) {
		return append(out, w.a[from:to]...)
	}
	out = append(out, w.a[from:]...)
	return append(out, w.b[:to-len(w.a)]...)
}
