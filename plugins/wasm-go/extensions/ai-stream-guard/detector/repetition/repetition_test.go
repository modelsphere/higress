package repetition

import (
	"strings"
	"testing"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-stream-guard/detector"
)

var bothFields = detector.NewFieldSet(detector.FieldContent, detector.FieldReasoning)

func mustNew(t *testing.T, L, N int) *Detector {
	t.Helper()
	d, err := New("rep", detector.ActionCut, Config{MaxPeriod: L, MinRepeatedChunks: N, Fields: bothFields})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func content(choice int, text string) *detector.Event {
	return &detector.Event{Choice: choice, Field: detector.FieldContent, Text: text}
}

// feed sends texts as content of choice 0 and returns the 1-based position of
// the first verdict (0 if none) and the verdict.
func feed(st detector.StreamState, texts ...string) (int, *detector.Verdict) {
	for i, s := range texts {
		if v := st.OnEvent(content(0, s)); v != nil {
			return i + 1, v
		}
	}
	return 0, nil
}

func repeat(block []string, chunks int) []string {
	out := make([]string, 0, chunks)
	for len(out) < chunks {
		out = append(out, block[len(out)%len(block)])
	}
	return out
}

func attr(v *detector.Verdict, key string) any {
	for _, a := range v.Attrs {
		if a.Key == key {
			return a.Value
		}
	}
	return nil
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{"defaults", Config{1, 512, bothFields}, true},
		{"min period", Config{1, 2, bothFields}, true},
		{"max period", Config{128, 256, bothFields}, true},
		{"period 0", Config{0, 512, bothFields}, false},
		{"period 129", Config{129, 1024, bothFields}, false},
		{"N exactly 2L", Config{16, 32, bothFields}, true},
		{"N below 2L", Config{16, 31, bothFields}, false},
		{"no fields", Config{1, 512, 0}, false},
	}
	for _, c := range cases {
		err := c.cfg.Validate()
		if (err == nil) != c.ok {
			t.Errorf("%s: Validate() err=%v, want ok=%v", c.name, err, c.ok)
		}
	}
}

func TestPeriodOneFiresAtExactlyN(t *testing.T) {
	const N = 20
	pos, v := feed(mustNew(t, 1, N).NewStream(), repeat([]string{"a"}, N)...)
	if pos != N {
		t.Fatalf("fired at %d, want %d", pos, N)
	}
	if attr(v, "period") != 1 || attr(v, "tail_chunks") != N || attr(v, "repeats") != N {
		t.Fatalf("unexpected attrs: %+v", v.Attrs)
	}
	// One short: must not fire.
	if pos, _ := feed(mustNew(t, 1, N).NewStream(), repeat([]string{"a"}, N-1)...); pos != 0 {
		t.Fatalf("fired at %d with only N-1 chunks", pos)
	}
}

func TestPeriodsUpToL(t *testing.T) {
	const L, N = 8, 40
	for p := 1; p <= L; p++ {
		block := make([]string, p)
		for i := range block {
			block[i] = string(rune('a' + i))
		}
		pos, v := feed(mustNew(t, L, N).NewStream(), repeat(block, N)...)
		if pos != N {
			t.Fatalf("p=%d: fired at %d, want %d", p, pos, N)
		}
		if attr(v, "period") != p {
			t.Fatalf("p=%d: reported period %v", p, attr(v, "period"))
		}
		if pos, _ := feed(mustNew(t, L, N).NewStream(), repeat(block, N-1)...); pos != 0 {
			t.Fatalf("p=%d: fired at %d with N-1 chunks", p, pos)
		}
	}
}

func TestPeriodAboveLNeverFires(t *testing.T) {
	const L = 4
	block := []string{"a", "b", "c", "d", "e"} // period 5 > L
	if pos, _ := feed(mustNew(t, L, 8).NewStream(), repeat(block, 1000)...); pos != 0 {
		t.Fatalf("period %d fired with L=%d at %d", len(block), L, pos)
	}
	// Control: the same input fires once L covers the period.
	if pos, _ := feed(mustNew(t, 5, 10).NewStream(), repeat(block, 1000)...); pos != 10 {
		t.Fatalf("control: fired at %d, want 10", pos)
	}
}

func TestSmallestPeriodReported(t *testing.T) {
	// "a" repeated is periodic at every p; p=1 must win.
	_, v := feed(mustNew(t, 4, 16).NewStream(), repeat([]string{"a"}, 16)...)
	if v == nil || attr(v, "period") != 1 {
		t.Fatalf("got %+v, want period 1", v)
	}
}

func TestDifferentValueResetsRun(t *testing.T) {
	const N = 10
	in := append(repeat([]string{"a"}, N-1), "b")
	in = append(in, repeat([]string{"a"}, N-1)...)
	if pos, _ := feed(mustNew(t, 1, N).NewStream(), in...); pos != 0 {
		t.Fatalf("fired at %d despite reset", pos)
	}
	in = append(in, "a")
	if pos, _ := feed(mustNew(t, 1, N).NewStream(), in...); pos != len(in) {
		t.Fatalf("fired at %d, want %d after a fresh full run", pos, len(in))
	}
}

func TestInvisibleEventsDoNotBreakRun(t *testing.T) {
	d, err := New("rep", detector.ActionCut, Config{MaxPeriod: 1, MinRepeatedChunks: 4, Fields: detector.NewFieldSet(detector.FieldContent)})
	if err != nil {
		t.Fatal(err)
	}
	st := d.NewStream()
	for i := 0; i < 3; i++ {
		if v := st.OnEvent(content(0, "x")); v != nil {
			t.Fatalf("fired early at %d", i)
		}
		// excluded field and empty text are invisible
		if v := st.OnEvent(&detector.Event{Choice: 0, Field: detector.FieldReasoning, Text: "y"}); v != nil {
			t.Fatal("fired on excluded field")
		}
		if v := st.OnEvent(content(0, "")); v != nil {
			t.Fatal("fired on empty text")
		}
	}
	if v := st.OnEvent(content(0, "x")); v == nil {
		t.Fatal("run broken by invisible events")
	}
}

func TestFieldIsPartOfValue(t *testing.T) {
	st := mustNew(t, 1, 4).NewStream()
	for i := 0; i < 8; i++ {
		f := detector.FieldContent
		if i%2 == 1 {
			f = detector.FieldReasoning
		}
		if v := st.OnEvent(&detector.Event{Field: f, Text: "x"}); v != nil {
			t.Fatalf("content/reasoning with the same text matched at %d", i)
		}
	}
	// Control: with L=2 the alternation is a period-2 loop.
	st = mustNew(t, 2, 4).NewStream()
	var got *detector.Verdict
	for i := 0; i < 4 && got == nil; i++ {
		f := detector.FieldContent
		if i%2 == 1 {
			f = detector.FieldReasoning
		}
		got = st.OnEvent(&detector.Event{Field: f, Text: "x"})
	}
	if got == nil || attr(got, "period") != 2 {
		t.Fatalf("control: got %+v", got)
	}
}

func TestWhitespaceAndSingleCJKCharFire(t *testing.T) {
	for _, s := range []string{"\n", " ", "哈"} {
		if pos, _ := feed(mustNew(t, 1, 32).NewStream(), repeat([]string{s}, 32)...); pos != 32 {
			t.Errorf("%q: fired at %d, want 32", s, pos)
		}
	}
}

func TestChoicesAreIndependent(t *testing.T) {
	st := mustNew(t, 1, 4).NewStream()
	// Interleave two choices with different texts: each choice repeats on its own.
	var v *detector.Verdict
	for i := 0; i < 4 && v == nil; i++ {
		if v = st.OnEvent(content(0, "a")); v != nil {
			break
		}
		v = st.OnEvent(content(1, "b"))
	}
	if v == nil || v.Choice != 0 {
		t.Fatalf("got %+v, want choice 0 to fire first", v)
	}
	// Alternating texts on one choice never fire at L=1.
	st = mustNew(t, 1, 4).NewStream()
	for i := 0; i < 50; i++ {
		if v := st.OnEvent(content(0, []string{"a", "b"}[i%2])); v != nil {
			t.Fatal("alternating values fired at L=1")
		}
	}
}

func TestLongChunksCompareByFullContent(t *testing.T) {
	// Same 64-byte prefix, different tails: must not match.
	base := strings.Repeat("p", 100)
	in := make([]string, 40)
	for i := range in {
		in[i] = base + string(rune('a'+i%26))
	}
	if pos, _ := feed(mustNew(t, 1, 4).NewStream(), in...); pos != 0 {
		t.Fatalf("chunks differing after the prefix matched at %d", pos)
	}
}

func TestSample(t *testing.T) {
	_, v := feed(mustNew(t, 3, 9).NewStream(), repeat([]string{"ab", "cd", "ef"}, 9)...)
	if got := attr(v, "sample"); got != "abcdef" {
		t.Fatalf("sample = %q, want abcdef", got)
	}
	long := strings.Repeat("z", 100)
	_, v = feed(mustNew(t, 8, 16).NewStream(), repeat([]string{long, long + "1", long + "2", long + "3", long + "4"}, 16)...)
	if s := attr(v, "sample").(string); len(s) != SampleBytes {
		t.Fatalf("sample len %d, want %d", len(s), SampleBytes)
	}
	// A prefix cut inside a multi-byte rune is repaired.
	cjk := strings.Repeat("哈", 30) // 90 bytes; 64-byte prefix splits a rune
	_, v = feed(mustNew(t, 1, 4).NewStream(), repeat([]string{cjk}, 4)...)
	if s := attr(v, "sample").(string); !strings.HasSuffix(s, "�") || strings.Count(s, "哈") != 21 {
		t.Fatalf("sample not repaired: %q", s)
	}
}

func BenchmarkOnEventMaxPeriod(b *testing.B) {
	d, _ := New("rep", detector.ActionCut, Config{MaxPeriod: MaxPeriodLimit, MinRepeatedChunks: 1 << 30, Fields: bothFields})
	st := d.NewStream()
	words := []string{" the", " quick", " brown", " fox", " jumps", " over", " lazy", " dog", "."}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st.OnEvent(&detector.Event{Field: detector.FieldContent, Text: words[i%len(words)] + string(rune('a'+i%7))})
	}
}
