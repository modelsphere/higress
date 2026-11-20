package selection

import (
	"math/rand"
	"testing"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-backend-router/decisions"
	"github.com/stretchr/testify/require"
)

func model(t *testing.T, backends string) *decisions.Model {
	t.Helper()
	snapshot, err := decisions.Parse([]byte(`{"version":"v1","models":{"m":{"default":"a","backends":[` + backends + `]}}}`))
	require.NoError(t, err)
	return snapshot.Model("m")
}

const threeTiers = `
	{"id":"a","target":{"path":"/a"},"tier":0,"weight":70},
	{"id":"b","target":{"path":"/b"},"tier":0,"weight":30},
	{"id":"c","target":{"path":"/c"},"tier":1},
	{"id":"d","target":{"path":"/d"},"tier":7}`

func ids(tiers [][]*decisions.Backend) [][]string {
	var out [][]string
	for _, tier := range tiers {
		var names []string
		for _, b := range tier {
			names = append(names, b.ID)
		}
		out = append(out, names)
	}
	return out
}

func TestCandidatesByTier(t *testing.T) {
	m := model(t, threeTiers)
	require.Equal(t, [][]string{{"a", "b"}, {"c"}, {"d"}}, ids(Candidates(m, nil, ExcludeTier)))
	require.Equal(t, [][]string{{"c"}, {"d"}}, ids(Candidates(m, []string{"b"}, ExcludeTier)), "a tried backend removes its whole tier")
	require.Equal(t, [][]string{{"a"}, {"c"}, {"d"}}, ids(Candidates(m, []string{"b"}, ExcludeBackend)), "exclude=backend removes only that backend")
	require.Equal(t, [][]string{{"d"}}, ids(Candidates(m, []string{"a", "c"}, ExcludeTier)))
	require.Empty(t, Candidates(m, []string{"a", "c", "d"}, ExcludeTier))
	require.False(t, HasCandidate(m, []string{"a", "c", "d"}, ExcludeTier))
	require.True(t, HasCandidate(m, []string{"a", "c"}, ExcludeTier))
	require.Equal(t, [][]string{{"a", "b"}, {"c"}, {"d"}}, ids(Candidates(m, []string{"gone"}, ExcludeTier)), "an id no longer in the table excludes nothing")
}

func TestCandidatesSkipUnusable(t *testing.T) {
	m := model(t, `
		{"id":"a","target":{"path":"/a"},"tier":0,"weight":0},
		{"id":"b","target":{"path":"/b"},"tier":0,"enabled":false},
		{"id":"c","target":{"path":"/c"},"tier":1}`)
	require.Equal(t, [][]string{{"c"}}, ids(Candidates(m, nil, ExcludeTier)), "a tier whose backends are all drained is skipped")
}

func TestSelectFirstTierThenNext(t *testing.T) {
	m := model(t, threeTiers)
	picker := WeightedRandom{Random: rand.New(rand.NewSource(1))}
	for i := 0; i < 50; i++ {
		b, ok := Select(m, nil, ExcludeTier, picker, Request{})
		require.True(t, ok)
		require.Equal(t, 0, b.Tier)
	}
	b, ok := Select(m, []string{"a"}, ExcludeTier, picker, Request{})
	require.True(t, ok)
	require.Equal(t, "c", b.ID)
	_, ok = Select(m, []string{"a", "c", "d"}, ExcludeTier, picker, Request{})
	require.False(t, ok)
}

func TestWeightedRandomDistribution(t *testing.T) {
	m := model(t, `
		{"id":"a","target":{"path":"/a"},"tier":0,"weight":70},
		{"id":"b","target":{"path":"/b"},"tier":0,"weight":20},
		{"id":"c","target":{"path":"/c"},"tier":0,"weight":10}`)
	picker := WeightedRandom{Random: rand.New(rand.NewSource(42))}
	const n = 100000
	counts := map[string]float64{}
	for i := 0; i < n; i++ {
		b, _ := Select(m, nil, ExcludeTier, picker, Request{})
		counts[b.ID]++
	}
	// Chi-square with 2 degrees of freedom; 13.8 is the p = 0.001 critical value.
	expected := map[string]float64{"a": 0.7 * n, "b": 0.2 * n, "c": 0.1 * n}
	chi := 0.0
	for id, e := range expected {
		d := counts[id] - e
		chi += d * d / e
	}
	require.Less(t, chi, 13.8, "counts %v", counts)
}

func TestWeightedRandomOrderIsAPermutation(t *testing.T) {
	m := model(t, threeTiers)
	ordered := WeightedRandom{Random: rand.New(rand.NewSource(7))}.Order(Request{}, m.Backends)
	require.ElementsMatch(t, m.Backends, ordered)
}

type zeroThenHalf struct{ calls int }

func (z *zeroThenHalf) Float64() float64 {
	z.calls++
	if z.calls == 1 {
		return 0
	}
	return 0.5
}

func TestWeightedRandomRedrawsZero(t *testing.T) {
	m := model(t, `{"id":"a","target":{"path":"/a"},"tier":0}`)
	source := &zeroThenHalf{}
	ordered := WeightedRandom{Random: source}.Order(Request{}, m.Backends)
	require.Len(t, ordered, 1)
	require.Equal(t, 2, source.calls, "u = 0 would give every backend key 0; it is redrawn")
}

func TestParseExclude(t *testing.T) {
	mode, err := ParseExclude("backend")
	require.NoError(t, err)
	require.Equal(t, ExcludeBackend, mode)
	_, err = ParseExclude("tiers")
	require.Error(t, err)
}
