package decisions

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const validDoc = `{
  "version": "v1",
  "models": {
    "qwen3.5-27b": {
      "default": "ep8",
      "backends": [
        {"id": "ep8",  "target": {"path": "/qwen3.5-27b-h100-ep8"}, "tier": 0, "weight": 70},
        {"id": "pp2",  "target": {"path": "/qwen3.5-27b-h100-pp2"}, "tier": 0},
        {"id": "b300", "target": {"path": "/qwen3.5-27b-b300"}, "tier": 5, "enabled": false, "maxConcurrency": 0}
      ]
    }
  },
  "extensions": {"owner": "decision-service"}
}`

func TestParseValid(t *testing.T) {
	snapshot, err := Parse([]byte(validDoc))
	require.NoError(t, err)
	require.Equal(t, "v1", snapshot.Version)
	model := snapshot.Model("qwen3.5-27b")
	require.NotNil(t, model)
	require.Equal(t, "ep8", model.Default.ID)
	require.Equal(t, []int{0, 5}, model.Tiers)
	require.Equal(t, 70, model.Backend("ep8").Weight)
	require.Equal(t, DefaultWeight, model.Backend("pp2").Weight, "weight defaults to 100")
	require.False(t, model.Backend("b300").Enabled)
	require.False(t, model.Backend("b300").Usable())
	require.Nil(t, snapshot.Model("other"))
	var nilSnapshot *Snapshot
	require.Nil(t, nilSnapshot.Model("qwen3.5-27b"))
}

func TestParseRejects(t *testing.T) {
	backend := func(fields string) string {
		return `{"version":"v1","models":{"m":{"default":"a","backends":[` + fields + `]}}}`
	}
	cases := map[string]struct {
		doc  string
		want string
	}{
		"not json":              {`{`, "not valid JSON"},
		"not object":            {`[]`, "must be an object"},
		"unknown top field":     {`{"version":"v1","models":{},"modles":{}}`, `unknown field "modles"`},
		"missing version":       {`{"models":{}}`, "version"},
		"empty version":         {`{"version":" ","models":{}}`, "version"},
		"models not object":     {`{"version":"v1","models":[]}`, "models"},
		"no backends":           {`{"version":"v1","models":{"m":{"default":"a","backends":[]}}}`, "backends"},
		"unknown model field":   {`{"version":"v1","models":{"m":{"default":"a","backends":[{"id":"a","target":{"path":"/a"},"tier":0}],"tiers":1}}}`, `unknown field "tiers"`},
		"missing default":       {`{"version":"v1","models":{"m":{"backends":[{"id":"a","target":{"path":"/a"},"tier":0}]}}}`, "default"},
		"default not a backend": {`{"version":"v1","models":{"m":{"default":"b","backends":[{"id":"a","target":{"path":"/a"},"tier":0}]}}}`, "not one of"},
		"bad id":                {backend(`{"id":"A B","target":{"path":"/a"},"tier":0}`), ".id"},
		"duplicate id":          {backend(`{"id":"a","target":{"path":"/a"},"tier":0},{"id":"a","target":{"path":"/b"},"tier":0}`), "duplicate id"},
		"unknown backend field": {backend(`{"id":"a","target":{"path":"/a"},"tier":0,"maxConcurency":1}`), `unknown field "maxConcurency"`},
		"missing target":        {backend(`{"id":"a","tier":0}`), "target"},
		"reserved cluster":      {backend(`{"id":"a","target":{"path":"/a","cluster":"outbound|80||x"},"tier":0}`), "target.cluster: reserved"},
		"unknown target field":  {backend(`{"id":"a","target":{"path":"/a","host":"x"},"tier":0}`), `unknown field "host"`},
		"path no slash":         {backend(`{"id":"a","target":{"path":"a"},"tier":0}`), "target.path"},
		"path trailing slash":   {backend(`{"id":"a","target":{"path":"/a/"},"tier":0}`), "target.path"},
		"path double slash":     {backend(`{"id":"a","target":{"path":"/a//b"},"tier":0}`), "target.path"},
		"path traversal":        {backend(`{"id":"a","target":{"path":"/a/.."},"tier":0}`), "target.path"},
		"path query":            {backend(`{"id":"a","target":{"path":"/a?x=1"},"tier":0}`), "target.path"},
		"path percent":          {backend(`{"id":"a","target":{"path":"/a%2F"},"tier":0}`), "target.path"},
		"missing tier":          {backend(`{"id":"a","target":{"path":"/a"}}`), ".tier"},
		"negative tier":         {backend(`{"id":"a","target":{"path":"/a"},"tier":-1}`), ".tier"},
		"fractional tier":       {backend(`{"id":"a","target":{"path":"/a"},"tier":1.5}`), ".tier"},
		"weight too large":      {backend(`{"id":"a","target":{"path":"/a"},"tier":0,"weight":10001}`), ".weight"},
		"weight string":         {backend(`{"id":"a","target":{"path":"/a"},"tier":0,"weight":"1"}`), ".weight"},
		"enabled string":        {backend(`{"id":"a","target":{"path":"/a"},"tier":0,"enabled":"yes"}`), ".enabled"},
		"maxConcurrency > 0":    {backend(`{"id":"a","target":{"path":"/a"},"tier":0,"maxConcurrency":8}`), "maxConcurrency: reserved"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.doc))
			require.Error(t, err)
			require.True(t, strings.Contains(err.Error(), tc.want), "error %q should mention %q", err, tc.want)
		})
	}
}

func TestPathPrefixAcceptsUnreservedCharacters(t *testing.T) {
	require.NoError(t, validatePathPrefix("/qwen3.5-27b_H100~ep8/v2"))
}
