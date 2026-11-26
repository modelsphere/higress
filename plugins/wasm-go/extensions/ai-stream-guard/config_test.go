package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-stream-guard/detector"
	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-stream-guard/detector/repetition"
)

func parse(t *testing.T, js string) (pluginConfig, error) {
	t.Helper()
	var cfg pluginConfig
	err := parseGlobalConfig(gjson.Parse(js), &cfg)
	return cfg, err
}

func repCfg(t *testing.T, d detector.Detector) repetition.Config {
	t.Helper()
	r, ok := d.(*repetition.Detector)
	require.True(t, ok)
	return r.Config()
}

func TestParseDefaults(t *testing.T) {
	cfg, err := parse(t, `{"detectors":[{"type":"repetition"}]}`)
	require.NoError(t, err)
	require.Len(t, cfg.detectors, 1)
	d := cfg.detectors[0]
	require.Equal(t, "repetition", d.Name())
	require.Equal(t, detector.ActionCut, d.Action())
	rc := repCfg(t, d)
	require.Equal(t, 1, rc.MaxPeriod)
	require.Equal(t, 512, rc.MinRepeatedChunks)
	require.True(t, rc.Fields.Has(detector.FieldContent))
	require.True(t, rc.Fields.Has(detector.FieldReasoning))
	require.Equal(t, "content_filter", cfg.cutoff.FinishReason)
	require.Equal(t, "", cfg.cutoff.Message)
}

func TestParseFull(t *testing.T) {
	cfg, err := parse(t, `{
	  "detectors":[
	    {"name":"rep-short","type":"repetition","action":"cut","max_period":4,"min_repeated_chunks":512,"fields":["content"]},
	    {"name":"rep-long","type":"repetition","max_period":128,"min_repeated_chunks":2048,"fields":["reasoning_content","reasoning"]}
	  ],
	  "cutoff":{"finish_reason":"length","message":"[stopped]"}
	}`)
	require.NoError(t, err)
	require.Len(t, cfg.detectors, 2)
	short, long := repCfg(t, cfg.detectors[0]), repCfg(t, cfg.detectors[1])
	require.Equal(t, 4, short.MaxPeriod)
	require.False(t, short.Fields.Has(detector.FieldReasoning))
	require.Equal(t, 128, long.MaxPeriod)
	require.Equal(t, 2048, long.MinRepeatedChunks)
	require.True(t, long.Fields.Has(detector.FieldReasoning))
	require.False(t, long.Fields.Has(detector.FieldContent))
	require.Equal(t, "length", cfg.cutoff.FinishReason)
	require.Equal(t, "[stopped]", cfg.cutoff.Message)
}

func TestParseRejects(t *testing.T) {
	cases := map[string]string{
		"empty config":         `{}`,
		"detectors missing":    `{"cutoff":{}}`,
		"detectors empty":      `{"detectors":[]}`,
		"detectors not list":   `{"detectors":{"type":"repetition"}}`,
		"entry not object":     `{"detectors":["repetition"]}`,
		"type missing":         `{"detectors":[{"name":"x"}]}`,
		"type not string":      `{"detectors":[{"type":1}]}`,
		"unknown type":         `{"detectors":[{"type":"ngram"}]}`,
		"unknown key (typo)":   `{"detectors":[{"type":"repetition","min_repeat_chunks":10}]}`,
		"unknown action":       `{"detectors":[{"type":"repetition","action":"observe"}]}`,
		"empty name":           `{"detectors":[{"type":"repetition","name":""}]}`,
		"duplicate name":       `{"detectors":[{"type":"repetition"},{"type":"repetition"}]}`,
		"period 0":             `{"detectors":[{"type":"repetition","max_period":0}]}`,
		"period 129":           `{"detectors":[{"type":"repetition","max_period":129,"min_repeated_chunks":1000}]}`,
		"period not integer":   `{"detectors":[{"type":"repetition","max_period":1.5}]}`,
		"period string":        `{"detectors":[{"type":"repetition","max_period":"4"}]}`,
		"N below 2L":           `{"detectors":[{"type":"repetition","max_period":16,"min_repeated_chunks":31}]}`,
		"fields empty":         `{"detectors":[{"type":"repetition","fields":[]}]}`,
		"fields unknown":       `{"detectors":[{"type":"repetition","fields":["tool_arguments"]}]}`,
		"fields not strings":   `{"detectors":[{"type":"repetition","fields":[1]}]}`,
		"finish_reason bad":    `{"detectors":[{"type":"repetition"}],"cutoff":{"finish_reason":"loop_detected"}}`,
		"cutoff not object":    `{"detectors":[{"type":"repetition"}],"cutoff":"stop"}`,
		"message not a string": `{"detectors":[{"type":"repetition"}],"cutoff":{"message":1}}`,
	}
	for name, js := range cases {
		_, err := parse(t, js)
		require.Error(t, err, name)
	}
}

func TestParseAcceptsBoundaries(t *testing.T) {
	for _, js := range []string{
		`{"detectors":[{"type":"repetition","max_period":1,"min_repeated_chunks":2}]}`,
		`{"detectors":[{"type":"repetition","max_period":128,"min_repeated_chunks":256}]}`,
		`{"detectors":[{"type":"repetition","max_period":16,"min_repeated_chunks":32}]}`,
		`{"detectors":[{"type":"repetition"}],"unknown_top_level":1,"cutoff":{"unknown":1}}`,
	} {
		_, err := parse(t, js)
		require.NoError(t, err, js)
	}
}

func TestParseRuleOverridesGlobal(t *testing.T) {
	global, err := parse(t, `{"detectors":[{"type":"repetition","max_period":2}],"cutoff":{"finish_reason":"length","message":"m"}}`)
	require.NoError(t, err)

	// Only cutoff.finish_reason changes; detectors and message are inherited.
	var rule pluginConfig
	require.NoError(t, parseRuleConfig(gjson.Parse(`{"_match_route_":["r"],"cutoff":{"finish_reason":"stop"}}`), global, &rule))
	require.Same(t, global.detectors[0], rule.detectors[0])
	require.Equal(t, "stop", rule.cutoff.FinishReason)
	require.Equal(t, "m", rule.cutoff.Message)

	// A rule's detectors replace the whole list.
	require.NoError(t, parseRuleConfig(gjson.Parse(`{"_match_route_":["r"],"detectors":[{"name":"x","type":"repetition","max_period":8}]}`), global, &rule))
	require.Len(t, rule.detectors, 1)
	require.Equal(t, "x", rule.detectors[0].Name())

	// A rule without a usable global must define detectors itself.
	err = parseRuleConfig(gjson.Parse(`{"_match_route_":["r"],"cutoff":{"finish_reason":"stop"}}`), pluginConfig{}, &rule)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "detectors"))
}
