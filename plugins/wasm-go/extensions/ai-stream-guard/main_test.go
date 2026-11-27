package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/higress-group/wasm-go/pkg/test"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/stretchr/testify/require"
)

const loopConfig = `{"detectors":[{"type":"repetition","max_period":2,"min_repeated_chunks":6}]}`

func chatChunk(choice int, field, text string) string {
	b, _ := json.Marshal(text)
	return fmt.Sprintf(`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1763520000,"model":"qwen3","choices":[{"index":%d,"delta":{%q:%s},"finish_reason":null}]}`+"\n\n", choice, field, b)
}

func content(text string) string { return chatChunk(0, "content", text) }

func repeatChunks(n int, texts ...string) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteString(content(texts[i%len(texts)]))
	}
	return b.String()
}

const terminalContentFilter = `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1763520000,"model":"qwen3","choices":[{"index":0,"delta":{},"finish_reason":"content_filter"}]}` + "\n\n" + "data: [DONE]\n\n"

var sseHeaders = [][2]string{{":status", "200"}, {"content-type", "text/event-stream; charset=utf-8"}}

func startStream(t *testing.T, config string, respHeaders [][2]string) test.TestHost {
	t.Helper()
	host, status := test.NewTestHost(json.RawMessage(config))
	require.Equal(t, types.OnPluginStartStatusOK, status)
	host.CallOnHttpRequestHeaders([][2]string{
		{":authority", "example.com"}, {":path", "/v1/chat/completions"}, {":method", "POST"},
	})
	require.NoError(t, host.SetRouteName("r1"))
	require.NoError(t, host.SetClusterName("c1"))
	host.CallOnHttpResponseHeaders(respHeaders)
	return host
}

func body(t *testing.T, host test.TestHost, chunk string, eos bool) string {
	t.Helper()
	host.CallOnHttpStreamingResponseBody([]byte(chunk), eos)
	return string(host.GetResponseBody())
}

func aiLog(t *testing.T, host test.TestHost) map[string]any {
	t.Helper()
	raw, err := host.GetProperty([]string{wrapper.AILogKey})
	if err != nil || len(raw) == 0 {
		return map[string]any{}
	}
	m := map[string]any{}
	require.NoError(t, json.Unmarshal([]byte(wrapper.UnmarshalStr(`"`+string(raw)+`"`)), &m))
	return m
}

func TestPluginStartRejectsInvalidConfig(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host, status := test.NewTestHost(json.RawMessage(`{"detectors":[{"type":"repetition","max_period":0}]}`))
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusFailed, status)
	})
}

func TestBelowThresholdPassesThrough(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := startStream(t, loopConfig, sseHeaders)
		defer host.Reset()
		in := repeatChunks(5, "a")
		require.Equal(t, in, body(t, host, in, false))
		require.Empty(t, aiLog(t, host)["stream_guard_detector"])
	})
}

func TestCutInsideCallback(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := startStream(t, loopConfig, sseHeaders)
		defer host.Reset()
		// Period-2 loop; fires on the 6th chunk. Chunks 7.. must be dropped.
		in := content("x") + repeatChunks(10, "ab", "cd")
		got := body(t, host, in, false)
		want := content("x") + repeatChunks(6, "ab", "cd") + terminalContentFilter
		require.Equal(t, want, got)

		log := aiLog(t, host)
		require.Equal(t, "repetition", log["stream_guard_detector"])
		require.Equal(t, "cut", log["stream_guard_action"])
		require.EqualValues(t, 0, log["stream_guard_choice"])
		require.EqualValues(t, 2, log["stream_guard_period"])
		require.EqualValues(t, 3, log["stream_guard_repeats"])
		require.EqualValues(t, 6, log["stream_guard_tail_chunks"])
		require.EqualValues(t, 7, log["stream_guard_chunks_seen"])
		require.Equal(t, "abcd", log["stream_guard_sample"])
		require.Equal(t, true, log["stream_guard_usage_missing"])

		v, err := host.GetCounterMetric("route.r1.upstream.c1.model.qwen3.detector.repetition.metric.stream_guard_cutoff")
		require.NoError(t, err)
		require.EqualValues(t, 1, v)

		// Later upstream data must not be injected again.
		host.CallOnHttpStreamingResponseBody([]byte(repeatChunks(4, "ab", "cd")), true)
		require.Equal(t, want, string(host.GetResponseBody()))
	})
}

func TestForwardsWholeEventsOnly(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := startStream(t, loopConfig, sseHeaders)
		defer host.Reset()
		e1, e2 := content("hello"), content("world")
		split := len(e2) / 2
		require.Equal(t, e1, body(t, host, e1+e2[:split], false))
		require.Equal(t, e2, body(t, host, e2[split:], false))
		// An unterminated trailing event is flushed at end of stream.
		require.Equal(t, "data: [DONE]", body(t, host, "data: [DONE]", true))
	})
}

func TestCutAcrossCallbacks(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := startStream(t, loopConfig, sseHeaders)
		defer host.Reset()
		first := repeatChunks(5, "a")
		require.Equal(t, first, body(t, host, first, false))
		// The 6th chunk arrives split; it fires once complete.
		sixth := content("a")
		require.Equal(t, "", body(t, host, sixth[:10], false))
		got := body(t, host, sixth[10:]+content("a"), false)
		require.Equal(t, sixth+terminalContentFilter, got)
	})
}

func TestNotInspected(t *testing.T) {
	cases := map[string][][2]string{
		"non-200":  {{":status", "500"}, {"content-type", "text/event-stream"}},
		"not SSE":  {{":status", "200"}, {"content-type", "application/json"}},
		"no ctype": {{":status", "200"}},
	}
	for name, headers := range cases {
		t.Run(name, func(t *testing.T) {
			test.RunTest(t, func(t *testing.T) {
				host := startStream(t, loopConfig, headers)
				defer host.Reset()
				in := repeatChunks(20, "a")
				require.Equal(t, in, body(t, host, in, false))
				require.Empty(t, aiLog(t, host))
			})
		})
	}
}

func TestEncodedResponseBypassed(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := startStream(t, loopConfig, append(sseHeaders, [2]string{"content-encoding", "gzip"}))
		defer host.Reset()
		in := repeatChunks(20, "a")
		require.Equal(t, in, body(t, host, in, false))
		require.Equal(t, "encoded", aiLog(t, host)["stream_guard_bypass"])
		v, err := host.GetCounterMetric("route.r1.upstream.c1.model.-.reason.encoded.metric.stream_guard_bypass")
		require.NoError(t, err)
		require.EqualValues(t, 1, v)
	})
	// Control: the same stream without content-encoding is cut.
	test.RunTest(t, func(t *testing.T) {
		host := startStream(t, loopConfig, sseHeaders)
		defer host.Reset()
		require.Contains(t, body(t, host, repeatChunks(6, "a"), false), "content_filter")
	})
}

func TestContentLengthRemoved(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := startStream(t, loopConfig, append(sseHeaders, [2]string{"content-length", "100"}))
		defer host.Reset()
		require.False(t, test.HasHeader(host.GetResponseHeaders(), "content-length"))
	})
}

func TestChoicesBeyondLimitNotInspected(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := startStream(t, loopConfig, sseHeaders)
		defer host.Reset()
		var in strings.Builder
		for i := 0; i < 20; i++ {
			in.WriteString(chatChunk(maxChoices, "content", "a"))
		}
		require.Equal(t, in.String(), body(t, host, in.String(), false))
		require.Equal(t, "too_many_choices", aiLog(t, host)["stream_guard_bypass"])
		v, err := host.GetCounterMetric("route.r1.upstream.c1.model.qwen3.reason.too_many_choices.metric.stream_guard_bypass")
		require.NoError(t, err)
		require.EqualValues(t, 1, v, "counted once per stream")
	})
	// Control: the highest tracked index is inspected.
	test.RunTest(t, func(t *testing.T) {
		host := startStream(t, loopConfig, sseHeaders)
		defer host.Reset()
		var in strings.Builder
		for i := 0; i < 6; i++ {
			in.WriteString(chatChunk(maxChoices-1, "content", "a"))
		}
		require.Contains(t, body(t, host, in.String(), false), `"index":15,"delta":{},"finish_reason":"content_filter"`)
	})
}

func TestParseErrorDoesNotStopInspection(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := startStream(t, loopConfig, sseHeaders)
		defer host.Reset()
		bad := "data: {\"choices\":[\n\n"
		in := bad + repeatChunks(3, "a") + bad + repeatChunks(3, "a")
		got := body(t, host, in, false)
		require.Equal(t, in+terminalContentFilter, got)
		log := aiLog(t, host)
		require.Equal(t, "parse_error", log["stream_guard_bypass"])
		require.Equal(t, "repetition", log["stream_guard_detector"])
		// The malformed event came before any chunk named the model.
		v, err := host.GetCounterMetric("route.r1.upstream.c1.model.-.reason.parse_error.metric.stream_guard_bypass")
		require.NoError(t, err)
		require.EqualValues(t, 1, v)
	})
}

func TestOversizeEventSwitchesToPassThrough(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := startStream(t, loopConfig, sseHeaders)
		defer host.Reset()
		huge := "data: " + strings.Repeat("x", 1<<20)
		require.Equal(t, huge, body(t, host, huge, false), "held bytes are released, not dropped")
		require.Equal(t, "oversize_event", aiLog(t, host)["stream_guard_bypass"])
		in := "\n\n" + repeatChunks(20, "a")
		require.Equal(t, in, body(t, host, in, false), "no inspection after bypass")
	})
}

func TestCutoffConfigAndFinishedChoices(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		cfg := `{"detectors":[{"type":"repetition","min_repeated_chunks":4,"fields":["reasoning_content"]}],
		         "cutoff":{"finish_reason":"length","message":"[stopped]"}}`
		host := startStream(t, cfg, sseHeaders)
		defer host.Reset()
		finished := `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1763520000,"model":"qwen3","choices":[{"index":1,"delta":{},"finish_reason":"stop"}]}` + "\n\n"
		in := chatChunk(1, "content", "z") + finished
		// Content is not watched: a content loop must not fire.
		for i := 0; i < 8; i++ {
			in += chatChunk(0, "content", "a")
		}
		require.Equal(t, in, body(t, host, in, false))
		// Reasoning (sent as reasoning_content) is watched.
		loop := strings.Repeat(chatChunk(0, "reasoning_content", "r"), 4)
		got := body(t, host, loop, false)
		require.True(t, strings.HasPrefix(got, loop))
		tail := strings.TrimPrefix(got, loop)
		wantTail := `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1763520000,"model":"qwen3","choices":[{"index":0,"delta":{"content":"[stopped]"},"finish_reason":null}]}` + "\n\n" +
			`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1763520000,"model":"qwen3","choices":[{"index":0,"delta":{},"finish_reason":"length"}]}` + "\n\n" +
			"data: [DONE]\n\n"
		require.Equal(t, wantTail, tail, "choice 1 already finished and must not get a terminal chunk")
	})
}

func TestCompletionsEndpoint(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := startStream(t, loopConfig, sseHeaders)
		defer host.Reset()
		ev := `data: {"id":"cmpl-1","object":"text_completion","created":5,"model":"m","choices":[{"index":0,"text":"t","finish_reason":null}]}` + "\n\n"
		in := strings.Repeat(ev, 6)
		want := in + `data: {"id":"cmpl-1","object":"text_completion","created":5,"model":"m","choices":[{"index":0,"text":"","finish_reason":"content_filter"}]}` + "\n\n" + "data: [DONE]\n\n"
		require.Equal(t, want, body(t, host, in, false))
	})
}

func TestFirstBypassReasonKeptAllCounted(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := startStream(t, loopConfig, sseHeaders)
		defer host.Reset()
		in := content("m") + "data: {bad\n\n" + chatChunk(maxChoices, "content", "a") + "data: {bad\n\n"
		require.Equal(t, in, body(t, host, in, false))
		require.Equal(t, "parse_error", aiLog(t, host)["stream_guard_bypass"])
		for _, reason := range []string{"parse_error", "too_many_choices"} {
			v, err := host.GetCounterMetric("route.r1.upstream.c1.model.qwen3.reason." + reason + ".metric.stream_guard_bypass")
			require.NoError(t, err, reason)
			require.EqualValues(t, 1, v, reason)
		}
	})
}

func TestTriggerOnFinishingChunkSendsNoSecondFinish(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := startStream(t, loopConfig, sseHeaders)
		defer host.Reset()
		// vLLM often sends the last token together with finish_reason.
		last := `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1763520000,"model":"qwen3","choices":[{"index":0,"delta":{"content":"a"},"finish_reason":"length"}]}` + "\n\n"
		in := repeatChunks(5, "a") + last
		got := body(t, host, in, false)
		require.Equal(t, in+"data: [DONE]\n\n", got, "choice 0 already finished: no extra terminal chunk")
		require.Equal(t, 1, strings.Count(got, `"finish_reason":"`))
	})
}

// One invalid rule must not take the guard off every other route: the plugin
// still starts, the valid route is guarded, the invalid rule is skipped.
func TestInvalidRuleIsIsolated(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host, status := test.NewTestHost(json.RawMessage(`{"_rules_":[
			{"_match_route_":["iso-good"],"detectors":[{"type":"repetition","max_period":2,"min_repeated_chunks":6}]},
			{"_match_route_":["iso-bad"],"detectors":[{"type":"repetition","max_period":4,"min_repeated_chunks":2}]}]}`))
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)

		run := func(route string) string {
			require.NoError(t, host.SetRouteName(route))
			host.CallOnHttpRequestHeaders([][2]string{{":authority", "example.com"}, {":path", "/v1/chat/completions"}, {":method", "POST"}})
			host.CallOnHttpResponseHeaders(sseHeaders)
			out := body(t, host, repeatChunks(20, "a"), false)
			host.CompleteHttp()
			return out
		}
		require.Contains(t, run("iso-good"), `"finish_reason":"content_filter"`, "the valid route is still guarded")
		require.Equal(t, repeatChunks(20, "a"), run("iso-bad"), "the invalid rule is skipped: its route passes through")
	})
}

// Isolation has a floor: with no valid rule and no global config the plugin
// start still fails (the README says so).
func TestAllRulesInvalidStillFails(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host, status := test.NewTestHost(json.RawMessage(`{"_rules_":[{"_match_route_":["all-bad"],"detectors":[{"type":"repetition","max_period":0}]}]}`))
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusFailed, status)
	})
}
