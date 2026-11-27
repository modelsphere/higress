package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/proxytest"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/higress-group/wasm-go/pkg/test"
	"github.com/stretchr/testify/require"
)

const staticTable = `{"version":"v1","models":{"qwen3.5-27b":{"default":"ep8","backends":[
  {"id":"ep8","target":{"path":"/qwen-ep8"},"tier":0,"weight":70},
  {"id":"pp2","target":{"path":"/qwen-pp2"},"tier":0,"weight":30},
  {"id":"b300","target":{"path":"/qwen-b300"},"tier":1},
  {"id":"spare","target":{"path":"/qwen-spare"},"tier":2}]}}}`

const chatBody = `{"model":"qwen3.5-27b","stream":true,"messages":[{"role":"user","content":"hi"}]}`

func pluginConfig(extra string) json.RawMessage {
	if extra != "" {
		extra = "," + extra
	}
	return json.RawMessage(`{"decisions":{"static":` + staticTable + `}` + extra + `}`)
}

// declared records the declare_property arguments the plugin sent.
var declared [][]byte

func newHost(t *testing.T, config json.RawMessage) test.TestHost {
	t.Helper()
	host, status := test.NewTestHostWithForeignFuncs(config, map[string]func([]byte) []byte{
		"declare_property": func(args []byte) []byte {
			declared = append(declared, append([]byte(nil), args...))
			// The emulator cannot return an empty result; Envoy returns none.
			return []byte{0}
		},
	})
	require.Equal(t, types.OnPluginStartStatusOK, status)
	return host
}

func requestHeaders(path string) [][2]string {
	return [][2]string{
		{":authority", "gw.example.com"}, {":method", "POST"}, {":path", path},
		{"content-type", "application/json"}, {"content-length", "80"},
	}
}

// pass runs the request side of one filter-chain pass.
func pass(t *testing.T, host test.TestHost, body string) types.Action {
	t.Helper()
	action := host.CallOnHttpRequestHeaders(requestHeaders("/v1/chat/completions"))
	require.Equal(t, types.HeaderStopIteration, action)
	return host.CallOnHttpRequestBody([]byte(body))
}

func header(headers [][2]string, name string) string {
	value, _ := test.GetHeaderValue(headers, name)
	return value
}

func backendOf(host test.TestHost) string {
	return header(host.GetRequestHeaders(), backendIDHeader)
}

func respond(t *testing.T, host test.TestHost, status, details string) [][2]string {
	t.Helper()
	require.NoError(t, host.SetProperty([]string{"response", "code_details"}, []byte(details)))
	host.CallOnHttpResponseHeaders([][2]string{{":status", status}, {"content-type", "application/json"}})
	return host.GetResponseHeaders()
}

func state(t *testing.T, host test.TestHost) routerState {
	t.Helper()
	raw, err := host.GetProperty([]string{stateProperty})
	require.NoError(t, err)
	s, err := decodeState(raw)
	require.NoError(t, err)
	return s
}

func counter(t *testing.T, host test.TestHost, name string) uint64 {
	t.Helper()
	value, err := host.GetCounterMetric(name)
	if err != nil {
		return 0
	}
	return value
}

func TestDeclaresStatePropertyWithRequestLifespan(t *testing.T) {
	test.RunGoTest(t, func(t *testing.T) {
		declared = nil
		host := newHost(t, pluginConfig(""))
		defer host.Reset()
		require.NotEmpty(t, declared)
		require.Equal(t, declareStateArguments(), declared[0])
		// name field, then span = DownstreamRequest (field 5, varint 1).
		require.True(t, strings.HasSuffix(string(declared[0]), stateProperty+"\x28\x01"))
	})
}

func TestFirstPassRoutesToTopTier(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := newHost(t, pluginConfig(""))
		defer host.Reset()
		require.Equal(t, types.ActionContinue, pass(t, host, chatBody))

		headers := host.GetRequestHeaders()
		backend := header(headers, backendIDHeader)
		require.Contains(t, []string{"ep8", "pp2"}, backend)
		require.Equal(t, "/qwen-"+backend+"/v1/chat/completions", header(headers, ":path"))
		require.Equal(t, "/v1/chat/completions", header(headers, originalPathHeader), "the redirect must restore the path the route matched")
		require.Equal(t, chatBody, string(host.GetRequestBody()), "the body is replaced with the same bytes")

		s := state(t, host)
		require.Equal(t, 0, s.Hop)
		require.Equal(t, []string{backend}, s.Tried)
		require.Equal(t, "qwen3.5-27b", s.Model)
		require.Equal(t, "v1", s.DecisionsVersion)
		require.Equal(t, uint64(1), counter(t, host, "ai_backend_router.model.qwen3_5-27b.backend."+backend+".tier.0.picks_total"))
	})
}

func TestKeepsOriginalPathSetByEarlierFilter(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := newHost(t, pluginConfig(""))
		defer host.Reset()
		headers := append(requestHeaders("/v1/chat/completions"), [2]string{originalPathHeader, "/v1/messages"})
		host.CallOnHttpRequestHeaders(headers)
		host.CallOnHttpRequestBody([]byte(chatBody))
		require.Equal(t, "/v1/messages", header(host.GetRequestHeaders(), originalPathHeader))
	})
}

func TestSpillMarkerOnlyForRetriableBackendAnswers(t *testing.T) {
	cases := []struct {
		name, status, details string
		spill                 bool
	}{
		{"backend 429", "429", "via_upstream", true},
		{"backend 503", "503", "via_upstream", true},
		{"connection reset", "503", "upstream_reset_before_response_started{connection_termination}", true},
		{"backend 500 not in codes", "500", "via_upstream", false},
		{"backend 200", "200", "via_upstream", false},
		{"local 429 from a rate limiter", "429", "local_rate_limited", false},
		{"envoy timeout", "504", "upstream_response_timeout", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			test.RunTest(t, func(t *testing.T) {
				host := newHost(t, pluginConfig(""))
				defer host.Reset()
				pass(t, host, chatBody)
				headers := respond(t, host, tc.status, tc.details)
				require.Equal(t, tc.spill, test.HasHeaderWithValue(headers, spillHeader, "1"))
				if tc.spill {
					require.Equal(t, tc.status, itoa(state(t, host).LastStatus))
				}
			})
		})
	}
}

func itoa(n int) string {
	raw, _ := json.Marshal(n)
	return string(raw)
}

func TestSpillWalksTiersAcrossPasses(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := newHost(t, pluginConfig(""))
		defer host.Reset()

		pass(t, host, chatBody)
		first := backendOf(host)
		require.True(t, test.HasHeaderWithValue(respond(t, host, "429", "via_upstream"), spillHeader, "1"))
		host.CompleteHttp()

		// custom_response replays the original request: a new pass with the
		// original path; the state property survived the redirect.
		pass(t, host, chatBody)
		require.Equal(t, "b300", backendOf(host), "exclude=tier skips the rest of tier 0")
		require.Equal(t, "/qwen-b300/v1/chat/completions", header(host.GetRequestHeaders(), ":path"))
		s := state(t, host)
		require.Equal(t, 1, s.Hop)
		require.Equal(t, []string{first, "b300"}, s.Tried)
		require.Equal(t, 429, s.LastStatus)
		require.True(t, test.HasHeaderWithValue(respond(t, host, "503", "via_upstream"), spillHeader, "1"))
		host.CompleteHttp()

		pass(t, host, chatBody)
		require.Equal(t, "spare", backendOf(host))
		headers := respond(t, host, "503", "via_upstream")
		require.False(t, test.HasHeader(headers, spillHeader), "no tier is left: the last upstream error goes to the client")
		host.CompleteHttp()

		require.Equal(t, uint64(1), counter(t, host, "ai_backend_router.model.qwen3_5-27b.backend."+first+".status.429.spills_total"))
		require.Equal(t, uint64(1), counter(t, host, "ai_backend_router.model.qwen3_5-27b.backend.b300.status.503.spills_total"))
		require.Equal(t, uint64(1), counter(t, host, "ai_backend_router.model.qwen3_5-27b.result.exhausted.requests_total"))
		require.Equal(t, uint64(0), counter(t, host, "ai_backend_router.model.qwen3_5-27b.result.served.requests_total"))
	})
}

func TestExcludeBackendTriesTheRestOfTheTier(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := newHost(t, pluginConfig(`"retry":{"exclude":"backend"}`))
		defer host.Reset()
		pass(t, host, chatBody)
		first := backendOf(host)
		respond(t, host, "429", "via_upstream")
		host.CompleteHttp()

		pass(t, host, chatBody)
		other := map[string]string{"ep8": "pp2", "pp2": "ep8"}[first]
		require.Equal(t, other, backendOf(host))
	})
}

func TestServedRequestCountedOnce(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := newHost(t, pluginConfig(""))
		defer host.Reset()
		pass(t, host, chatBody)
		respond(t, host, "429", "via_upstream")
		host.CompleteHttp()
		pass(t, host, chatBody)
		respond(t, host, "200", "via_upstream")
		host.CompleteHttp()
		require.Equal(t, uint64(1), counter(t, host, "ai_backend_router.model.qwen3_5-27b.result.served.requests_total"))
	})
}

func TestReplayWithoutBodyNeverGoesUpstream(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := newHost(t, pluginConfig(""))
		defer host.Reset()
		pass(t, host, chatBody)
		respond(t, host, "429", "via_upstream")
		host.CompleteHttp()

		// The replay arrives with end_of_stream on the headers: no body.
		action := host.CallOnHttpRequestHeaders(requestHeaders("/v1/chat/completions"), test.WithEndOfStream(true))
		require.Equal(t, types.ActionPause, action)
		local := host.GetLocalResponse()
		require.NotNil(t, local)
		require.Equal(t, uint32(429), local.StatusCode, "the previous upstream status is returned")
		require.True(t, test.HasHeaderWithValue(local.Headers, errorHeader, "replay_without_body"))
		require.Equal(t, uint64(1), counter(t, host, "ai_backend_router.reason.replay_without_body.fallback_total"))
	})
}

func TestBodylessFirstPassIsUntouched(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := newHost(t, pluginConfig(""))
		defer host.Reset()
		action := host.CallOnHttpRequestHeaders([][2]string{{":authority", "gw.example.com"}, {":method", "GET"}, {":path", "/v1/models"}}, test.WithEndOfStream(true))
		require.Equal(t, types.ActionContinue, action)
		require.Nil(t, host.GetLocalResponse())
		require.Equal(t, "/v1/models", header(host.GetRequestHeaders(), ":path"))
	})
}

func TestNonJSONBodyIsUntouched(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := newHost(t, pluginConfig(""))
		defer host.Reset()
		headers := [][2]string{{":authority", "gw.example.com"}, {":method", "POST"}, {":path", "/v1/files"}, {"content-type", "multipart/form-data; boundary=x"}}
		require.Equal(t, types.ActionContinue, host.CallOnHttpRequestHeaders(headers))
		require.Equal(t, "/v1/files", header(host.GetRequestHeaders(), ":path"))
	})
}

func TestRejectsBadRequests(t *testing.T) {
	cases := []struct {
		name, body string
		status     uint32
		code       string
	}{
		{"unknown model", `{"model":"gpt-x","messages":[]}`, 404, "model_not_found"},
		{"missing model", `{"messages":[]}`, 400, "invalid_request"},
		{"model not a string", `{"model":7}`, 400, "invalid_request"},
		{"duplicate model", `{"model":"qwen3.5-27b","model":"gpt-x"}`, 400, "duplicate_model_member"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			test.RunTest(t, func(t *testing.T) {
				host := newHost(t, pluginConfig(""))
				defer host.Reset()
				require.Equal(t, types.ActionPause, pass(t, host, tc.body))
				local := host.GetLocalResponse()
				require.NotNil(t, local)
				require.Equal(t, tc.status, local.StatusCode)
				require.True(t, test.HasHeaderWithValue(local.Headers, errorHeader, tc.code))
				require.Contains(t, string(local.Data), `"code":"`+tc.code+`"`)
			})
		})
	}
}

func TestNoUsableBackendFallsBackToDefaultWithoutSpill(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		table := `{"version":"v1","models":{"m":{"default":"b","backends":[
			{"id":"a","target":{"path":"/a"},"tier":0,"weight":0},
			{"id":"b","target":{"path":"/b"},"tier":1,"enabled":false}]}}}`
		host := newHost(t, json.RawMessage(`{"decisions":{"static":`+table+`}}`))
		defer host.Reset()
		pass(t, host, `{"model":"m"}`)
		require.Equal(t, "/b/v1/chat/completions", header(host.GetRequestHeaders(), ":path"))
		require.False(t, test.HasHeader(respond(t, host, "429", "via_upstream"), spillHeader))
		host.CompleteHttp()
		require.Equal(t, uint64(1), counter(t, host, "ai_backend_router.reason.no_candidate.fallback_total"))
		require.Equal(t, uint64(1), counter(t, host, "ai_backend_router.model.m.result.fallback.requests_total"))
	})
}

func TestCorruptStateRoutesToDefaultWithoutSpill(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := newHost(t, pluginConfig(""))
		defer host.Reset()
		require.NoError(t, host.SetProperty([]string{stateProperty}, []byte("{not json")))
		pass(t, host, chatBody)
		require.Equal(t, "ep8", backendOf(host))
		require.False(t, test.HasHeader(respond(t, host, "429", "via_upstream"), spillHeader))
	})
}

func TestRejectsInvalidConfig(t *testing.T) {
	cases := map[string]string{
		"no decisions":          `{}`,
		"no static":             `{"decisions":{}}`,
		"invalid static":        `{"decisions":{"static":{"version":"v1","models":{"m":{"default":"a","backends":[{"id":"a","target":{"path":"/a"},"tier":0,"maxConcurrency":4}]}}}}}`,
		"codes empty":           `{"retry":{"codes":[]},"decisions":{"static":` + staticTable + `}}`,
		"code out of range":     `{"retry":{"codes":[200]},"decisions":{"static":` + staticTable + `}}`,
		"exclude unknown":       `{"retry":{"exclude":"model"},"decisions":{"static":` + staticTable + `}}`,
		"maxAttempts reserved":  `{"retry":{"maxAttempts":3},"decisions":{"static":` + staticTable + `}}`,
		"body limit too large":  `{"limits":{"maxRequestBodyBytes":1073741824},"decisions":{"static":` + staticTable + `}}`,
		"source without name":   `{"decisions":{"static":` + staticTable + `,"source":{"servicePort":80,"path":"/d"}}}`,
		"source bad port":       `{"decisions":{"static":` + staticTable + `,"source":{"serviceName":"d.example.com","servicePort":0,"path":"/d"}}}`,
		"source bad path":       `{"decisions":{"static":` + staticTable + `,"source":{"serviceName":"d.example.com","servicePort":80,"path":"d"}}}`,
		"source short interval": `{"decisions":{"static":` + staticTable + `,"source":{"serviceName":"d.example.com","servicePort":80,"path":"/d","intervalMs":10}}}`,
		"unknown top field":     `{"retires":{},"decisions":{"static":` + staticTable + `}}`,
		"unknown retry field":   `{"retry":{"code":[429]},"decisions":{"static":` + staticTable + `}}`,
		"unknown limits field":  `{"limits":{"maxBodyBytes":1},"decisions":{"static":` + staticTable + `}}`,
		"unknown decisions key": `{"decisions":{"static":` + staticTable + `,"sources":{}}}`,
		"unknown source field":  `{"decisions":{"static":` + staticTable + `,"source":{"serviceName":"d.example.com","servicePort":80,"path":"/d","interval":5000}}}`,
	}
	for name, config := range cases {
		t.Run(name, func(t *testing.T) {
			test.RunGoTest(t, func(t *testing.T) {
				host, status := test.NewTestHostWithForeignFuncs(json.RawMessage(config), map[string]func([]byte) []byte{
					"declare_property": func([]byte) []byte { return []byte{0} },
				})
				defer host.Reset()
				require.Equal(t, types.OnPluginStartStatusFailed, status)
			})
		})
	}
}

const sourceConfigJSON = `"source":{"serviceName":"decision.example.svc.cluster.local","servicePort":8080,"path":"/v1/decisions","intervalMs":1000}`

func sourcePluginConfig() json.RawMessage {
	return json.RawMessage(`{"decisions":{"static":` + staticTable + `,` + sourceConfigJSON + `}}`)
}

const pulledTable = `{"version":"v2","models":{"qwen3.5-27b":{"default":"new","backends":[
  {"id":"new","target":{"path":"/qwen-new"},"tier":0}]}}}`

// Pulls run in the root context (on tick), so their callouts are recorded
// under the plugin context, not under the current HTTP context.
func rootCallouts(host test.TestHost) []proxytest.HttpCalloutAttribute {
	return host.GetCalloutAttributesFromContext(proxytest.PluginContextID)
}

func answerPull(host test.TestHost, headers [][2]string, body string) {
	callouts := rootCallouts(host)
	host.CallOnHttpCallResponse(callouts[len(callouts)-1].CalloutID, headers, nil, []byte(body))
}

func TestPullReplacesTheStaticTable(t *testing.T) {
	test.RunGoTest(t, func(t *testing.T) {
		host := newHost(t, sourcePluginConfig())
		defer host.Reset()

		host.Tick()
		callouts := rootCallouts(host)
		require.Len(t, callouts, 1)
		require.True(t, test.HasHeaderWithValue(callouts[0].Headers, ":path", "/v1/decisions"))
		require.Equal(t, "outbound|8080||decision.example.svc.cluster.local", callouts[0].Upstream)

		answerPull(host, [][2]string{{":status", "200"}, {"etag", `"v2"`}}, pulledTable)
		pass(t, host, chatBody)
		require.Equal(t, "new", backendOf(host))
		require.Equal(t, "v2", state(t, host).DecisionsVersion)
	})
}

func TestPullFailureKeepsTheCurrentTable(t *testing.T) {
	for name, response := range map[string]struct {
		status string
		body   string
	}{
		"server error":     {"500", `oops`},
		"invalid document": {"200", `{"version":"v2","models":{"qwen3.5-27b":{"default":"x","backends":[]}}}`},
	} {
		t.Run(name, func(t *testing.T) {
			test.RunGoTest(t, func(t *testing.T) {
				host := newHost(t, sourcePluginConfig())
				defer host.Reset()
				host.Tick()
				require.Len(t, rootCallouts(host), 1)
				answerPull(host, [][2]string{{":status", response.status}}, response.body)
				require.Equal(t, uint64(1), counter(t, host, "ai_backend_router.decision_pull_failures_total"))
				pass(t, host, chatBody)
				require.Contains(t, []string{"ep8", "pp2"}, backendOf(host))
				require.Equal(t, "v1", state(t, host).DecisionsVersion)
			})
		})
	}
}

func TestPullHonoursIntervalAndETag(t *testing.T) {
	test.RunGoTest(t, func(t *testing.T) {
		host := newHost(t, sourcePluginConfig())
		defer host.Reset()
		host.Tick()
		answerPull(host, [][2]string{{":status", "200"}, {"etag", `"v2"`}}, pulledTable)

		host.Tick()
		require.Empty(t, rootCallouts(host), "no second pull within intervalMs (answered callouts are not listed)")

		time.Sleep(1100 * time.Millisecond)
		host.Tick()
		callouts := rootCallouts(host)
		require.Len(t, callouts, 1)
		require.True(t, test.HasHeaderWithValue(callouts[0].Headers, "if-none-match", `"v2"`))

		answerPull(host, [][2]string{{":status", "304"}}, "")
		require.Equal(t, uint64(0), counter(t, host, "ai_backend_router.decision_pull_failures_total"), "304 keeps the table and is not a failure")
		pass(t, host, chatBody)
		require.Equal(t, "new", backendOf(host))
	})
}

func TestAcceptsWrapperKeys(t *testing.T) {
	test.RunGoTest(t, func(t *testing.T) {
		config := json.RawMessage(`{"decisions":{"static":` + staticTable + `},"_rules_":[{"_match_route_":["r1"],"decisions":{"static":` + staticTable + `}}]}`)
		host := newHost(t, config)
		defer host.Reset()
	})
}

func TestUpstreamSpillMarkerIsRemoved(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		// A routed pass: the backend answers 200 with the marker.
		host := newHost(t, pluginConfig(""))
		defer host.Reset()
		pass(t, host, chatBody)
		require.NoError(t, host.SetProperty([]string{"response", "code_details"}, []byte("via_upstream")))
		host.CallOnHttpResponseHeaders([][2]string{{":status", "200"}, {spillHeader, "1"}})
		require.False(t, test.HasHeader(host.GetResponseHeaders(), spillHeader), "only the plugin may ask for a redirect")
	})
	test.RunTest(t, func(t *testing.T) {
		// A pass the plugin did not route (no body).
		host := newHost(t, pluginConfig(""))
		defer host.Reset()
		host.CallOnHttpRequestHeaders([][2]string{{":authority", "gw.example.com"}, {":method", "GET"}, {":path", "/v1/models"}}, test.WithEndOfStream(true))
		host.CallOnHttpResponseHeaders([][2]string{{":status", "429"}, {spillHeader, "1"}})
		require.False(t, test.HasHeader(host.GetResponseHeaders(), spillHeader))
	})
}

func TestUnknownModelMetricHasAFixedLabel(t *testing.T) {
	test.RunGoTest(t, func(t *testing.T) {
		host := newHost(t, pluginConfig(""))
		defer host.Reset()
		for _, name := range []string{"gpt-x", "attacker-chosen-name"} {
			pass(t, host, `{"model":"`+name+`"}`)
			host.CompleteHttp()
		}
		require.Equal(t, uint64(2), counter(t, host, "ai_backend_router.model.unknown.result.unknown_model.requests_total"))
		_, err := host.GetCounterMetric("ai_backend_router.model.gpt-x.result.unknown_model.requests_total")
		require.Error(t, err, "client-supplied model names must not become metric names")
	})
}

func TestReplayWithoutBodyEvenWithUnreadableState(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host := newHost(t, pluginConfig(""))
		defer host.Reset()
		require.NoError(t, host.SetProperty([]string{stateProperty}, []byte("{not json")))
		action := host.CallOnHttpRequestHeaders(requestHeaders("/v1/chat/completions"), test.WithEndOfStream(true))
		require.Equal(t, types.ActionPause, action)
		local := host.GetLocalResponse()
		require.NotNil(t, local)
		require.Equal(t, uint32(502), local.StatusCode)
	})
}

// One invalid rule must not take routing off every other route: the plugin
// still starts, the valid route is routed, the invalid rule is skipped.
func TestInvalidRuleIsIsolated(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		config := json.RawMessage(`{"_rules_":[
			{"_match_route_":["iso-good"],"decisions":{"static":` + staticTable + `}},
			{"_match_route_":["iso-bad"],"decisions":{"static":{"version":"v1","models":{}}},"retry":{"exclude":"nope"}}]}`)
		host := newHost(t, config)
		defer host.Reset()

		require.NoError(t, host.SetRouteName("iso-good"))
		pass(t, host, chatBody)
		require.NotEmpty(t, backendOf(host), "the valid route is still routed")
		host.CompleteHttp()

		require.NoError(t, host.SetRouteName("iso-bad"))
		action := host.CallOnHttpRequestHeaders(requestHeaders("/v1/chat/completions"))
		require.Equal(t, types.ActionContinue, action, "the invalid rule is skipped: the plugin does not run on its route")
		require.Empty(t, backendOf(host))
	})
}
