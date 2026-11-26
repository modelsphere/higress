// ai-backend-router picks, per request, which backend deployment serves an
// LLM call. Backends of a model share one upstream (openresty) and differ by
// path prefix; the plugin walks the model's priority tiers, picks by weight
// within the first tier that has candidates, and rewrites :path.
//
// A retriable answer from the chosen backend (before any response byte)
// spills the request: the plugin marks the response with spillHeader, and the
// gateway's custom_response filter replays the original request through the
// filter chain, where the plugin skips what this request already tried. The
// plugin decides alone; custom_response only matches the marker.
package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-backend-router/decisions"
	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-backend-router/selection"
	"github.com/alibaba/higress/plugins/wasm-go/pkg/jsonbody"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/higress-group/wasm-go/pkg/log"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
)

const (
	pluginName = "ai-backend-router"

	// spillHeader marks a response that custom_response must turn into an
	// internal redirect. It never reaches the client unless the redirect
	// itself fails.
	spillHeader = "x-higress-spill"
	// backendIDHeader tells later filters (ai-statistics) and openresty which
	// backend served the pass.
	backendIDHeader = "x-higress-backend-id"
	// originalPathHeader is what custom_response's use_original_request_uri
	// restores. ai-proxy sets it when it rewrites the path; otherwise this
	// plugin sets it to the path it saw, before adding the prefix.
	originalPathHeader = "x-envoy-original-path"
	// errorHeader tags local replies from this plugin.
	errorHeader = "x-higress-backend-router"

	pickContextKey = "ai_backend_router_pick"
)

// pick is what one pass chose, kept for the response and stream-done phases.
type pick struct {
	model     *decisions.Model
	backend   *decisions.Backend
	state     routerState
	fallback  string // non-empty when the default backend was used
	spillable bool
	spilled   bool
	exhausted bool
}

func main() {}

func init() {
	wrapper.SetCtx(
		pluginName,
		wrapper.ParseConfig(parseConfig),
		wrapper.ProcessRequestHeaders(onHttpRequestHeaders),
		wrapper.ProcessRequestBody(onHttpRequestBody),
		wrapper.ProcessResponseHeaders(onHttpResponseHeaders),
		wrapper.ProcessStreamDone(onHttpStreamDone),
	)
}

func onHttpRequestHeaders(ctx wrapper.HttpContext, config Config) types.Action {
	// The plugin rewrites :path; it must not move the request to another route.
	ctx.DisableReroute()

	state, found, _ := loadState()
	if !ctx.HasRequestBody() {
		// Any state means this is a replayed pass, even if the state cannot be
		// decoded; state is only written for requests that had a body.
		if found {
			// A replayed pass must carry the original body (custom_response
			// use_original_request_body + the buffer filter). Never send a
			// body-less POST upstream.
			config.log.errorf(reasonReplayNoBody, "[ai-backend-router] replayed pass %d of model %q arrived without a body; returning the previous upstream status %d", state.Hop+1, state.Model, state.LastStatus)
			config.metrics.fallback(reasonReplayNoBody)
			config.metrics.request(state.Model, resultNoBody)
			status := state.LastStatus
			if status == 0 {
				status = http.StatusBadGateway
			}
			replyError(status, "replay_without_body", "the request body is not available to retry the request on another backend")
			return types.ActionPause
		}
		ctx.DontReadRequestBody()
		return types.ActionContinue
	}
	if !isJSON(ctx) {
		ctx.DontReadRequestBody()
		return types.ActionContinue
	}
	ctx.SetRequestBodyBufferLimit(config.maxRequestBodyBytes)
	return types.HeaderStopIteration
}

func isJSON(ctx wrapper.HttpContext) bool {
	contentType, _ := proxywasm.GetHttpRequestHeader("content-type")
	contentType = strings.ToLower(contentType)
	return contentType == "" || strings.Contains(contentType, "json")
}

func onHttpRequestBody(ctx wrapper.HttpContext, config Config, body []byte) types.Action {
	if duplicate, err := jsonbody.DuplicateTopLevelMember(body, "model"); err == nil && duplicate {
		config.metrics.request("", resultInvalid)
		replyError(http.StatusBadRequest, "duplicate_model_member", jsonbody.DuplicateMemberMessage("model"))
		return types.ActionPause
	}
	modelField := gjson.GetBytes(body, "model")
	if modelField.Type != gjson.String || modelField.String() == "" {
		config.metrics.request("", resultInvalid)
		replyError(http.StatusBadRequest, "invalid_request", "the request body must be JSON with a non-empty string \"model\"")
		return types.ActionPause
	}
	snapshot := config.store.snapshot()
	model := snapshot.Model(modelField.String())
	if model == nil {
		// The name comes from the client: never put it in a metric name.
		config.metrics.request(unknownModelLabel, resultUnknownModel)
		replyError(http.StatusNotFound, "model_not_found", "model \""+modelField.String()+"\" is not served by this gateway")
		return types.ActionPause
	}

	previous, found, err := loadState()
	p := &pick{model: model, spillable: config.spillEnabled}
	state := routerState{Model: model.Name, DecisionsVersion: snapshot.Version}
	switch {
	case err != nil && !errors.Is(err, errStateCorrupt):
		// The property could not be read at all; treat as a first pass but
		// do not spill, since the next pass could not read it either.
		config.log.errorf(reasonStateCorrupt, "[ai-backend-router] cannot read %s: %v; routing to the default backend without spillover", stateProperty, err)
		p.fallback = reasonStateCorrupt
	case err != nil:
		config.log.errorf(reasonStateCorrupt, "[ai-backend-router] %v; routing to the default backend without spillover", err)
		p.fallback = reasonStateCorrupt
	case found:
		state.Hop = previous.Hop + 1
		state.Tried = append(state.Tried, previous.Tried...)
		state.LastStatus = previous.LastStatus
	}

	if p.fallback == "" {
		backend, ok := selection.Select(model, state.Tried, config.exclude, config.picker, selection.Request{Model: model.Name, Hop: state.Hop})
		switch {
		case ok:
			p.backend = backend
		case found:
			// The previous pass saw a candidate, but the decisions changed in
			// between and left none. Answer like an exhausted request.
			config.log.errorf(reasonNoCandidateHop, "[ai-backend-router] model %q has no candidate left on pass %d after decisions changed; returning the previous upstream status %d", model.Name, state.Hop, state.LastStatus)
			config.metrics.fallback(reasonNoCandidateHop)
			config.metrics.request(model.Name, resultExhausted)
			replyError(nonZero(state.LastStatus, http.StatusServiceUnavailable), "no_backend_left", "no backend is left to retry the request")
			return types.ActionPause
		default:
			p.fallback = reasonNoCandidate
			config.log.errorf(reasonNoCandidate, "[ai-backend-router] model %q has no enabled backend with weight > 0; routing to its default backend %q", model.Name, model.Default.ID)
		}
	}
	if p.fallback != "" {
		p.backend = model.Default
		p.spillable = false
		config.metrics.fallback(p.fallback)
	}

	// Replacing the body with the same bytes makes Envoy back up the
	// original body for custom_response's use_original_request_body, when
	// no earlier filter replaced it. Without a backup, a spill would replay
	// the request without its body.
	if p.spillable {
		if err := proxywasm.ReplaceHttpRequestBody(body); err != nil {
			config.log.errorf("body_backup", "[ai-backend-router] cannot back up the request body, spillover is off for this request: %v", err)
			p.spillable = false
		}
	}

	path, err := proxywasm.GetHttpRequestHeader(":path")
	if err != nil || !strings.HasPrefix(path, "/") {
		config.log.errorf("path", "[ai-backend-router] request has no usable :path (%q): %v", path, err)
		replyError(http.StatusBadRequest, "invalid_path", "the request path is not usable")
		return types.ActionPause
	}
	if original, _ := proxywasm.GetHttpRequestHeader(originalPathHeader); original == "" {
		_ = proxywasm.ReplaceHttpRequestHeader(originalPathHeader, path)
	}
	if err := proxywasm.ReplaceHttpRequestHeader(":path", p.backend.Path+path); err != nil {
		config.log.errorf("path", "[ai-backend-router] cannot rewrite :path: %v", err)
	}
	_ = proxywasm.ReplaceHttpRequestHeader(backendIDHeader, p.backend.ID)

	state.Tried = append(state.Tried, p.backend.ID)
	if err := saveState(state); err != nil {
		config.log.errorf(reasonStateUnwritable, "[ai-backend-router] cannot write %s, spillover is off for this request: %v", stateProperty, err)
		p.spillable = false
	}
	p.state = state
	ctx.SetContext(pickContextKey, p)
	config.metrics.pick(model.Name, p.backend.ID, p.backend.Tier)
	return types.ActionContinue
}

func onHttpResponseHeaders(ctx wrapper.HttpContext, config Config) types.Action {
	// custom_response redirects on the marker alone, so only this plugin may
	// set it: drop any copy that came from upstream, on every response.
	if marker, err := proxywasm.GetHttpResponseHeader(spillHeader); err == nil && marker != "" {
		_ = proxywasm.RemoveHttpResponseHeader(spillHeader)
		config.log.errorf("upstream_marker", "[ai-backend-router] removed a %s header sent by the upstream", spillHeader)
	}
	p, _ := ctx.GetContext(pickContextKey).(*pick)
	if p == nil {
		return types.ActionContinue
	}
	statusText, _ := proxywasm.GetHttpResponseHeader(":status")
	status, _ := strconv.Atoi(statusText)
	if !config.retryCodes[status] {
		return types.ActionContinue
	}
	details, _ := proxywasm.GetProperty([]string{"response", "code_details"})
	if !fromBackend(string(details)) {
		// Local replies (rate limiters, auth, Envoy timeouts) never spill.
		return types.ActionContinue
	}
	if !p.spillable {
		p.exhausted = true
		return types.ActionContinue
	}
	if !selection.HasCandidate(p.model, p.state.Tried, config.exclude) {
		p.exhausted = true
		return types.ActionContinue
	}
	p.state.LastStatus = status
	if err := saveState(p.state); err != nil {
		config.log.errorf(reasonStateUnwritable, "[ai-backend-router] cannot record the spilled status, not spilling: %v", err)
		p.exhausted = true
		return types.ActionContinue
	}
	if err := proxywasm.ReplaceHttpResponseHeader(spillHeader, "1"); err != nil {
		config.log.errorf("spill_marker", "[ai-backend-router] cannot set %s, not spilling: %v", spillHeader, err)
		p.exhausted = true
		return types.ActionContinue
	}
	p.spilled = true
	config.metrics.spill(p.model.Name, p.backend.ID, status)
	return types.ActionContinue
}

// fromBackend reports whether a response came from the backend side: an
// upstream answer, or the upstream connection failing before any response.
func fromBackend(details string) bool {
	return details == "via_upstream" || strings.HasPrefix(details, "upstream_reset_before_response_started")
}

func onHttpStreamDone(ctx wrapper.HttpContext, config Config) {
	p, _ := ctx.GetContext(pickContextKey).(*pick)
	if p == nil || p.spilled {
		// Spilled passes are counted by spills_total; the final pass counts
		// the request.
		return
	}
	result := resultServed
	switch {
	case p.fallback != "":
		result = resultFallback
	case p.exhausted:
		result = resultExhausted
	}
	config.metrics.request(p.model.Name, result)
}

func replyError(status int, code, message string) {
	body, _ := json.Marshal(map[string]any{"error": map[string]string{
		"message": message,
		"type":    pluginName,
		"code":    code,
	}})
	headers := [][2]string{{"content-type", "application/json"}, {errorHeader, code}}
	if err := proxywasm.SendHttpResponseWithDetail(uint32(status), "ai_backend_router."+code, headers, body, -1); err != nil {
		log.Errorf("[ai-backend-router] cannot send local reply %d %s: %v", status, code, err)
	}
}

func nonZero(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

// rateLimitedLog writes at most one error line per reason per second and
// reports how many it suppressed.
type rateLimitedLog struct {
	now        func() time.Time
	last       map[string]int64
	suppressed map[string]int
}

func newRateLimitedLog(now func() time.Time) *rateLimitedLog {
	return &rateLimitedLog{now: now, last: map[string]int64{}, suppressed: map[string]int{}}
}

func (l *rateLimitedLog) errorf(reason, format string, args ...any) {
	now := l.now().UnixMilli()
	if now-l.last[reason] < 1000 {
		l.suppressed[reason]++
		return
	}
	l.last[reason] = now
	if n := l.suppressed[reason]; n > 0 {
		l.suppressed[reason] = 0
		log.Errorf("[ai-backend-router] %d more %q errors suppressed in the last second", n, reason)
	}
	log.Errorf(format, args...)
}
