// ai-stream-guard inspects streamed (SSE) OpenAI-format LLM responses and cuts
// the stream when a configured detector fires. See README.md.
package main

import (
	"fmt"
	"strings"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/higress-group/wasm-go/pkg/log"
	"github.com/higress-group/wasm-go/pkg/wrapper"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-stream-guard/detector"
	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-stream-guard/stream"
)

const (
	pluginName = "ai-stream-guard"
	ctxGuard   = "ai-stream-guard.stream"

	// maxChoices bounds per-stream detector state; choices with a higher
	// index are not inspected.
	maxChoices = 16

	attrPrefix = "stream_guard_"
)

// Bypass reasons (log attribute stream_guard_bypass, metric label).
const (
	bypassEncoded        = "encoded"
	bypassOversizeEvent  = "oversize_event"
	bypassTooManyChoices = "too_many_choices"
	bypassParseError     = "parse_error"
)

func main() {}

func init() {
	wrapper.SetCtx(
		pluginName,
		wrapper.ParseOverrideConfig(parseGlobalConfig, parseRuleConfig),
		wrapper.ProcessResponseHeaders(onHttpResponseHeaders),
		wrapper.ProcessStreamingResponseBody(onHttpStreamingResponseBody),
	)
}

type mode uint8

const (
	modeInspecting mode = iota
	modeCut
	modeBypass
)

// guardStream is the per-response state.
type guardStream struct {
	framer   *stream.Framer
	meta     stream.Meta
	states   []detector.StreamState
	mode     mode
	bypassed map[string]bool
}

func newGuardStream(cfg pluginConfig) *guardStream {
	g := &guardStream{framer: stream.NewFramer(), bypassed: map[string]bool{}}
	for _, d := range cfg.detectors {
		g.states = append(g.states, d.NewStream())
	}
	return g
}

func onHttpResponseHeaders(ctx wrapper.HttpContext, cfg pluginConfig) types.Action {
	status, _ := proxywasm.GetHttpResponseHeader(":status")
	contentType, _ := proxywasm.GetHttpResponseHeader("content-type")
	if status != "200" || !isEventStream(contentType) {
		ctx.DontReadResponseBody()
		return types.ActionContinue
	}
	// Any content-encoding (even identity): the wrapper treats such bodies as
	// binary and never calls the body callback, so record why we stand aside.
	if enc, _ := proxywasm.GetHttpResponseHeader("content-encoding"); enc != "" {
		recordBypass(ctx, cfg, &stream.Meta{}, nil, bypassEncoded)
		ctx.DontReadResponseBody()
		return types.ActionContinue
	}
	// The guard may end the body early; a stale length would corrupt it.
	_ = proxywasm.RemoveHttpResponseHeader("content-length")
	ctx.SetContext(ctxGuard, newGuardStream(cfg))
	return types.ActionContinue
}

func isEventStream(contentType string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(contentType)), "text/event-stream")
}

func onHttpStreamingResponseBody(ctx wrapper.HttpContext, cfg pluginConfig, chunk []byte, endOfStream bool) []byte {
	g, _ := ctx.GetContext(ctxGuard).(*guardStream)
	if g == nil {
		return chunk
	}
	switch g.mode {
	case modeCut:
		return []byte{}
	case modeBypass:
		return chunk
	}

	events, overflow := g.framer.Feed(chunk)
	out := make([]byte, 0, len(chunk))
	for _, raw := range events {
		out = append(out, raw...)
		if v := g.inspect(ctx, cfg, raw); v != nil {
			return g.cut(ctx, cfg, v, out)
		}
	}
	if overflow {
		g.mode = modeBypass
		recordBypass(ctx, cfg, &g.meta, g.bypassed, bypassOversizeEvent)
		out = append(out, g.framer.Take()...)
	}
	if endOfStream {
		out = append(out, g.framer.Take()...)
	}
	return out
}

// inspect parses one raw event and runs it through the detectors.
func (g *guardStream) inspect(ctx wrapper.HttpContext, cfg pluginConfig, raw []byte) *detector.Verdict {
	res := stream.ParseEvent(raw, &g.meta)
	if res.ParseError {
		recordBypass(ctx, cfg, &g.meta, g.bypassed, bypassParseError)
	}
	for i := range res.Events {
		ev := &res.Events[i]
		if ev.Choice >= maxChoices {
			recordBypass(ctx, cfg, &g.meta, g.bypassed, bypassTooManyChoices)
			continue
		}
		for _, st := range g.states {
			if v := st.OnEvent(ev); v != nil {
				return v
			}
		}
	}
	return nil
}

// cut ends the stream: forwarded holds every event up to and including the
// one that fired. They are injected together with the terminal bytes in ONE
// call and the callback returns empty: mixing a returned prefix with an
// injected suffix could reorder bytes.
func (g *guardStream) cut(ctx wrapper.HttpContext, cfg pluginConfig, v *detector.Verdict, forwarded []byte) []byte {
	g.mode = modeCut
	g.framer.Take()
	payload := append(forwarded, stream.TerminalBytes(&g.meta, cfg.cutoff)...)
	reportCut(ctx, cfg, &g.meta, v)
	if err := proxywasm.InjectEncodedDataToFilterChain(payload, true); err != nil {
		// Fall back to replacing this chunk: the client still gets a clean
		// ending, but the stream stays open until upstream finishes.
		log.Warnf("%s: inject failed, returning terminal chunk inline: %v", pluginName, err)
		return payload
	}
	return []byte{}
}

func reportCut(ctx wrapper.HttpContext, cfg pluginConfig, meta *stream.Meta, v *detector.Verdict) {
	ctx.SetUserAttribute(attrPrefix+"detector", v.Detector)
	ctx.SetUserAttribute(attrPrefix+"action", string(v.Action))
	ctx.SetUserAttribute(attrPrefix+"choice", v.Choice)
	var details strings.Builder
	for _, a := range v.Attrs {
		ctx.SetUserAttribute(attrPrefix+a.Key, a.Value)
		if s, ok := a.Value.(string); ok {
			fmt.Fprintf(&details, " %s=%q", a.Key, s)
		} else {
			fmt.Fprintf(&details, " %s=%v", a.Key, a.Value)
		}
	}
	ctx.SetUserAttribute(attrPrefix+"usage_missing", true)
	_ = ctx.WriteUserAttributeToLogWithKey(wrapper.AILogKey)

	route, cluster := routeAndCluster()
	incrementCounter(cfg, fmt.Sprintf("route.%s.upstream.%s.model.%s.detector.%s.metric.stream_guard_cutoff",
		route, cluster, modelLabel(meta), v.Detector))
	log.Warnf("%s: cut stream detector=%s reason=%s choice=%d model=%s%s",
		pluginName, v.Detector, v.Reason, v.Choice, meta.Model, details.String())
}

// recordBypass counts a bypass reason once per stream (seen may be nil when
// there is no stream state yet) and records the first reason in ai_log.
func recordBypass(ctx wrapper.HttpContext, cfg pluginConfig, meta *stream.Meta, seen map[string]bool, reason string) {
	first := true
	if seen != nil {
		if seen[reason] {
			return
		}
		seen[reason] = true
		first = len(seen) == 1
	}
	if first {
		ctx.SetUserAttribute(attrPrefix+"bypass", reason)
		_ = ctx.WriteUserAttributeToLogWithKey(wrapper.AILogKey)
	}
	route, cluster := routeAndCluster()
	incrementCounter(cfg, fmt.Sprintf("route.%s.upstream.%s.model.%s.reason.%s.metric.stream_guard_bypass",
		route, cluster, modelLabel(meta), reason))
}

func routeAndCluster() (string, string) {
	route, cluster := "-", "-"
	if raw, err := proxywasm.GetProperty([]string{"route_name"}); err == nil && len(raw) > 0 {
		route = string(raw)
	}
	if raw, err := proxywasm.GetProperty([]string{"cluster_name"}); err == nil && len(raw) > 0 {
		cluster = string(raw)
	}
	return route, cluster
}

func modelLabel(meta *stream.Meta) string {
	if meta == nil || meta.Model == "" {
		return "-"
	}
	return meta.Model
}

func incrementCounter(cfg pluginConfig, name string) {
	c, ok := cfg.counters[name]
	if !ok {
		c = proxywasm.DefineCounterMetric(name)
		cfg.counters[name] = c
	}
	c.Increment(1)
}
