package main

import (
	"fmt"
	"strings"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
)

// Request results, counted once per client request (on its final pass).
const (
	resultServed       = "served"
	resultExhausted    = "exhausted"
	resultFallback     = "fallback"
	resultUnknownModel = "unknown_model"
	resultInvalid      = "invalid_request"
	resultNoBody       = "replay_without_body"
)

// unknownModelLabel stands in for model names that are not in the decisions
// table: those come from clients and would make metric names unbounded.
const unknownModelLabel = "unknown"

// Fallback reasons.
const (
	reasonNoCandidate     = "no_candidate"
	reasonStateCorrupt    = "state_corrupt"
	reasonReplayNoBody    = "replay_without_body"
	reasonNoCandidateHop  = "no_candidate_after_spill"
	reasonStateUnwritable = "state_unwritable"
)

// Metric names carry their labels in the name, as ai-statistics does; the
// gateway's stats-tag rules (or dashboards) turn them into labels.
type pluginMetrics struct {
	counters map[string]proxywasm.MetricCounter
	gauges   map[string]proxywasm.MetricGauge
}

func newPluginMetrics() *pluginMetrics {
	return &pluginMetrics{counters: map[string]proxywasm.MetricCounter{}, gauges: map[string]proxywasm.MetricGauge{}}
}

func (m *pluginMetrics) counter(name string) proxywasm.MetricCounter {
	counter, ok := m.counters[name]
	if !ok {
		counter = proxywasm.DefineCounterMetric(name)
		m.counters[name] = counter
	}
	return counter
}

func (m *pluginMetrics) gauge(name string) proxywasm.MetricGauge {
	gauge, ok := m.gauges[name]
	if !ok {
		gauge = proxywasm.DefineGaugeMetric(name)
		m.gauges[name] = gauge
	}
	return gauge
}

func (m *pluginMetrics) pick(model, backend string, tier int) {
	m.counter(fmt.Sprintf("ai_backend_router.model.%s.backend.%s.tier.%d.picks_total", metricToken(model), metricToken(backend), tier)).Increment(1)
}

func (m *pluginMetrics) spill(model, backend string, status int) {
	m.counter(fmt.Sprintf("ai_backend_router.model.%s.backend.%s.status.%d.spills_total", metricToken(model), metricToken(backend), status)).Increment(1)
}

func (m *pluginMetrics) request(model, result string) {
	m.counter(fmt.Sprintf("ai_backend_router.model.%s.result.%s.requests_total", metricToken(model), result)).Increment(1)
}

func (m *pluginMetrics) fallback(reason string) {
	m.counter(fmt.Sprintf("ai_backend_router.reason.%s.fallback_total", reason)).Increment(1)
}

func (m *pluginMetrics) pullFailure() {
	m.counter("ai_backend_router.decision_pull_failures_total").Increment(1)
}

func (m *pluginMetrics) decisionsAge(ms int64) {
	gauge := m.gauge("ai_backend_router.decisions_age_ms")
	gauge.Add(max(ms, 0) - gauge.Value())
}

// metricToken keeps a model or backend name from adding name segments.
func metricToken(value string) string {
	if value == "" {
		return "unknown"
	}
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, value)
}
