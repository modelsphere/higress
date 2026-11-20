package main

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-backend-router/decisions"
	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-backend-router/selection"
	"github.com/higress-group/wasm-go/pkg/log"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
)

const (
	defaultMaxRequestBodyBytes = 16 << 20
	maxRequestBodyBytesLimit   = 256 << 20
	defaultPullIntervalMs      = 5000
	defaultPullTimeoutMs       = 1000
	defaultMaxStaleMs          = 300000
)

var defaultRetryCodes = []int{429, 502, 503, 504}

// Config is one parsed plugin configuration (global or per rule).
type Config struct {
	retryCodes          map[int]bool
	exclude             selection.Exclude
	maxRequestBodyBytes uint32
	picker              selection.Picker
	store               *decisionStore
	metrics             *pluginMetrics
	log                 *rateLimitedLog
	// spillEnabled is false when the state property could not be declared
	// with the request lifespan. Spilling then stays off: without state that
	// survives the redirect, a spilled request would retry tier 0 forever.
	spillEnabled bool
}

func parseConfig(json gjson.Result, config *Config) error {
	if !json.IsObject() {
		return fmt.Errorf("config must be an object")
	}
	if err := parseRetry(json.Get("retry"), config); err != nil {
		return err
	}

	config.maxRequestBodyBytes = defaultMaxRequestBodyBytes
	if limits := json.Get("limits"); limits.Exists() {
		if !limits.IsObject() {
			return fmt.Errorf("limits must be an object")
		}
		if value := limits.Get("maxRequestBodyBytes"); value.Exists() {
			n, ok := intIn(value, 1, maxRequestBodyBytesLimit)
			if !ok {
				return fmt.Errorf("limits.maxRequestBodyBytes must be an integer in [1,%d]", maxRequestBodyBytesLimit)
			}
			config.maxRequestBodyBytes = uint32(n)
		}
	}

	decisionsJSON := json.Get("decisions")
	if !decisionsJSON.IsObject() {
		return fmt.Errorf("decisions must be an object")
	}
	staticJSON := decisionsJSON.Get("static")
	if !staticJSON.Exists() {
		return fmt.Errorf("decisions.static is required: it is the bootstrap table and the fallback when nothing has been pulled yet")
	}
	static, err := decisions.ParseResult(staticJSON)
	if err != nil {
		return fmt.Errorf("decisions.static: %v", err)
	}
	var source *sourceConfig
	if sourceJSON := decisionsJSON.Get("source"); sourceJSON.Exists() {
		source, err = parseSource(sourceJSON)
		if err != nil {
			return err
		}
	}

	config.metrics = newPluginMetrics()
	config.log = newRateLimitedLog(time.Now)
	config.picker = selection.WeightedRandom{Random: rand.New(rand.NewSource(time.Now().UnixNano()))}
	config.store = newDecisionStore(static, source, config.metrics)
	if source != nil {
		config.store.adopt()
		store := config.store
		wrapper.RegisterTickFunc(tickPeriodMs, store.tick)
	}

	if err := declareStateProperty(); err != nil {
		log.Errorf("[ai-backend-router] cannot declare %s with the request lifespan, spillover is disabled: %v", stateProperty, err)
		config.spillEnabled = false
	} else {
		config.spillEnabled = true
	}
	return nil
}

func parseRetry(retry gjson.Result, config *Config) error {
	config.retryCodes = map[int]bool{}
	for _, code := range defaultRetryCodes {
		config.retryCodes[code] = true
	}
	config.exclude = selection.ExcludeTier
	if !retry.Exists() {
		return nil
	}
	if !retry.IsObject() {
		return fmt.Errorf("retry must be an object")
	}
	if codes := retry.Get("codes"); codes.Exists() {
		if !codes.IsArray() || len(codes.Array()) == 0 {
			return fmt.Errorf("retry.codes must be a non-empty array of HTTP status codes")
		}
		config.retryCodes = map[int]bool{}
		for _, code := range codes.Array() {
			n, ok := intIn(code, 400, 599)
			if !ok {
				return fmt.Errorf("retry.codes: %s is not an HTTP status code in [400,599]", code.Raw)
			}
			config.retryCodes[int(n)] = true
		}
	}
	if exclude := retry.Get("exclude"); exclude.Exists() {
		mode, err := selection.ParseExclude(exclude.String())
		if exclude.Type != gjson.String || err != nil {
			return fmt.Errorf("retry.exclude must be %q or %q", selection.ExcludeTier, selection.ExcludeBackend)
		}
		config.exclude = mode
	}
	if attempts := retry.Get("maxAttempts"); attempts.Exists() {
		if n, ok := intIn(attempts, 0, 0); !ok || n != 0 {
			return fmt.Errorf("retry.maxAttempts is reserved: only 0 (unlimited) is supported")
		}
	}
	return nil
}

func parseSource(json gjson.Result) (*sourceConfig, error) {
	if !json.IsObject() {
		return nil, fmt.Errorf("decisions.source must be an object")
	}
	source := &sourceConfig{
		intervalMs: defaultPullIntervalMs,
		timeoutMs:  defaultPullTimeoutMs,
		maxStaleMs: defaultMaxStaleMs,
	}
	source.serviceName = strings.TrimSpace(json.Get("serviceName").String())
	if source.serviceName == "" {
		return nil, fmt.Errorf("decisions.source.serviceName is required (for example decision.default.svc.cluster.local)")
	}
	port, ok := intIn(json.Get("servicePort"), 1, 65535)
	if !ok {
		return nil, fmt.Errorf("decisions.source.servicePort must be an integer in [1,65535]")
	}
	source.servicePort = port
	source.host = json.Get("host").String()
	source.path = json.Get("path").String()
	if !strings.HasPrefix(source.path, "/") {
		return nil, fmt.Errorf("decisions.source.path must start with '/'")
	}
	if value := json.Get("intervalMs"); value.Exists() {
		n, ok := intIn(value, 1000, 600000)
		if !ok {
			return nil, fmt.Errorf("decisions.source.intervalMs must be an integer in [1000,600000]")
		}
		source.intervalMs = n
	}
	if value := json.Get("timeoutMs"); value.Exists() {
		n, ok := intIn(value, 50, source.intervalMs)
		if !ok {
			return nil, fmt.Errorf("decisions.source.timeoutMs must be an integer in [50,intervalMs]")
		}
		source.timeoutMs = n
	}
	if value := json.Get("maxStaleMs"); value.Exists() {
		n, ok := intIn(value, source.intervalMs, math.MaxInt32)
		if !ok {
			return nil, fmt.Errorf("decisions.source.maxStaleMs must be an integer >= intervalMs")
		}
		source.maxStaleMs = n
	}
	return source, nil
}

func intIn(value gjson.Result, lo, hi int64) (int64, bool) {
	if value.Type != gjson.Number {
		return 0, false
	}
	f := value.Float()
	if f != math.Trunc(f) || f < float64(lo) || f > float64(hi) {
		return 0, false
	}
	return int64(f), true
}
