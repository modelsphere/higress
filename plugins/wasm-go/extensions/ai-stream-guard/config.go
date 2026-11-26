package main

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/tidwall/gjson"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-stream-guard/detector"
	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-stream-guard/detector/repetition"
	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-stream-guard/stream"
)

const defaultFinishReason = "content_filter"

type pluginConfig struct {
	detectors []detector.Detector
	cutoff    stream.Cutoff
	// counters caches defined metrics; shared by the global config and the
	// rules derived from it.
	counters map[string]proxywasm.MetricCounter
}

// detectorFactory builds one detector from its config entry. allowedKeys are
// the type-specific keys; common keys (type, name, action) are always allowed.
type detectorFactory struct {
	allowedKeys []string
	build       func(name string, action detector.Action, entry gjson.Result) (detector.Detector, error)
}

var commonDetectorKeys = []string{"type", "name", "action"}

var detectorFactories = map[string]detectorFactory{
	"repetition": {
		allowedKeys: []string{"max_period", "min_repeated_chunks", "fields"},
		build:       buildRepetition,
	},
}

var allowedActions = []detector.Action{detector.ActionCut}

func parseGlobalConfig(json gjson.Result, cfg *pluginConfig) error {
	return parseConfigOver(json, pluginConfig{}, cfg)
}

// parseRuleConfig starts from the global config: a rule that sets
// `detectors` replaces the whole list; a rule that sets `cutoff` overrides
// only the cutoff keys it names.
func parseRuleConfig(json gjson.Result, global pluginConfig, cfg *pluginConfig) error {
	return parseConfigOver(json, global, cfg)
}

func parseConfigOver(json gjson.Result, base pluginConfig, cfg *pluginConfig) error {
	out := base
	if out.counters == nil {
		out.counters = map[string]proxywasm.MetricCounter{}
	}
	if out.cutoff.FinishReason == "" {
		out.cutoff.FinishReason = defaultFinishReason
	}
	if d := json.Get("detectors"); d.Exists() {
		dets, err := parseDetectors(d)
		if err != nil {
			return err
		}
		out.detectors = dets
	}
	if c := json.Get("cutoff"); c.Exists() {
		if err := parseCutoff(c, &out.cutoff); err != nil {
			return err
		}
	}
	if len(out.detectors) == 0 {
		return errors.New("detectors: must be a non-empty list")
	}
	*cfg = out
	return nil
}

func parseDetectors(list gjson.Result) ([]detector.Detector, error) {
	if !list.IsArray() {
		return nil, errors.New("detectors: must be a list")
	}
	entries := list.Array()
	if len(entries) == 0 {
		return nil, errors.New("detectors: must be a non-empty list")
	}
	names := map[string]bool{}
	var out []detector.Detector
	for i, entry := range entries {
		d, err := parseDetector(entry)
		if err != nil {
			return nil, fmt.Errorf("detectors[%d]: %w", i, err)
		}
		if names[d.Name()] {
			return nil, fmt.Errorf("detectors[%d]: duplicate name %q", i, d.Name())
		}
		names[d.Name()] = true
		out = append(out, d)
	}
	return out, nil
}

func parseDetector(entry gjson.Result) (detector.Detector, error) {
	if !entry.IsObject() {
		return nil, errors.New("must be an object")
	}
	typ, err := requiredString(entry, "type")
	if err != nil {
		return nil, err
	}
	factory, ok := detectorFactories[typ]
	if !ok {
		return nil, fmt.Errorf("unknown type %q", typ)
	}
	var unknownErr error
	entry.ForEach(func(k, _ gjson.Result) bool {
		key := k.String()
		if !slices.Contains(commonDetectorKeys, key) && !slices.Contains(factory.allowedKeys, key) {
			unknownErr = fmt.Errorf("unknown key %q for type %q", key, typ)
			return false
		}
		return true
	})
	if unknownErr != nil {
		return nil, unknownErr
	}
	name, err := optionalString(entry, "name", typ)
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, errors.New("name: must not be empty")
	}
	actionStr, err := optionalString(entry, "action", string(detector.ActionCut))
	if err != nil {
		return nil, err
	}
	action := detector.Action(actionStr)
	if !slices.Contains(allowedActions, action) {
		return nil, fmt.Errorf("action: unsupported value %q", actionStr)
	}
	return factory.build(name, action, entry)
}

func buildRepetition(name string, action detector.Action, entry gjson.Result) (detector.Detector, error) {
	maxPeriod, err := optionalInt(entry, "max_period", repetition.DefaultMaxPeriod)
	if err != nil {
		return nil, err
	}
	minChunks, err := optionalInt(entry, "min_repeated_chunks", repetition.DefaultMinRepeatedChunks)
	if err != nil {
		return nil, err
	}
	fields := detector.NewFieldSet(detector.FieldContent, detector.FieldReasoning)
	if f := entry.Get("fields"); f.Exists() {
		if fields, err = parseFields(f); err != nil {
			return nil, err
		}
	}
	return repetition.New(name, action, repetition.Config{
		MaxPeriod:         maxPeriod,
		MinRepeatedChunks: minChunks,
		Fields:            fields,
	})
}

func parseFields(f gjson.Result) (detector.FieldSet, error) {
	if !f.IsArray() {
		return 0, errors.New("fields: must be a list")
	}
	var set detector.FieldSet
	for _, v := range f.Array() {
		if v.Type != gjson.String {
			return 0, fmt.Errorf("fields: entries must be strings, got %s", v.Raw)
		}
		switch v.String() {
		case "content":
			set |= detector.NewFieldSet(detector.FieldContent)
		case "reasoning", "reasoning_content":
			set |= detector.NewFieldSet(detector.FieldReasoning)
		default:
			return 0, fmt.Errorf("fields: unknown field %q (want content, reasoning or reasoning_content)", v.String())
		}
	}
	if set == 0 {
		return 0, errors.New("fields: must not be empty")
	}
	return set, nil
}

func parseCutoff(c gjson.Result, out *stream.Cutoff) error {
	if !c.IsObject() {
		return errors.New("cutoff: must be an object")
	}
	fr, err := optionalString(c, "finish_reason", out.FinishReason)
	if err != nil {
		return fmt.Errorf("cutoff.%w", err)
	}
	if !slices.Contains(stream.FinishReasons, fr) {
		return fmt.Errorf("cutoff.finish_reason: unsupported value %q (want one of %v)", fr, stream.FinishReasons)
	}
	msg, err := optionalString(c, "message", out.Message)
	if err != nil {
		return fmt.Errorf("cutoff.%w", err)
	}
	out.FinishReason, out.Message = fr, msg
	return nil
}

func requiredString(obj gjson.Result, key string) (string, error) {
	v := obj.Get(key)
	if !v.Exists() {
		return "", fmt.Errorf("%s: required", key)
	}
	if v.Type != gjson.String {
		return "", fmt.Errorf("%s: must be a string", key)
	}
	return v.String(), nil
}

func optionalString(obj gjson.Result, key, def string) (string, error) {
	v := obj.Get(key)
	if !v.Exists() {
		return def, nil
	}
	if v.Type != gjson.String {
		return "", fmt.Errorf("%s: must be a string", key)
	}
	return v.String(), nil
}

func optionalInt(obj gjson.Result, key string, def int) (int, error) {
	v := obj.Get(key)
	if !v.Exists() {
		return def, nil
	}
	if v.Type != gjson.Number || v.Num != math.Trunc(v.Num) || math.Abs(v.Num) > math.MaxInt32 {
		return 0, fmt.Errorf("%s: must be an integer, got %s", key, v.Raw)
	}
	return int(v.Num), nil
}
