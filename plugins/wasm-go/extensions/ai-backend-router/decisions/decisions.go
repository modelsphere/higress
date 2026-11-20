// Package decisions holds the model → backends table that ai-backend-router
// routes by, and parses and validates it.
//
// A Snapshot is immutable once parsed: a new table replaces the old one as a
// whole, so a request never sees a half-applied update.
package decisions

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/tidwall/gjson"
)

const (
	// DefaultWeight applies when a backend omits weight.
	DefaultWeight = 100
	// MaxWeight bounds a backend's weight.
	MaxWeight = 10000
)

var backendIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)

// Backend is one deployment behind openresty, addressed by a path prefix.
type Backend struct {
	ID      string
	Path    string
	Tier    int
	Weight  int
	Enabled bool
}

// Usable reports whether selection may pick the backend. A drained or
// disabled backend can still be a model's default.
func (b *Backend) Usable() bool {
	return b.Enabled && b.Weight > 0
}

// Model is one canonical model with its backends.
type Model struct {
	Name     string
	Default  *Backend
	Backends []*Backend
	// Tiers lists the distinct tier numbers in ascending order.
	Tiers []int
	byID  map[string]*Backend
}

// Backend returns the backend with the given id, or nil.
func (m *Model) Backend(id string) *Backend {
	return m.byID[id]
}

// Snapshot is a validated, immutable decisions table.
type Snapshot struct {
	Version string
	Models  map[string]*Model
}

// Model returns the model with the given canonical name, or nil.
func (s *Snapshot) Model(name string) *Model {
	if s == nil {
		return nil
	}
	return s.Models[name]
}

// Parse validates a decisions document. Any error rejects the whole document.
func Parse(raw []byte) (*Snapshot, error) {
	if !gjson.ValidBytes(raw) {
		return nil, fmt.Errorf("decisions: not valid JSON")
	}
	return ParseResult(gjson.ParseBytes(raw))
}

// ParseResult validates an already parsed decisions document.
func ParseResult(doc gjson.Result) (*Snapshot, error) {
	if !doc.IsObject() {
		return nil, fmt.Errorf("decisions: must be an object")
	}
	if err := onlyKeys(doc, "decisions", "version", "models", "extensions"); err != nil {
		return nil, err
	}
	version := doc.Get("version")
	if version.Type != gjson.String || strings.TrimSpace(version.String()) == "" {
		return nil, fmt.Errorf("decisions.version: required non-empty string")
	}
	models := doc.Get("models")
	if !models.IsObject() {
		return nil, fmt.Errorf("decisions.models: required object")
	}
	snapshot := &Snapshot{Version: version.String(), Models: map[string]*Model{}}
	var err error
	models.ForEach(func(key, value gjson.Result) bool {
		name := key.String()
		if strings.TrimSpace(name) == "" {
			err = fmt.Errorf("decisions.models: empty model name")
			return false
		}
		if _, dup := snapshot.Models[name]; dup {
			err = fmt.Errorf("decisions.models: duplicate model %q", name)
			return false
		}
		var model *Model
		model, err = parseModel(name, value)
		if err != nil {
			return false
		}
		snapshot.Models[name] = model
		return true
	})
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func parseModel(name string, value gjson.Result) (*Model, error) {
	where := fmt.Sprintf("decisions.models[%q]", name)
	if !value.IsObject() {
		return nil, fmt.Errorf("%s: must be an object", where)
	}
	if err := onlyKeys(value, where, "default", "backends"); err != nil {
		return nil, err
	}
	backends := value.Get("backends")
	if !backends.IsArray() || len(backends.Array()) == 0 {
		return nil, fmt.Errorf("%s.backends: required non-empty array", where)
	}
	model := &Model{Name: name, byID: map[string]*Backend{}}
	tiers := map[int]struct{}{}
	for i, item := range backends.Array() {
		backend, err := parseBackend(fmt.Sprintf("%s.backends[%d]", where, i), item)
		if err != nil {
			return nil, err
		}
		if _, dup := model.byID[backend.ID]; dup {
			return nil, fmt.Errorf("%s.backends[%d].id: duplicate id %q", where, i, backend.ID)
		}
		model.byID[backend.ID] = backend
		model.Backends = append(model.Backends, backend)
		tiers[backend.Tier] = struct{}{}
	}
	for tier := range tiers {
		model.Tiers = append(model.Tiers, tier)
	}
	sort.Ints(model.Tiers)
	def := value.Get("default")
	if def.Type != gjson.String || def.String() == "" {
		return nil, fmt.Errorf("%s.default: required backend id", where)
	}
	model.Default = model.byID[def.String()]
	if model.Default == nil {
		return nil, fmt.Errorf("%s.default: %q is not one of the model's backends", where, def.String())
	}
	return model, nil
}

func parseBackend(where string, item gjson.Result) (*Backend, error) {
	if !item.IsObject() {
		return nil, fmt.Errorf("%s: must be an object", where)
	}
	if err := onlyKeys(item, where, "id", "target", "tier", "weight", "enabled", "maxConcurrency"); err != nil {
		return nil, err
	}
	id := item.Get("id")
	if id.Type != gjson.String || !backendIDPattern.MatchString(id.String()) {
		return nil, fmt.Errorf("%s.id: must match %s", where, backendIDPattern.String())
	}
	backend := &Backend{ID: id.String(), Weight: DefaultWeight, Enabled: true}

	target := item.Get("target")
	if !target.IsObject() {
		return nil, fmt.Errorf("%s.target: required object", where)
	}
	if target.Get("cluster").Exists() {
		return nil, fmt.Errorf("%s.target.cluster: reserved; one cluster per backend is not supported yet", where)
	}
	if err := onlyKeys(target, where+".target", "path", "cluster"); err != nil {
		return nil, err
	}
	path := target.Get("path")
	if path.Type != gjson.String {
		return nil, fmt.Errorf("%s.target.path: required string", where)
	}
	if err := validatePathPrefix(path.String()); err != nil {
		return nil, fmt.Errorf("%s.target.path: %v", where, err)
	}
	backend.Path = path.String()

	tier := item.Get("tier")
	if !isNonNegativeInt(tier) {
		return nil, fmt.Errorf("%s.tier: required integer >= 0", where)
	}
	backend.Tier = int(tier.Int())

	if weight := item.Get("weight"); weight.Exists() {
		if !isNonNegativeInt(weight) || weight.Int() > MaxWeight {
			return nil, fmt.Errorf("%s.weight: must be an integer in [0,%d]", where, MaxWeight)
		}
		backend.Weight = int(weight.Int())
	}
	if enabled := item.Get("enabled"); enabled.Exists() {
		if enabled.Type != gjson.True && enabled.Type != gjson.False {
			return nil, fmt.Errorf("%s.enabled: must be a boolean", where)
		}
		backend.Enabled = enabled.Bool()
	}
	if limit := item.Get("maxConcurrency"); limit.Exists() {
		if !isNonNegativeInt(limit) {
			return nil, fmt.Errorf("%s.maxConcurrency: must be an integer >= 0", where)
		}
		if limit.Int() > 0 {
			return nil, fmt.Errorf("%s.maxConcurrency: reserved; per-backend limits are not enforced in this version, so only 0 is accepted", where)
		}
	}
	return backend, nil
}

// validatePathPrefix accepts "/seg[/seg...]" made of unreserved URL
// characters, so the prefix cannot carry a query, fragment, traversal or
// encoding that would change how openresty matches it.
func validatePathPrefix(path string) error {
	if !strings.HasPrefix(path, "/") || len(path) < 2 {
		return fmt.Errorf("must start with '/' and name at least one segment")
	}
	if strings.HasSuffix(path, "/") {
		return fmt.Errorf("must not end with '/'")
	}
	if len(path) > 256 {
		return fmt.Errorf("must be at most 256 bytes")
	}
	for _, segment := range strings.Split(path[1:], "/") {
		if segment == "" {
			return fmt.Errorf("must not contain '//'")
		}
		if segment == "." || segment == ".." {
			return fmt.Errorf("must not contain '.' or '..' segments")
		}
		for _, c := range segment {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~') {
				return fmt.Errorf("character %q is not allowed", c)
			}
		}
	}
	return nil
}

func isNonNegativeInt(value gjson.Result) bool {
	if value.Type != gjson.Number {
		return false
	}
	f := value.Float()
	return f >= 0 && f == float64(int64(f)) && f <= 1<<31
}

func onlyKeys(object gjson.Result, where string, allowed ...string) error {
	var err error
	object.ForEach(func(key, _ gjson.Result) bool {
		for _, name := range allowed {
			if key.String() == name {
				return true
			}
		}
		err = fmt.Errorf("%s: unknown field %q", where, key.String())
		return false
	})
	return err
}
