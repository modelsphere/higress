// Package selection picks the backend for one pass of a request: the first
// tier that still has candidates, then one backend of that tier chosen by the
// Picker.
package selection

import (
	"fmt"
	"math"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/ai-backend-router/decisions"
)

// Exclude says what a retriable failure removes from the rest of the request.
type Exclude string

const (
	// ExcludeTier skips every backend in the tier of a backend already tried.
	ExcludeTier Exclude = "tier"
	// ExcludeBackend skips only the backends already tried.
	ExcludeBackend Exclude = "backend"
)

// ParseExclude validates an exclusion mode.
func ParseExclude(value string) (Exclude, error) {
	switch Exclude(value) {
	case ExcludeTier, ExcludeBackend:
		return Exclude(value), nil
	}
	return "", fmt.Errorf("must be %q or %q", ExcludeTier, ExcludeBackend)
}

// Request carries what a Picker may use. Fields beyond Model and Hop are
// reserved for cache- or session-aware pickers.
type Request struct {
	Model string
	Hop   int
}

// Picker orders a tier's candidates by preference. It must not filter:
// exclusion is the caller's job. The slice it returns may be the input,
// reordered.
type Picker interface {
	Order(req Request, tier []*decisions.Backend) []*decisions.Backend
}

// RandomSource is the subset of math/rand used by WeightedRandom.
type RandomSource interface {
	Float64() float64
}

// WeightedRandom orders candidates by a weighted random permutation
// (Efraimidis–Spirakis: key = u^(1/w), largest first). The first element is a
// plain weighted pick.
type WeightedRandom struct {
	Random RandomSource
}

// Order implements Picker.
func (p WeightedRandom) Order(_ Request, tier []*decisions.Backend) []*decisions.Backend {
	type keyed struct {
		backend *decisions.Backend
		key     float64
	}
	items := make([]keyed, 0, len(tier))
	for _, backend := range tier {
		u := p.Random.Float64()
		for u == 0 {
			u = p.Random.Float64()
		}
		items = append(items, keyed{backend, math.Pow(u, 1/float64(backend.Weight))})
	}
	// Insertion sort: tiers hold a handful of backends.
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j].key > items[j-1].key; j-- {
			items[j], items[j-1] = items[j-1], items[j]
		}
	}
	ordered := make([]*decisions.Backend, len(items))
	for i, item := range items {
		ordered[i] = item.backend
	}
	return ordered
}

// Candidates groups the usable, not excluded backends of a model by tier, in
// ascending tier order. Empty tiers are omitted.
func Candidates(model *decisions.Model, tried []string, exclude Exclude) [][]*decisions.Backend {
	triedIDs := make(map[string]struct{}, len(tried))
	triedTiers := map[int]struct{}{}
	for _, id := range tried {
		triedIDs[id] = struct{}{}
		if backend := model.Backend(id); backend != nil {
			triedTiers[backend.Tier] = struct{}{}
		}
	}
	var result [][]*decisions.Backend
	for _, tier := range model.Tiers {
		if _, skip := triedTiers[tier]; skip && exclude == ExcludeTier {
			continue
		}
		var members []*decisions.Backend
		for _, backend := range model.Backends {
			if backend.Tier != tier || !backend.Usable() {
				continue
			}
			if _, done := triedIDs[backend.ID]; done {
				continue
			}
			members = append(members, backend)
		}
		if len(members) > 0 {
			result = append(result, members)
		}
	}
	return result
}

// Select returns the backend for this pass, or false when the request has
// no candidate left.
func Select(model *decisions.Model, tried []string, exclude Exclude, picker Picker, req Request) (*decisions.Backend, bool) {
	tiers := Candidates(model, tried, exclude)
	if len(tiers) == 0 {
		return nil, false
	}
	ordered := picker.Order(req, tiers[0])
	if len(ordered) == 0 {
		return nil, false
	}
	return ordered[0], true
}

// HasCandidate reports whether another pass could still pick a backend.
func HasCandidate(model *decisions.Model, tried []string, exclude Exclude) bool {
	return len(Candidates(model, tried, exclude)) > 0
}
