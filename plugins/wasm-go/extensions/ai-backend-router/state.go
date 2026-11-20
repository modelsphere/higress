package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
)

// stateProperty names the per-request filter state that carries spill state
// across internal redirects. It is declared with the DownstreamRequest
// lifespan: only state at or above that lifespan survives recreateStream.
const stateProperty = "ai_backend_router_state"

const stateVersion = 1

// routerState is what one pass hands to the next pass of the same request.
// Clients cannot set filter state, so the tried set cannot be forged.
type routerState struct {
	V                int      `json:"v"`
	Model            string   `json:"model"`
	Tried            []string `json:"tried"`
	Hop              int      `json:"hop"`
	LastStatus       int      `json:"lastStatus,omitempty"`
	DecisionsVersion string   `json:"decisionsVersion,omitempty"`
}

var errStateCorrupt = errors.New("router state is unreadable")

func encodeState(state routerState) []byte {
	state.V = stateVersion
	raw, _ := json.Marshal(state)
	return raw
}

func decodeState(raw []byte) (routerState, error) {
	var state routerState
	if err := json.Unmarshal(raw, &state); err != nil {
		return routerState{}, fmt.Errorf("%w: %v", errStateCorrupt, err)
	}
	if state.V != stateVersion || state.Hop < 0 || len(state.Tried) == 0 {
		return routerState{}, fmt.Errorf("%w: version %d, hop %d, %d tried", errStateCorrupt, state.V, state.Hop, len(state.Tried))
	}
	return state, nil
}

// loadState returns the state left by an earlier pass. found is false on the
// first pass of a request.
func loadState() (state routerState, found bool, err error) {
	raw, err := proxywasm.GetProperty([]string{stateProperty})
	if err != nil {
		if errors.Is(err, types.ErrorStatusNotFound) {
			return routerState{}, false, nil
		}
		return routerState{}, false, err
	}
	if len(raw) == 0 {
		return routerState{}, false, nil
	}
	state, err = decodeState(raw)
	return state, true, err
}

func saveState(state routerState) error {
	return proxywasm.SetProperty([]string{stateProperty}, encodeState(state))
}

// declareStateArguments hand-encodes
// envoy.source.extensions.common.wasm.DeclarePropertyArguments
// {name = 1, type = Bytes (0, omitted), span = DownstreamRequest (5: 1)}.
func declareStateArguments() []byte {
	args := []byte{0x0a, byte(len(stateProperty))}
	args = append(args, stateProperty...)
	return append(args, 0x28, 0x01)
}

// declareStateProperty registers the property's lifespan with Envoy. Envoy
// answers BadArgument when the name is already declared in this root
// context, which happens when several rule configs are parsed; that is fine.
func declareStateProperty() error {
	_, err := proxywasm.CallForeignFunction("declare_property", declareStateArguments())
	if err == nil || errors.Is(err, types.ErrorStatusBadArgument) {
		return nil
	}
	return err
}
