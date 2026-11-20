---
title: AI Backend Router
keywords: [ AI Gateway, load balancing, priority, failover, spillover ]
description: Routes LLM requests across a model's backend deployments by priority tier and weight, and moves a refused request on to the next tier
---

## Function Description

`ai-backend-router` picks, per request, which backend deployment serves an LLM call. It is built for backends that share one upstream (for example one openresty address) and are told apart by a path prefix:

```
http://openresty.example.com/qwen3.5-27b-h100-ep8/v1/chat/completions
http://openresty.example.com/qwen3.5-27b-h100-pp2/v1/chat/completions
http://openresty.example.com/qwen3.5-27b-b300/v1/chat/completions
```

For each request the plugin:

1. reads the canonical `model` from the JSON body (resolve aliases earlier, for example with `model-mapper`);
2. walks the model's **tiers** in ascending order and, in the first tier that still has candidates, picks a backend by **weighted random**;
3. rewrites `:path` to `<backend path prefix><request path>`. The route's upstream cluster stays the same.

**Spillover.** If the chosen backend answers with a retriable status (default 429, 502, 503, 504, or the connection is reset before any response), the plugin marks the response with `x-higress-spill: 1`. The gateway's `custom_response` filter turns that into an internal redirect: the **original** request (path and body) runs through the filter chain again, and the plugin skips what this request already tried (by default the whole tier). The client sees nothing of this. When no tier is left, the last upstream error goes to the client.

Spillover is per request: there is no memory of saturation, so the next request starts at the top tier again.

## Running Attributes

Plugin execution phase: `Default Phase`
Plugin execution priority: `90`. Set both in the `WasmPlugin` resource. The plugin must run **after** `model-mapper` and `ai-proxy` (priority 100): it reads the canonical model and prefixes the path that `ai-proxy` produced.

The plugin only works together with the gateway configuration in [Gateway setup](#gateway-setup). Without it, nothing spills; the plugin still routes by tier and weight.

## Configuration

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `decisions.static` | object | yes | — | Decisions table (see below). The bootstrap table, and the only one when there is no `source`. |
| `decisions.source` | object | no | — | Pull the table from a decision service (see below). A pulled table replaces the static one completely. |
| `retry.codes` | array of int | no | `[429, 502, 503, 504]` | Statuses in 400..599 that spill. Only answers from the backend count (`via_upstream`, or a connection reset before the response); local replies from other filters and Envoy timeouts never spill. |
| `retry.exclude` | string | no | `tier` | What a spilled pass removes for the rest of the request: `tier` (every backend of the tried backend's tier) or `backend` (only that backend). |
| `retry.maxAttempts` | int | no | `0` | Reserved. Only `0` (unlimited) is accepted; the number of tiers bounds the passes. |
| `limits.maxRequestBodyBytes` | int | no | `16777216` | Request body buffer limit, 1..268435456. Keep it equal to the `buffer` filter's `max_request_bytes`. |

### Decisions table

```json
{
  "version": "2026-11-20#42",
  "models": {
    "qwen3.5-27b": {
      "default": "h100-ep8",
      "backends": [
        {"id": "h100-ep8", "target": {"path": "/qwen3.5-27b-h100-ep8"}, "tier": 0, "weight": 70},
        {"id": "h100-pp2", "target": {"path": "/qwen3.5-27b-h100-pp2"}, "tier": 0, "weight": 30},
        {"id": "b300",     "target": {"path": "/qwen3.5-27b-b300"},     "tier": 1}
      ]
    }
  }
}
```

| Field | Description |
|---|---|
| `version` | Required non-empty string. Logged and recorded in the per-request state. |
| `models.<name>` | Key is the canonical model name, exactly as `model` arrives at the plugin. |
| `default` | Backend id used when the model has no usable backend (all disabled or weight 0) and on internal errors. Spillover is off for such a pass. |
| `backends[].id` | `[a-z0-9][a-z0-9._-]{0,62}`, unique per model. Used in metrics and in the tried set. |
| `backends[].target.path` | Path prefix: `/segment[/segment…]`, unreserved URL characters only, no trailing `/`. |
| `backends[].tier` | Integer ≥ 0; lower is preferred. |
| `backends[].weight` | Integer 0..10000, default 100. `0` drains the backend. |
| `backends[].enabled` | Boolean, default `true`. |
| `backends[].target.cluster`, `backends[].maxConcurrency` | Reserved. `target.cluster` and any `maxConcurrency` > 0 are rejected. |

Unknown fields are rejected (a top-level `extensions` object is allowed for the decision service's own data). An invalid table is rejected as a whole: as plugin config it fails the plugin start, as a pulled document it is ignored and the current table stays.

### Decision source

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `serviceName` | string | yes | — | FQDN of the decision service as a gateway cluster, for example `decision.default.svc.cluster.local` or a McpBridge `*.dns` / `*.static` name. The cluster `outbound|<port>||<serviceName>` must exist in the gateway. |
| `servicePort` | int | yes | — | Service port. |
| `path` | string | yes | — | Path of the `GET` that returns the decisions table. |
| `host` | string | no | `serviceName` | `Host` header of the pull. |
| `intervalMs` | int | no | `5000` | Pull interval, 1000..600000. |
| `timeoutMs` | int | no | `1000` | Pull timeout, 50..`intervalMs`. |
| `maxStaleMs` | int | no | `300000` | After this long without a successful pull the plugin logs an error once a minute. It keeps routing by the last good table. |

One VM per gateway process holds a pull lease and fetches the document (sending `If-None-Match` with the last `ETag`; `304` counts as success). It validates the document and stores it in shared data; every VM adopts a new table within about a second. A failed pull (network error, non-2xx, invalid document) keeps the current table.

### Example

```yaml
retry:
  codes: [429, 502, 503, 504]
  exclude: tier
decisions:
  static:
    version: bootstrap-1
    models:
      qwen3.5-27b:
        default: h100-ep8
        backends:
        - {id: h100-ep8, target: {path: /qwen3.5-27b-h100-ep8}, tier: 0, weight: 70}
        - {id: h100-pp2, target: {path: /qwen3.5-27b-h100-pp2}, tier: 0, weight: 30}
        - {id: b300, target: {path: /qwen3.5-27b-b300}, tier: 1}
  source:
    serviceName: decision.default.svc.cluster.local
    servicePort: 8080
    path: /v1/decisions
```

## Gateway setup

Spillover replays the original request through the filter chain. Two EnvoyFilters provide that; neither changes Higress code.

**1. Spill matcher on `custom_response`.** Higress already inserts `envoy.filters.http.custom_response` as the first HTTP filter. Merge a matcher that reacts only to the plugin's marker, so it is safe on every route:

```yaml
apiVersion: networking.istio.io/v1alpha3
kind: EnvoyFilter
metadata:
  name: ai-backend-router-spill
  namespace: higress-system
spec:
  configPatches:
  - applyTo: HTTP_FILTER
    match:
      context: GATEWAY
      listener:
        filterChain:
          filter:
            name: envoy.filters.network.http_connection_manager
            subFilter: {name: envoy.filters.http.custom_response}
    patch:
      operation: MERGE
      value:
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.http.custom_response.v3.CustomResponse
          custom_response_matcher:
            matcher_list:
              matchers:
              - predicate:
                  single_predicate:
                    input:
                      name: spill
                      typed_config:
                        "@type": type.googleapis.com/envoy.type.matcher.v3.HttpResponseHeaderMatchInput
                        header_name: x-higress-spill
                    value_match: {exact: "1"}
                on_match:
                  action:
                    name: ai-backend-router-spill
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.http.custom_response.redirect_policy.v3.RedirectPolicy
                      use_original_request_uri: true
                      use_original_request_body: true
                      keep_original_response_code: false
                      only_redirect_upstream_code: false
                      max_internal_redirects: 16
                      request_headers_to_add:
                      - header: {key: x-higress-fallback-from, value: ai-backend-router}
                        append_action: OVERWRITE_IF_EXISTS_OR_ADD
```

- `use_original_request_body: true` is required. Without it Envoy replays the body the router saw; when an earlier filter converted the body (for example `ai-proxy` turning Anthropic Messages into Chat Completions), that converted body is converted again and the request is silently changed.
- `keep_original_response_code: false` is required: the default (`true`) would give the client the first pass's status with the final pass's body.
- `x-higress-fallback-from` marks the replay. `ai-proxy` uses it to keep the client's original `Authorization` (in `X-HI-ORIGINAL-AUTH`) after it swapped in the upstream token, so authentication can run again on the replayed pass.

**2. `buffer` filter right after `custom_response`.** Envoy backs up the original body only when a wasm filter replaces a buffered body. A body that arrives in one frame (HTTP/2 clients, and every replayed pass) is otherwise not buffered, and the next pass would arrive without a body:

```yaml
  - applyTo: HTTP_FILTER
    match:
      context: GATEWAY
      listener:
        filterChain:
          filter:
            name: envoy.filters.network.http_connection_manager
            subFilter: {name: envoy.filters.http.custom_response}
    patch:
      operation: INSERT_AFTER
      value:
        name: envoy.filters.http.buffer
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.http.buffer.v3.Buffer
          max_request_bytes: 16777216
```

`max_request_bytes` is the largest request the gateway accepts (larger requests get 413). Each in-flight request holds its body until it completes.

**Routes.** Point every model's routes at the one upstream cluster. Do not use a route-level path rewrite (the plugin owns the path), and do not add a route retry policy on the spill statuses (Envoy would retry the same backend first).

## Observability

**Metrics** (Envoy exports them with the `wasmcustom.` prefix; labels are encoded in the name):

| Metric | Meaning |
|---|---|
| `ai_backend_router.model.<m>.backend.<b>.tier.<t>.picks_total` | Passes routed to a backend (including spilled ones) |
| `ai_backend_router.model.<m>.backend.<b>.status.<s>.spills_total` | Passes that spilled, by backend and status |
| `ai_backend_router.model.<m>.result.<r>.requests_total` | Client requests by result: `served`, `exhausted`, `fallback`, `unknown_model`, `invalid_request`, `replay_without_body` |
| `ai_backend_router.reason.<reason>.fallback_total` | Default-backend routing and other fallbacks |
| `ai_backend_router.decision_pull_failures_total` | Failed decision pulls |
| `ai_backend_router.decisions_age_ms` | Time since the last successful pull |

**Request header** `x-higress-backend-id` carries the backend of the pass to later filters, the access log (`%REQ(X-HIGRESS-BACKEND-ID)%`) and the upstream.

**Access log.** Each pass of a spilled request writes its own access-log line with the same `x-request-id`. Only the final pass's `%RESPONSE_CODE_DETAILS%` does not start with `internal_redirect:`; keep that line for billing.

**Local replies** from the plugin carry `x-higress-backend-router: <code>` and an OpenAI-style error body: `404 model_not_found`, `400 invalid_request`, `400 duplicate_model_member`, and, if a replayed pass arrives without its body, the previous upstream status with `replay_without_body`.

## Things to know

- **Every pass runs the whole filter chain.** Filters before this plugin run once per pass: authentication, `ai-proxy` conversion, and **request-count rate limiters, which count every spilled pass** (a request that spills once uses two tokens). `ai-statistics` also records each pass.
- Only answers before any response byte can spill. A stream that fails midway reaches the client.
- A request whose body exceeds the `buffer` filter limit is rejected with 413 rather than served without the ability to spill.
- If the state property cannot be declared with the request lifespan, the plugin logs an error at start and routes without spillover.
- Requests without a body (for example `GET /v1/models`) and non-JSON requests are not routed: they pass through with their path unchanged.
