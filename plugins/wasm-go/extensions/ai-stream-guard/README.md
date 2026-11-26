---
title: AI Stream Guard
keywords: [ AI Gateway, streaming, repetition, loop detection ]
description: Detects anomalous streamed LLM output, such as degenerate repetition loops, and cuts the stream
---

## Function Description

`ai-stream-guard` inspects streamed (SSE) OpenAI-format LLM responses as they pass through the gateway. When a configured detector fires, the plugin **cuts** the stream: the client receives the events up to the trigger, then a terminal chunk with a configurable `finish_reason` and `data: [DONE]`. The rest of the upstream response is dropped and the event is logged and counted.

Every detection cuts the stream; there is no observe-only mode. Enabling the plugin on a route is the switch.

Detectors in this version:

- **repetition**: the model keeps emitting the same group of up to `max_period` SSE chunks. With the defaults (`max_period: 1`) this is LiteLLM's "same chunk N times in a row" check (`REPEATED_STREAMING_CHUNK_LIMIT`), generalized to periods up to 128.

## Running Attributes

Plugin execution phase: `Default Phase`
Plugin execution priority: `50`. Set both in the `WasmPlugin` resource; the plugin cannot enforce them. The priority must stay below `ai-proxy`'s `100`: on the response path the guard then sees the raw OpenAI stream from the upstream before `ai-proxy` converts it, and `/v1/messages` clients get the terminal chunk converted by `ai-proxy`.

## What Is Inspected

- Only responses with status `200`, `content-type: text/event-stream` and **no** `content-encoding` header. Everything else passes through untouched.
- Chat Completions chunks (`object: chat.completion.chunk`): `delta.content`, and `delta.reasoning` (or `delta.reasoning_content` when `reasoning` is empty).
- Completions chunks (`object: text_completion`): `text`.
- Chunks without text (role-only, tool calls, usage-only, finish chunks) are invisible: they neither count nor break a run.
- Each `choices[].index` has its own state, for indexes `0..15`. Higher indexes are not inspected.

## Configuration

| Name | Type | Requirement | Default | Description |
|---|---|---|---|---|
| `detectors` | array of object | Required | - | Detectors, evaluated in order for every chunk. Must not be empty |
| `cutoff` | object | Optional | - | What the client receives on a cut. Shared by all detectors |

`detectors[]` common fields:

| Name | Type | Requirement | Default | Description |
|---|---|---|---|---|
| `type` | string | Required | - | Detector type. Only `repetition` in this version |
| `name` | string | Optional | value of `type` | Unique name, used in logs and metrics |
| `action` | string | Optional | `cut` | Only `cut` in this version |

`repetition` fields:

| Name | Type | Requirement | Default | Description |
|---|---|---|---|---|
| `max_period` | int | Optional | `1` | Longest repeating group, in chunks, to detect. Every period from 1 to this value is checked. Range `1..128` |
| `min_repeated_chunks` | int | Optional | `512` | Fire when the repeating tail covers this many chunks, including the first occurrence of the group. Must be `>= 2 * max_period` |
| `fields` | array of string | Optional | `["content", "reasoning"]` | Which text fields are watched: `content`, `reasoning`, and `reasoning_content` (alias of `reasoning`) |

Unknown keys inside a `detectors[]` entry are rejected, so a typo cannot silently fall back to a default.

`cutoff` fields:

| Name | Type | Requirement | Default | Description |
|---|---|---|---|---|
| `finish_reason` | string | Optional | `content_filter` | `finish_reason` of the terminal chunk: `content_filter`, `length` or `stop` |
| `message` | string | Optional | `""` | If set, sent as one extra content (or `text`) chunk before the terminal chunk |

Rules (`_rules_`) inherit the global config: a rule that sets `detectors` replaces the whole list; a rule that sets `cutoff` overrides only the keys it names.

### How repetition is counted

For one choice, the detector keeps the last `max_period` text chunks (compared exactly by field and text) and fires when the last `min_repeated_chunks` chunks repeat a group of `p <= max_period` chunks. The smallest such `p` is reported.

- `max_period: 1, min_repeated_chunks: 512`: 512 identical chunks in a row.
- `max_period: 4, min_repeated_chunks: 512`: also `A B A B …` (p=2), `A B C A B C …` (p=3) and `A B C D …` (p=4), each covering 512 chunks.
- Whitespace and single-character chunks are **not** skipped: `\n` or `哈` repeated 512 times fires.

## What the Client Receives on a Cut

```
data: …events up to and including the one that triggered…

data: {"id":"chatcmpl-123","object":"chat.completion.chunk","created":1763520000,"model":"qwen3","choices":[{"index":0,"delta":{},"finish_reason":"content_filter"}]}

data: [DONE]

```

- One terminal chunk per choice that has not finished yet. `id`, `object`, `created` and `model` are copied from the last upstream chunk.
- **No usage chunk** is sent, and the gateway does not make one up. The log records `stream_guard_usage_missing: true`.
- Anthropic (`/v1/messages`) clients see `stop_reason: "content_filter"` after `ai-proxy`'s conversion. This is not a standard Anthropic value.
- **Known issue:** after a cut, `ai-proxy`'s Anthropic conversion emits `message_delta` **without a `usage` object** (no usage chunk reached it). The Anthropic TypeScript SDK's `MessageStream` reads `event.usage.output_tokens` and throws `Cannot read properties of undefined (reading 'output_tokens')`, so a cut stream surfaces as an error in such clients. OpenAI-protocol clients are not affected.

## Observability

Log attributes (written into `ai_log`) on a cut:

| Attribute | Meaning |
|---|---|
| `stream_guard_detector` | Detector `name` |
| `stream_guard_action` | `cut` |
| `stream_guard_choice` | Choice index that triggered |
| `stream_guard_period` | Period of the repeating group, in chunks |
| `stream_guard_repeats` | Repeating tail length / period |
| `stream_guard_tail_chunks` | Repeating tail length, in chunks |
| `stream_guard_chunks_seen` | Text chunks seen for that choice |
| `stream_guard_sample` | The repeating group (up to 64 bytes per chunk, 256 bytes total) |
| `stream_guard_usage_missing` | Always `true` on a cut |

When the guard stands aside, `stream_guard_bypass` records the first reason: `encoded`, `oversize_event` (an SSE event over 1 MiB; inspection stops for the rest of the stream), `too_many_choices` or `parse_error` (that event is skipped; inspection continues).

Counters:

```
route.<route>.upstream.<cluster>.model.<model>.detector.<name>.metric.stream_guard_cutoff
route.<route>.upstream.<cluster>.model.<model>.reason.<reason>.metric.stream_guard_bypass
```

Each cut also writes one `warn` log line with the detector, choice, period and sample.

## Configuration Examples

LiteLLM-style check, 512 identical chunks:

```yaml
detectors:
  - type: repetition
```

Strict on short loops, lenient on long ones:

```yaml
detectors:
  - {name: rep-short, type: repetition, max_period: 4,   min_repeated_chunks: 512}
  - {name: rep-long,  type: repetition, max_period: 128, min_repeated_chunks: 2048}
```

Watch content only and show a marker:

```yaml
detectors:
  - {type: repetition, max_period: 16, fields: [content]}
cutoff:
  finish_reason: length
  message: "\n\n[output stopped: repetition detected]"
```

## Limitations

- Detection works on chunk boundaries. Loops longer than `max_period` chunks, and near-repeats (incrementing numbers, small wording changes), are not detected.
- When the upstream merges deltas under load (vLLM does when its API server falls behind), chunk boundaries become irregular and detection may be late or missed. This never causes a false positive.
- With `n > 1`, one looping choice cuts the whole stream.
- Text the client has already received cannot be taken back.
