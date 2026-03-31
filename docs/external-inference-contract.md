# External Inference Contract

`comet-node` exposes a controller-owned plane-scoped inference contract for external clients.
This is the supported external boundary for agent clients such as `maglev`.

Authoritative endpoints:

- `GET /api/v1/planes/<plane>/interaction/status`
- `GET /api/v1/planes/<plane>/interaction/models`
- `POST /api/v1/planes/<plane>/interaction/chat/completions`
- `POST /api/v1/planes/<plane>/interaction/chat/completions/stream`
- `GET /api/v1/planes/<plane>/skills`
- `POST /api/v1/planes/<plane>/skills`
- `GET /api/v1/planes/<plane>/skills/<skill_id>`
- `PUT /api/v1/planes/<plane>/skills/<skill_id>`
- `PATCH /api/v1/planes/<plane>/skills/<skill_id>`
- `DELETE /api/v1/planes/<plane>/skills/<skill_id>`

The raw plane gateway `/v1/*` is a runtime implementation detail. It is not the supported
external contract.

## Responsibility Split

`comet-node` owns:

- readiness and not-ready semantics
- request normalization and validation
- stable SSE and non-stream transport semantics
- sanitization, failure signaling, and request correlation
- plane/model/session metadata

`maglev` or another external client owns:

- planning and tool orchestration
- repository/file/git/deploy workflows
- retries above the contract layer
- JSON repair beyond the guarantees explicitly documented here

## Request Field Matrix

Supported fields for chat requests:

- `messages`
- `stream`
- `temperature`
- `top_p`
- `max_tokens`
- `preferred_language`
- `model`
- `session_id`
- `skill_ids`
- `auto_skills`

Controller-owned normalization:

- `model`
  - when absent, `comet-node` injects the plane's active served model
  - when present, it must match the active served model name or root model id
- `response_format`
  - supported only as `{"type":"json_object"}`
  - unsupported variants are rejected explicitly

Ignored or normalized internally:

- plane-level interaction/completion policy from desired state
- semantic completion instructions
- vLLM no-thinking transport shaping
- plane-scoped Skills resolution into a synthesized system instruction block

Explicitly unsupported and rejected:

- `tools`
- `tool_choice`
- `functions`
- `function_call`
- unsupported `response_format` variants, including tool/schema/function-oriented modes not
  guaranteed by `comet-node`

## Status Contract

`/interaction/status` is the source of truth for readiness.

Stable top-level fields:

- `ready`
- `degraded`
- `reason`
- `plane_name`
- `plane_mode`
- `active_model_id`
- `served_model_name`
- `completion_policy`
- `long_completion_policy`
- `request_id`
- `comet`

Current `reason` values are controller-owned enums. External clients should branch on `ready`,
`degraded`, and `reason`, not on logs.

Important reasons currently emitted include:

- `ready`
- `plane_mode_compute`
- `plane_not_running`
- `unsupported_local_runtime`
- `no_observation`
- `runtime_start_failed`
- `runtime_status_missing`
- `active_model_missing`
- `worker_group_partial`
- `gateway_not_ready`
- `distributed_bootstrap_pending`
- `inference_not_ready`
- `gateway_target_missing`

When Skills are enabled for the plane, status may also include:

- `skills_enabled`
- `skills_ready`
- `skills_container_name`

## Error Contract

Interaction endpoints return a machine-readable error envelope:

```json
{
  "status": "error",
  "request_id": "req-...",
  "reason": "plane_not_running",
  "plane_name": "qwen35-35b-a3b",
  "served_model_name": "qwen3.5-35b-a3b",
  "active_model_id": "Qwen/...",
  "error": {
    "code": "plane_not_ready",
    "message": "plane interaction target is not ready",
    "retryable": true
  },
  "comet": {
    "request_id": "req-...",
    "plane_name": "qwen35-35b-a3b",
    "served_model_name": "qwen3.5-35b-a3b",
    "active_model_id": "Qwen/..."
  }
}
```

HTTP behavior:

- `400` malformed request or unsupported field
- `404` unknown plane
- `409` interaction disabled, plane not ready, or model mismatch
- `422` malformed or truncated structured output
- `502` upstream invalid response or connection reset
- `503` upstream unavailable / degraded target unavailable
- `504` upstream timeout

Additional machine-readable interaction errors for Skills:

- `409 skills_disabled` when `session_id` or `skill_ids` are supplied for a plane without enabled Skills
- `409 skills_not_ready` when the dedicated `skills-<plane>` service is configured but not reachable yet
- `400 invalid_skill_reference` when `skill_ids` references an unknown or disabled skill

## Streaming Contract

SSE event order:

1. `session_started`
2. `segment_started`
3. `delta`
4. `segment_complete`
5. optional `continuation_started`
6. terminal `session_complete` or `session_failed`
7. compatibility `complete` or `error`
8. final `[DONE]`

Every SSE lifecycle event includes `request_id`. Session-scoped events also include `session_id`.
When Skills are active for a request, `session_started`, `session_complete`, and compatibility
terminal events also include `applied_skills[]`, `auto_applied_skills[]`,
`skill_resolution_mode`, and `skills_session_id`.

Terminal semantics:

- exactly one terminal session event per request
- non-stream and stream surface the same:
  - `completion_status`
  - `stop_reason`
  - `finish_reason`
  - `segment_count`
  - `continuation_count`

## Structured Output

Initial structured-output transport support is limited to:

```json
{
  "response_format": { "type": "json_object" }
}
```

Behavior:

- `comet-node` shapes the request for JSON-only output
- success is returned only when the final content parses as a JSON object
- truncated structured output returns `422 structured_output_truncated`
- malformed structured output returns `422 structured_output_malformed`

This phase does not provide:

- tool calling
- function calling
- arbitrary JSON schema enforcement

## Skills Contract

Canonical Skills content is stored in controller-owned `SkillsFactory`.
The dedicated `skills-<plane>` runtime remains a plane-local materialized copy used during
request-time resolution.

Skill shape:

- `id`
- `name`
- `description`
- `content`
- `enabled`
- `session_ids[]`
- `comet_links[]`
- `created_at`
- `updated_at`

Resolution behavior for interaction requests:

- `skill_ids[]` are resolved first, in request order
- if `skill_ids[]` is absent or empty, `auto_skills` defaults to `true`
- when contextual resolution is enabled, only enabled plane-local skills already materialized in
  `skills-<plane>` are candidates
- `SkillsFactory` is not queried directly during interaction-time resolution
- enabled skills bound to `session_id` are appended next, ordered by `updated_at DESC`
- duplicates are removed by skill id
- each resolved skill is materialized by `comet-node` into a controller-owned system prompt block
- `comet_links[]` are stored as metadata in v1 and are not injected into the prompt

Response metadata when Skills are applied:

- top-level `applied_skills[]`
- top-level `auto_applied_skills[]`
- top-level `skill_resolution_mode`
- top-level `skills_session_id`
- mirrored `applied_skills[]`, `auto_applied_skills[]`, `skill_resolution_mode`, and
  `skills_session_id` inside `session`

Contextual resolution debugging:

- `POST /api/v1/planes/<plane>/skills/resolve-context`
  - accepts `prompt` or `messages[]`
  - returns `skill_resolution_mode`, `candidate_count`, `selected_skill_ids`, `selected_skills`
  - uses only enabled plane-local skills for the current plane

Plane selection and global catalog behavior for `SkillsFactory` are documented in
[skills-factory.md](./skills-factory.md).
