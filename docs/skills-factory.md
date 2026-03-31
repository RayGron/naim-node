# SkillsFactory

## Summary

`SkillsFactory` is the controller-owned canonical catalog for Skills in `comet-node`.

- Canonical skill content is stored once in controller SQLite.
- Plane `skills-<plane>` SQLite remains a runtime copy used by request-time resolution.
- Plane selection uses snapshot semantics through `desired-state.v2.skills.factory_skill_ids[]`.
- Plane-local binding metadata stays plane-scoped:
  - `enabled`
  - `session_ids[]`
  - `comet_links[]`

## Controller APIs

Global catalog:

- `GET /api/v1/skills-factory`
- `POST /api/v1/skills-factory`
- `GET /api/v1/skills-factory/<skill_id>`
- `PUT /api/v1/skills-factory/<skill_id>`
- `PATCH /api/v1/skills-factory/<skill_id>`
- `DELETE /api/v1/skills-factory/<skill_id>`

Plane-scoped catalog:

- `GET /api/v1/planes/<plane>/skills`
- `POST /api/v1/planes/<plane>/skills`
- `GET /api/v1/planes/<plane>/skills/<skill_id>`
- `PUT /api/v1/planes/<plane>/skills/<skill_id>`
- `PATCH /api/v1/planes/<plane>/skills/<skill_id>`
- `DELETE /api/v1/planes/<plane>/skills/<skill_id>`

Model Library:

- `GET /api/v1/model-library`
  - each model item now includes `skills_factory_worker`
- `POST /api/v1/model-library/skills-factory-worker`
  - body: `{ "path": "<absolute-model-path>" }`

## Desired State Contract

`desired-state.v2.skills` extends to:

```json
{
  "enabled": true,
  "factory_skill_ids": ["skill-alpha", "skill-beta"]
}
```

Rules:

- `factory_skill_ids[]` is valid only when `skills.enabled=true`
- items must be unique non-empty strings
- `Skills` remains `llm`-only

## Behavior

### SkillsFactory CRUD

- Create or update in `SkillsFactory` changes canonical content immediately.
- `GET /api/v1/skills-factory` returns:
  - `id`
  - `name`
  - `description`
  - `content`
  - `created_at`
  - `updated_at`
  - `plane_names[]`
  - `plane_count`

### Plane-scoped CRUD

- Plane create/edit for a skill writes canonical content through to `SkillsFactory`.
- The current plane is attached to the same `skill_id`.
- Plane delete is detach-only:
  - remove plane binding
  - remove the id from that plane’s `factory_skill_ids[]`
  - keep the canonical `SkillsFactory` record

### Factory delete

Deleting from `SkillsFactory` is global detach:

- remove the canonical record
- remove plane bindings for that `skill_id`
- remove the id from every plane’s `factory_skill_ids[]`
- best-effort delete the live runtime copy from ready planes
- offline or unready planes converge on the next reconciliation

### Runtime synchronization

`PlaneSkillRuntimeSyncService` materializes selected factory skills into plane-local runtime state:

- create/update/start reconciliation syncs selected skills into `skills-<plane>`
- deselection removes runtime copies no longer selected
- interaction-time resolution still reads only the plane-local runtime copy

### Contextual interaction-time activation

Contextual skill activation is plane-local only:

- interaction-time skill resolution reads only from the current plane's materialized
  `skills-<plane>` catalog/runtime
- `SkillsFactory` is never queried directly as a runtime source during interaction
- canonical records become interaction candidates only after they are attached to the plane and
  synchronized into that plane's local runtime/catalog

Interaction behavior:

- if `skill_ids[]` is present and non-empty, explicit selection takes precedence
- otherwise `auto_skills` defaults to `true`
- when `auto_skills=true`, `comet-node` may auto-select a bounded set of relevant enabled
  plane-local skills
- when `auto_skills=false`, contextual resolution is skipped

Debugging:

- `POST /api/v1/planes/<plane>/skills/resolve-context`
  - resolves contextual skills for a prompt or `messages[]`
  - returns selected ids, selected records, candidate count, and compact rationale
  - uses only plane-local enabled skills

## Operator UI

### Sidebar

The main operator sidebar now includes `Skills Factory`.

### Skills Factory page

The page supports:

- create, edit, and delete canonical skills
- visibility of:
  - `id`
  - `name`
  - `description`
  - `plane_names`
  - `plane_count`
- sorting by:
  - `name`
  - `plane_count`
- dynamic search across all visible fields

### Plane editor

When `Skills` is enabled for an `llm` plane:

- a `Skills Factory` selector table appears under `Features`
- selected records persist into `desired-state.v2.skills.factory_skill_ids[]`
- rollout or restart syncs those skills into the plane runtime copy

### Plane dashboard

Plane dashboard now exposes a compact Skills summary derived from plane-local state:

- `enabled`
- `enabled_count`
- `total_count`

The Web UI renders this as a dashboard summary card:

- disabled planes show `0` with `disabled`
- enabled planes show the enabled skill count with `enabled / X total`

The counter is derived from the plane-local attached skill set, not from the global
`SkillsFactory` catalog size.

### Models page

Models page supports a single `Skills Factory Worker` designation:

- set by the hardhat action button
- exactly one model can hold the designation at a time
- selecting a new model replaces the previous designation
- this flag is persisted and displayed in this stage only
- it has no runtime behavior yet
