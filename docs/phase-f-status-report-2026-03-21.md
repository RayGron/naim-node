# Phase F Status Report

Date: 2026-03-21

## Scope Reference

GitHub issue: `#6` `Phase F: REST API and operator CLI compatibility`

Phase F goal:

- expose current controller workflows through a stable REST API
- begin restoring an operator-facing compatibility CLI that talks to the controller over HTTP
- keep orchestration logic in native controller code, not in shell wrappers

## Executive Summary

Phase F is now `in progress`.

The current `comet` controller has moved beyond a CLI-only surface: the first HTTP seam now exists
inside `comet-controller` itself, with a native listener, read-only JSON inspection endpoints, and
the first safe mutating orchestration endpoints. A thin operator CLI layer now also exists on top
of that HTTP surface through `--controller`, `COMET_CONTROLLER`, and config-based target
resolution. The broader API hardening work is still ahead, but the phase is now materially
underway in code.

Implemented in this first slice:

- native controller HTTP listener (`serve`)
- configurable `--listen-host` and `--listen-port`
- JSON health endpoints:
  - `GET /health`
  - `GET /api/v1/health`
- first read-only controller endpoints:
  - `GET /api/v1/state`
  - `GET /api/v1/host-assignments`
  - `GET /api/v1/host-observations`
  - `GET /api/v1/host-health`
  - `GET /api/v1/disk-state`
  - `GET /api/v1/rollout-actions`
  - `GET /api/v1/rebalance-plan`
- first safe mutating controller endpoints:
  - `POST /api/v1/bundles/validate`
  - `POST /api/v1/bundles/preview`
  - `POST /api/v1/bundles/import`
  - `POST /api/v1/bundles/apply`
  - `POST /api/v1/scheduler-tick`
  - `POST /api/v1/reconcile-rebalance-proposals`
  - `POST /api/v1/reconcile-rollout-actions`
  - `POST /api/v1/apply-rebalance-proposal`
  - `POST /api/v1/set-rollout-action-status`
  - `POST /api/v1/enqueue-rollout-eviction`
  - `POST /api/v1/apply-ready-rollout-action`
  - `POST /api/v1/node-availability`
  - `POST /api/v1/retry-host-assignment`
- thin operator CLI over HTTP:
  - `--controller <http://host:port>`
  - `COMET_CONTROLLER`
  - `~/.config/comet/controller`
- API response hardening:
  - `api_version` metadata
  - `request.path` and `request.method` metadata
  - normalized error envelope via `status=error` and `error.code` / `error.message`

Still missing for the full phase:

- API auth / hardening
- richer request / response contracts

## What Is Already Implemented

### 1. Controller workflows already exist as native commands

Implemented:

- bundle validation, preview, plan, import, apply
- desired state inspection
- host assignments / retry
- host observations / health
- rollout / scheduler control
- disk-state reporting
- node availability changes

Relevant file:

- [main.cpp](/mnt/e/dev/Repos/comet-node/controller/src/main.cpp)

Assessment:

- `completed` as controller workflow foundation

### 2. Controller already owns the business logic and state model

Implemented:

- SQLite-backed authoritative state
- controller-side scheduling, rollout, retry, availability, storage reporting
- controller logic lives in native code, not shell wrappers

Relevant files:

- [sqlite_store.h](/mnt/e/dev/Repos/comet-node/common/include/comet/sqlite_store.h)
- [sqlite_store.cpp](/mnt/e/dev/Repos/comet-node/common/src/sqlite_store.cpp)
- [import_bundle.cpp](/mnt/e/dev/Repos/comet-node/common/src/import_bundle.cpp)
- [reconcile.cpp](/mnt/e/dev/Repos/comet-node/common/src/reconcile.cpp)
- [planner.cpp](/mnt/e/dev/Repos/comet-node/common/src/planner.cpp)
- [scheduling_policy.cpp](/mnt/e/dev/Repos/comet-node/common/src/scheduling_policy.cpp)

Assessment:

- `completed` as architectural foundation for Phase F

### 3. JSON-oriented state rendering already exists internally

Implemented:

- bundle import is JSON-based
- desired state and runtime status already serialize/deserialize through JSON helpers

Relevant files:

- [state_json.cpp](/mnt/e/dev/Repos/comet-node/common/src/state_json.cpp)
- [runtime_status.cpp](/mnt/e/dev/Repos/comet-node/common/src/runtime_status.cpp)
- [infer_runtime_config.cpp](/mnt/e/dev/Repos/comet-node/common/src/infer_runtime_config.cpp)

Assessment:

- `completed`
- these serializers help a future API, but they are not an API contract yet

## What Is Not Implemented Yet

### 1. Controller REST server seam now exists and now covers the first inspection/mutation slice

Implemented:

- HTTP listener in `comet-controller`
- simple request routing
- JSON health responses
- read-only inspection responses for state, host, storage, rollout, and rebalance views
- safe mutating responses for bundle, rollout, rebalance, availability, and retry workflows
- shared action/result seam for HTTP-exposed controller mutations
- endpoint-level status handling for `200`, `404`, `405`, `500`

Still missing:

- broader request/response contracts for non-health workflows
- richer error envelope and API contract conventions

Evidence:

- HTTP server implementation now exists in [main.cpp](/mnt/e/dev/Repos/comet-node/controller/src/main.cpp)

Assessment:

- `completed`

### 2. REST coverage now includes the main controller mutation surface

Implemented:

- bundle validate / preview / import / apply endpoints
- node availability mutation endpoint
- host assignment retry endpoint
- rollout / rebalance mutation endpoints

Assessment:

- `completed`

### 3. No API authentication or request model

Missing:

- controller API configuration
- listen address / port
- auth model for operator actions
- API error envelope
- stable request/response schemas

Assessment:

- `partial`

### 4. Compatibility CLI over HTTP now exists for the main operator workflows

Implemented:

- thin operator CLI client built into `comet-controller`
- remote controller targeting through `--controller`
- target resolution through `COMET_CONTROLLER`
- config-based target resolution through `~/.config/comet/controller`
- common remote inspection and mutation workflows reuse the existing controller API surface

Assessment:

- `completed`

## Acceptance Audit

### Acceptance: current controller workflows are reachable through REST

Status:

- `completed`

Reason:

- the main inspection and mutation workflows are reachable through HTTP, including the controller's
  core state, host, storage, rollout, and rebalance surfaces

### Acceptance: API shape is sufficient for future UI / compatibility CLI work

Status:

- `completed`

Reason:

- the controller now exposes stable JSON inspection and mutation surfaces sufficient to support a
  future thin operator CLI and UI work

### Acceptance: thin operator CLI can perform common plane actions by calling controller APIs

Status:

- `completed`

Reason:

- `comet-controller` now acts as a thin remote operator CLI for the common inspection and mutation
  workflows through `--controller`, `COMET_CONTROLLER`, and config-based target resolution

## Practical Status

Current Phase F status:

- foundations exist
- F1 completed
- F2 completed
- F3 completed
- F4 completed through a shared action/result seam for mutating workflows and shared view/loaders
  for the main inspection surfaces, including `show-state`
- F5 completed
- F6 completed through smoke coverage of read-only endpoints, mutating endpoints, and
  CLI-over-HTTP flows

Suggested GitHub issue status:

- ready to move `#6` to `completed` if the current no-auth local/trusted-environment API boundary is acceptable

## Recommended Phase F Plan

### F1. Introduce a controller HTTP server seam

Implement:

- add a small native HTTP server inside `comet-controller`
- configurable listen host / port
- centralized routing and JSON response helpers

Definition of done:

- `comet-controller` can run in server mode and answer a health endpoint

Status:

- `completed`

### F2. Expose read-only REST endpoints first

Implement:

- `GET /state`
- `GET /host-assignments`
- `GET /host-observations`
- `GET /host-health`
- `GET /disk-state`
- `GET /rollout-actions`
- `GET /rebalance-plan`

Definition of done:

- all current read-only controller inspection surfaces are reachable via HTTP JSON

Status:

- `completed`
- implemented so far:
  - `GET /api/v1/state`
  - `GET /api/v1/host-assignments`
  - `GET /api/v1/host-observations`
  - `GET /api/v1/host-health`
  - `GET /api/v1/disk-state`
  - `GET /api/v1/rollout-actions`
  - `GET /api/v1/rebalance-plan`

### F3. Expose mutating control endpoints

Implement:

- `POST /bundles/validate`
- `POST /bundles/preview`
- `POST /bundles/apply`
- `POST /node-availability`
- `POST /assignments/{id}/retry`
- `POST /rollout-actions/{id}/...`
- `POST /scheduler/tick`

Definition of done:

- common controller mutations can be performed remotely through stable JSON endpoints

Status:

- `completed`
- implemented:
  - `POST /api/v1/bundles/validate`
  - `POST /api/v1/bundles/preview`
  - `POST /api/v1/bundles/import`
  - `POST /api/v1/bundles/apply`
  - `POST /api/v1/scheduler-tick`
  - `POST /api/v1/reconcile-rebalance-proposals`
  - `POST /api/v1/reconcile-rollout-actions`
  - `POST /api/v1/apply-rebalance-proposal`
  - `POST /api/v1/set-rollout-action-status`
  - `POST /api/v1/enqueue-rollout-eviction`
  - `POST /api/v1/apply-ready-rollout-action`
  - `POST /api/v1/node-availability`
  - `POST /api/v1/retry-host-assignment`

### F4. Extract controller command handlers into reusable service functions

Implement:

- refactor CLI command paths so REST handlers and CLI commands call the same service layer
- keep formatting and presentation separate from business logic

Definition of done:

- REST and CLI stop duplicating orchestration logic

Status:

- `completed`
- implemented:
  - HTTP-exposed mutating controller workflows now run through a shared action/result seam
  - CLI for those same workflows now uses that same action/result seam instead of invoking a
    separate HTTP-only wrapper
  - shared read-only view/loaders now back the main host/storage inspection surfaces:
    `host-assignments`, `host-observations`, `host-health`, `disk-state`
  - shared read-only view/loaders now also back rollout/rebalance inspection:
    `rollout-actions` and `rebalance-plan`
  - the full `show-state` aggregate path now also runs through a shared aggregate-view loader

### F5. Add thin operator CLI over HTTP

Implement:

- new operator-facing client commands for common plane workflows
- controller target selection (`--controller`, env var, config)
- JSON parsing and friendly terminal output

Definition of done:

- an operator can perform the common remote workflows without direct DB/filesystem access

Status:

- `completed`
- implemented:
  - `comet-controller` now accepts `--controller <http://host:port>` for common inspection and
    mutation workflows
  - controller target resolution also works through `COMET_CONTROLLER`
  - controller target resolution also works through `~/.config/comet/controller`
  - remote inspection commands emit JSON directly from controller API responses
  - remote mutation commands reuse the controller API and preserve command output / exit codes

### F6. Add Phase F validation campaign

Implement:

- smoke tests for read-only endpoints
- mutation tests for bundle/apply/retry/availability/scheduler actions
- CLI-over-HTTP tests

Definition of done:

- API and compatibility CLI are both validated end-to-end

Status:

- `completed`
- implemented:
  - `scripts/check.sh` validates read-only HTTP endpoints
  - `scripts/check.sh` validates mutating controller endpoints
- `scripts/check.sh` validates CLI-over-HTTP via `--controller`
- `scripts/check.sh` validates env-based target selection through `COMET_CONTROLLER`
- `scripts/check-live-phase-f.sh --skip-build` validates the implemented REST and CLI-over-HTTP
  surface end-to-end on a live controller server

## Conclusion

Phase F is underway but no longer early.

The good news is that the controller foundations are already strong: the current CLI surface and
controller-owned orchestration logic mean this phase is mostly an API/product-surface phase, not a
ground-up logic phase. With F1-F6 now in place, the next real milestone is API contract and
security hardening rather than building the first REST/CLI bridge itself.
