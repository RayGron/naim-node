# Live Phase F Validation Report

Date: 2026-03-21

## Scope

Validation target:

- Phase F: REST API and operator CLI compatibility

Environment:

- Linux x64 build
- native `comet-controller serve`
- local HTTP access on `127.0.0.1`
- CLI-over-HTTP through `--controller` and `COMET_CONTROLLER`

## What Was Validated

### 1. Read-only API surface

Confirmed on a live controller server:

- `GET /health`
- `GET /api/v1/health`
- `GET /api/v1/state`
- `GET /api/v1/host-assignments`
- `GET /api/v1/host-observations`
- `GET /api/v1/host-health`
- `GET /api/v1/node-availability`
- `GET /api/v1/disk-state`
- `GET /api/v1/rollout-actions`
- `GET /api/v1/rebalance-plan`

Observed contract:

- responses include `api_version`
- responses include `request.path` and `request.method`

### 2. Mutating API surface

Confirmed on a live controller server:

- `POST /api/v1/bundles/validate`
- `POST /api/v1/bundles/preview`
- `POST /api/v1/bundles/import`
- `POST /api/v1/bundles/apply`
- `POST /api/v1/node-availability`
- `POST /api/v1/retry-host-assignment`
- `POST /api/v1/scheduler-tick`
- `POST /api/v1/reconcile-rebalance-proposals`
- `POST /api/v1/reconcile-rollout-actions`
- `POST /api/v1/apply-rebalance-proposal`
- `POST /api/v1/set-rollout-action-status`
- `POST /api/v1/enqueue-rollout-eviction`
- `POST /api/v1/apply-ready-rollout-action`

These were validated across three live controller scenarios:

- basic bundle and availability workflow
- safe-direct rebalance workflow
- deferred preemption / rollout workflow

### 3. Thin operator CLI over HTTP

Confirmed on a live controller server:

- `comet-controller ... --controller http://127.0.0.1:<port>`
- `COMET_CONTROLLER=http://127.0.0.1:<port> comet-controller ...`

Validated command classes:

- inspection commands:
  - `show-state`
  - `show-host-assignments`
  - `show-node-availability`
- mutation commands:
  - `validate-bundle`
  - `set-node-availability`

## Hardening Improvements Validated

The live run also covered the latest API hardening slice:

- percent-decoding for query parameters
- stable `api_version` metadata on API responses
- stable `request` metadata on API responses
- normalized error envelope:
  - `status=error`
  - `error.code`
  - `error.message`

## Commands Run

Validated with:

- `./scripts/build-target.sh linux x64 Debug`
- `./scripts/check.sh`
- `./scripts/check-live-phase-f.sh --skip-build`

## Result

Result: `OK`

Phase F currently has:

- working native controller HTTP server
- working read-only inspection API
- working mutating controller API
- working thin operator CLI over HTTP
- working smoke and live validation coverage for the implemented surface
