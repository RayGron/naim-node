# Phase G Status Report

Date: 2026-03-21

## Scope Reference

GitHub issue: `#7` `Phase G: Extended telemetry and event stream`

Phase G goal:

- expand observability beyond heartbeat status
- collect richer host and runtime telemetry
- store and surface telemetry useful for production scheduling and diagnostics
- introduce a controller-visible event stream for later API / SSE / UI work

## Executive Summary

Phase G is `in progress`, but unevenly.

The current codebase already has a meaningful telemetry layer around host observations:

- runtime status snapshots
- per-instance runtime status
- per-GPU telemetry
- scheduler consumption of observed GPU pressure
- controller-side inspection surfaces for those signals

That means the phase is no longer hypothetical. The gap is that the implementation is still
focused mostly on GPU/runtime readiness. The broader "extended telemetry and event stream" scope is
not complete yet:

- disk telemetry is lifecycle-oriented, not yet usage/performance-oriented
- network telemetry is not implemented
- there is no structured event stream or persisted event log

## What Is Already Implemented

### 1. Runtime and GPU telemetry are already collected and persisted

Implemented:

- `hostd` collects runtime status snapshots and per-instance runtime state
- `hostd` collects GPU telemetry through NVML when available
- `hostd` falls back to `nvidia-smi` in degraded mode
- host observations persist both runtime and GPU telemetry into SQLite

Relevant files:

- [main.cpp](/mnt/e/dev/Repos/comet-node/hostd/src/main.cpp)
- [runtime_status.h](/mnt/e/dev/Repos/comet-node/common/include/comet/runtime_status.h)
- [runtime_status.cpp](/mnt/e/dev/Repos/comet-node/common/src/runtime_status.cpp)
- [sqlite_store.h](/mnt/e/dev/Repos/comet-node/common/include/comet/sqlite_store.h)
- [sqlite_store.cpp](/mnt/e/dev/Repos/comet-node/common/src/sqlite_store.cpp)

Assessment:

- `completed`

### 2. The controller already surfaces telemetry to operators

Implemented:

- controller parses `runtime_status_json`
- controller parses `gpu_telemetry_json`
- controller inspection surfaces show telemetry in:
  - `show-host-observations`
  - `show-host-health`
  - `show-state`
- the Phase F HTTP layer exposes those same views as JSON

Relevant files:

- [main.cpp](/mnt/e/dev/Repos/comet-node/controller/src/main.cpp)

Assessment:

- `completed`

### 3. Scheduler already consumes actual observed telemetry

Implemented:

- target admission can hold on `compute-pressure`
- target admission can hold on `observed-insufficient-vram`
- telemetry degradation is visible as `telemetry-degraded`
- move verification checks observed runtime readiness and observed GPU ownership

Relevant files:

- [main.cpp](/mnt/e/dev/Repos/comet-node/controller/src/main.cpp)

Assessment:

- `completed`

### 4. Disk visibility exists, but as runtime lifecycle reporting, not full telemetry

Implemented:

- desired vs realized disk state is persisted
- controller can inspect realized disk lifecycle
- live validation exists for ensure / teardown / restart reconciliation

Relevant files:

- [sqlite_store.h](/mnt/e/dev/Repos/comet-node/common/include/comet/sqlite_store.h)
- [sqlite_store.cpp](/mnt/e/dev/Repos/comet-node/common/src/sqlite_store.cpp)
- [main.cpp](/mnt/e/dev/Repos/comet-node/hostd/src/main.cpp)
- [main.cpp](/mnt/e/dev/Repos/comet-node/controller/src/main.cpp)

Assessment:

- `partial`
- this is operational lifecycle visibility, not yet filesystem usage / capacity / performance telemetry

## What Is Not Implemented Yet

### 1. No network telemetry layer

Missing:

- interface inventory
- link status
- RX/TX byte counters
- controller-visible network diagnostics

Assessment:

- `not completed`

### 2. No structured event stream

Missing:

- controller event log
- hostd event log
- persisted event records for important state transitions
- typed event payloads for later SSE / UI use

Assessment:

- `not completed`

### 3. No disk usage / performance telemetry

Missing:

- filesystem usage for mounted disks
- free/used space reporting
- mount health / IO error telemetry

Assessment:

- `not completed`

## Acceptance Audit

### Acceptance: controller can inspect richer hardware and runtime signals than simple heartbeat status

Status:

- `completed`

Reason:

- runtime status, instance runtime state, GPU device telemetry, and degraded telemetry state are
  already visible in controller views

### Acceptance: scheduling can consume actual telemetry rather than only static config

Status:

- `completed`

Reason:

- scheduler and move verification already use observed GPU pressure, observed free VRAM, telemetry
  degradation, runtime readiness, and GPU ownership signals

### Acceptance: telemetry is shaped for API and SSE use later

Status:

- `partial`

Reason:

- telemetry is already serialized and Phase F HTTP endpoints can surface it, but there is still no
  structured event stream and no broader telemetry contract for later SSE/UI layers

## Practical Status

Current Phase G status:

- GPU/runtime telemetry: strong
- scheduler telemetry integration: strong
- disk telemetry: partial
- network telemetry: missing
- event stream: missing

Suggested GitHub issue status:

- keep `#7` as `in progress`

## Recommended Phase G Plan

### G1. Stabilize telemetry envelopes and controller-facing contracts

Implement:

- define explicit controller-visible payloads for:
  - runtime status
  - per-instance runtime status
  - GPU telemetry
  - degraded telemetry state
- ensure telemetry fields are stable across CLI and HTTP inspection surfaces

Definition of done:

- telemetry output is no longer just "whatever hostd stored", but a stable controller contract

### G2. Extend disk telemetry beyond lifecycle state

Implement:

- filesystem capacity / used / free bytes for mounted managed disks
- mount health / verification status
- disk usage reporting in host observations and controller views

Definition of done:

- controller can inspect not only realized mounts, but also actual disk usage state

### G3. Add network telemetry collection

Implement:

- interface inventory
- link state
- RX/TX byte counters
- optional listen-port / service reachability summary where useful

Definition of done:

- host observations include a first useful network telemetry slice

### G4. Introduce a structured event log

Implement:

- persisted controller events for:
  - desired generation changes
  - assignment lifecycle
  - rollout lifecycle
  - scheduler lifecycle
  - availability changes
- persisted host events for:
  - apply start/finish/failure
  - disk lifecycle transitions
  - telemetry degradation / recovery

Definition of done:

- important control-plane and host transitions are queryable as typed events

### G5. Surface telemetry and events through inspection APIs

Implement:

- CLI and HTTP views for:
  - extended telemetry
  - recent events
- keep contracts aligned with later SSE / UI work

Definition of done:

- Phase G data is ready to be reused by later REST / SSE / UI phases

### G6. Add validation campaign for telemetry and events

Implement:

- smoke tests for telemetry serialization / storage / controller rendering
- live tests for:
  - GPU telemetry collection
  - degraded telemetry fallback
  - disk telemetry
  - event emission on controller / hostd transitions

Definition of done:

- telemetry and event stream behavior are validated end-to-end

## Conclusion

Phase G is not empty; substantial GPU/runtime telemetry work is already present in the current
codebase and is already used by the scheduler. The phase is still not complete, though, because
the broader observability goal requires disk usage telemetry, network telemetry, and a structured
event stream.
