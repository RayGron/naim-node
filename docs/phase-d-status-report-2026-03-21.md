# Phase D Status Report

Date: 2026-03-21
Issue: `#4` Phase D: Production scheduler and soft-sharing

## Overall Status

Current assessment: `in progress`

Phase D is substantially implemented, including live single-GPU deferred-preemption validation,
but it is not ready to be marked fully complete yet.

## Scope Review

### 1. Add VRAM-aware worker admission

Status: `completed`

Evidence:

- static VRAM admission and oversubscription checks in `memory_cap_mb` / `gpu_memory_mb`
- live GPU telemetry persisted in host observations
- eviction verification checks free VRAM before advancing rollout

Relevant code:

- [scheduling_policy.cpp](/mnt/e/dev/Repos/comet-node/common/src/scheduling_policy.cpp)
- [hostd/src/main.cpp](/mnt/e/dev/Repos/comet-node/hostd/src/main.cpp)
- [sqlite_store.cpp](/mnt/e/dev/Repos/comet-node/common/src/sqlite_store.cpp)

### 2. Add compute-budget admission

Status: `completed`

What is done:

- controller-side capacity and headroom model based on `gpu_fraction`
- safe-direct vs preemption-aware fit classification
- observed target gating from live GPU telemetry:
  - `compute-pressure`
  - `observed-insufficient-vram`

Relevant code:

- [scheduling_policy.cpp](/mnt/e/dev/Repos/comet-node/common/src/scheduling_policy.cpp)
- [controller/src/main.cpp](/mnt/e/dev/Repos/comet-node/controller/src/main.cpp)

### 3. Implement empty-GPU-first placement

Status: `completed`

Evidence:

- auto-placement prefers idle GPUs
- movable workers get ranked candidates with idle targets preferred
- safe-direct rebalance proposals use empty/idle GPU opportunities first

Relevant code:

- [import_bundle.cpp](/mnt/e/dev/Repos/comet-node/common/src/import_bundle.cpp)
- [scheduling_policy.cpp](/mnt/e/dev/Repos/comet-node/common/src/scheduling_policy.cpp)
- [controller/src/main.cpp](/mnt/e/dev/Repos/comet-node/controller/src/main.cpp)

### 4. Implement legal soft-share fallback when a free GPU is not available

Status: `completed`

Evidence:

- `share_mode=shared|best-effort`
- deferred preemption path
- rollout actions `evict-best-effort` and `retry-placement`
- live single-GPU validation of one-GPU contention path

Relevant code:

- [scheduling_policy.cpp](/mnt/e/dev/Repos/comet-node/common/src/scheduling_policy.cpp)
- [controller/src/main.cpp](/mnt/e/dev/Repos/comet-node/controller/src/main.cpp)
- [check-live-scheduler-single-gpu.sh](/mnt/e/dev/Repos/comet-node/scripts/check-live-scheduler-single-gpu.sh)
- [live-single-gpu-scheduler-report-2026-03-21.md](/mnt/e/dev/Repos/comet-node/docs/live-single-gpu-scheduler-report-2026-03-21.md)

### 5. Add worker priority and preemption rules

Status: `completed`

Evidence:

- priority-aware victim ordering
- `best-effort` eviction preference
- persisted rollout actions and controller-managed eviction workflow
- live `evict-workers` verification on real runtime

Relevant code:

- [scheduling_policy.cpp](/mnt/e/dev/Repos/comet-node/common/src/scheduling_policy.cpp)
- [controller/src/main.cpp](/mnt/e/dev/Repos/comet-node/controller/src/main.cpp)
- [hostd/src/main.cpp](/mnt/e/dev/Repos/comet-node/hostd/src/main.cpp)

### 6. Add drain-aware worker movement planning

Status: `completed`

What is done:

- node availability overrides
- `drain-node-state`
- drain/resync flow
- scheduler gates respect active rollout and host availability/health
- scheduler now prefers active off-node targets for workers currently placed on a draining node
- scheduler can emit `ready-drain-move` and `draining-source` states

Relevant code:

- [controller/src/main.cpp](/mnt/e/dev/Repos/comet-node/controller/src/main.cpp)
- [hostd/src/main.cpp](/mnt/e/dev/Repos/comet-node/hostd/src/main.cpp)

### 7. Expose scheduler reasoning in operator-visible planning output

Status: `completed`

Evidence:

- `show-rebalance-plan`
- `show-rollout-actions`
- `rebalance-policy`
- `rebalance-controller-gate`
- `rebalance-loop-status`
- `scheduler-runtime`

Relevant code:

- [controller/src/main.cpp](/mnt/e/dev/Repos/comet-node/controller/src/main.cpp)

## Acceptance Criteria Review

### Invalid or unsafe placements are rejected before apply

Status: `completed`

### Scheduling output explains why each worker was placed or rejected

Status: `completed`

### Soft-sharing obeys explicit policy constraints rather than only static fractions

Status: `completed`

### Host assignments only contain legal runtime work

Status: `completed`

Current assignment types stay within scheduler-approved actions such as `apply-node-state` and
`evict-workers`, and deferred scheduler steps are not applied silently.

### Drain and availability transitions can trigger safe rescheduling decisions

Status: `completed`

Availability and drain now affect both orchestration and scheduler-side relocation planning.

## Conclusion

Phase D should remain `in progress`.

Recommended completion judgment:

- `completed`: 7 scope items
- `partially completed`: 0 scope items
- `not completed`: 0 scope items

Main blockers before closing Phase D:

1. Run the separate live two-GPU safe-direct scenario on a host with at least two visible GPUs.
2. Decide whether the existing Phase D scope is sufficient for closure or whether additional live scheduler campaigns should be treated as exit criteria.
