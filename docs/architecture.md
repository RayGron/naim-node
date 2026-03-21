# Architecture Notes

## Roles

- `comet-controller` owns desired state, scheduling decisions, reconciliation, and API surface
- `comet-hostd` owns local execution on one node: disks, mounts, compose artifacts, container lifecycle, telemetry
- disk declarations and realized disk runtime state are now stored separately, so controller/hostd can compare the desired disk inventory with the current realized host-side lifecycle instead of overloading one state model for both
- the host agent now has a privileged Linux disk lifecycle path (`image -> loop -> mkfs -> mount`) plus an explicit unprivileged directory-backed fallback for local development and smoke scenarios
- controller reporting can now show both declared disk inventory and realized disk lifecycle separately, including node-scoped disk views for operator inspection
- the controller now also has a native HTTP server seam (`serve`) with JSON health, state, host-assignment, host-observation, host-health, node-availability, disk-state, rollout-action, and rebalance-plan endpoints, plus the first safe mutating orchestration endpoints for `scheduler-tick`, `reconcile-rebalance-proposals`, `reconcile-rollout-actions`, `apply-rebalance-proposal`, `set-rollout-action-status`, `enqueue-rollout-eviction`, and `apply-ready-rollout-action`
- `comet-controller` can now also operate as a thin remote operator CLI for the common controller workflows through `--controller`, `COMET_CONTROLLER`, and `~/.config/comet/controller`, so the first Phase F bridge now covers both REST and CLI-over-HTTP
- the current Phase F API contract also injects `api_version` plus `request.path` / `request.method` into JSON responses and normalizes API errors under `status=error` with `error.code` / `error.message`

## Current Implementation Boundary

The first implementation slice is intentionally narrow:

- shared domain models exist in `common/`
- a bundle importer reads `plane.json`, `infer.json`, and `workers/*.json`
- the controller can validate and preview bundle results before import
- the controller can compute a reconcile plan against current SQLite state
- the controller can compute explicit per-node host execution plans
- the planner converts desired state into per-node compose plans
- the compose renderer emits deterministic `docker-compose.yml` content
- `apply-bundle` materializes compose artifacts under `var/artifacts/<plane>/<node>/docker-compose.yml`
- `apply-bundle` also materializes `var/artifacts/<plane>/infer-runtime.json` as the first direct replacement for legacy infer-side runtime config rendering
- `hostd` writes that infer runtime manifest into `control_root` on the infer node shared disk, so runtime containers can consume it without depending on the controller artifact path at startup
- `runtime/infer/inferctl.sh` consumes that manifest for validation, bootstrap, model-state planning, and launch control
- `runtime/infer/runtime-profiles.json` and `inferctl.sh bootstrap-runtime|doctor` cover the preflight layer: profile selection, directory preparation, and local readiness checks
- `inferctl.sh preload-model|cache-status|switch-model|show-active-model` now add a dry-run model lifecycle layer on top of that preflight path by materializing cache-registry and active-model state files under `control_root`, still without downloads or process launch
- host-side staging can preserve the runtime-visible GGUF mount path separately from the host-local staging path, so `hostd`-materialized shared disks and the in-container `llama.cpp` loader stay consistent during real deployments
- `inferctl.sh gateway-plan|gateway-status|status|stop` connect that model lifecycle back to gateway wiring and a combined readiness view under `control_root`
- `inferctl.sh launch-runtime` now selects the in-process `llama.cpp` backend whenever the active model resolves to a local GGUF file; that backend is linked directly into `comet-inferctl` as a library, not driven through external `llama-cli` commands
- the local HTTP runtime serves `/health`, `/v1/models`, `/v1/completions`, and `/v1/chat/completions` itself while keeping `runtime-status.json` current
- infer compose healthchecks now probe the live infer HTTP health endpoint instead of only checking for a marker file
- `hostd show-runtime-status` reads the same `runtime-status.json` from the infer node shared disk, so the host agent and infer-side helper share one runtime snapshot instead of maintaining separate ad-hoc summaries
- when `hostd` reports observed state back to the controller, it now includes that serialized runtime snapshot too, so controller-side host observations and health views can show runtime readiness without talking to infer containers directly
- the controller can persist desired state in SQLite
- the controller can queue per-node host assignments in SQLite for `hostd`
- queued host assignments are versioned by desired generation, and older pending/claimed rows are superseded instead of being silently dropped
- queued host assignments track `attempt_count/max_attempts`; transient hostd failures return a claimed assignment to `pending`, while exhausted assignments become `failed` until an operator explicitly requeues them
- controller-side node availability overrides (`active`, `draining`, `unavailable`) are stored separately from desired state and determine whether new host assignments are emitted for a node
- controller-side scheduling policy now validates worker GPU pinning, `share_mode`, `gpu_fraction`, `priority`, `preemptible`, and `memory_cap_mb` before desired state is persisted, and it produces per-`node/gpu` soft-share summaries, capacity/headroom views, rebalance hints, preemption order hints for `best-effort` workers, and ranked placement recommendations backed by node inventory `gpu_memory_mb`
- placement recommendations now distinguish direct-fit targets from targets that only become viable after controlled `best-effort` eviction; this is currently exposed as controller-side policy/reporting and not yet as automatic live eviction during apply
- workers now carry explicit `placement_mode` semantics: `manual` keeps placement strict, `auto` fills missing placement from ranked scheduler candidates, and `movable` lets the scheduler relocate or normalize a worker onto a better full-GPU placement when safe, while still requiring a materially better target before it overrides the current placement
- explicit `movable` requests now stay on their requested placement until full-state scheduler analysis runs; after that, the controller exposes either a direct-fit rebalance proposal or a deferred scheduler decision when the next step would require controlled preemption
- deferred scheduler decisions now also emit an explicit rollout action sequence, so controller output can tell the operator which `best-effort` workers to evict first and when to retry placement
- those deferred rollout actions now feed back into host-assignment planning as scheduler-gate messages on the affected target nodes, keeping orchestration output aligned with scheduler state even before automatic eviction exists
- the controller now exposes those persisted rollout actions through a dedicated CLI surface, so rollout orchestration can be inspected independently from raw scheduling reports
- rollout actions are now persisted in controller SQLite with an explicit lifecycle (`pending`, `acknowledged`, `ready-to-retry`), which is the first controller-managed state machine layer on top of deferred scheduler decisions
- once a deferred `retry-placement` action reaches `ready-to-retry`, the controller can materialize it into a new desired generation: evict recorded `best-effort` victims from desired state, apply the placement retry, regenerate scheduler output, and emit fresh host assignments for that new generation
- deferred scheduler decisions now also have an explicit node-execution seam: the controller can enqueue `evict-workers` host assignments for victim nodes, `hostd` applies those temporary eviction states, and controller-side rollout reconciliation can then auto-advance the paired `retry-placement` into a new desired generation
- controller output now derives a worker-level rollout lifecycle (`planned`, `eviction-enqueued`, `eviction-applied`, `retry-ready`, `retry-materialized`, `rollout-applied`) by combining persisted rollout actions with host assignments and host observations
- preemption-aware placement now picks the minimal ordered set of `best-effort` victims required to satisfy fraction and memory constraints for a candidate target, rather than assuming every best-effort worker on that GPU must be evicted
- controller-side rebalance planning now filters placement recommendations through observed host gates, so movable/auto workers only surface as ready targets when the destination node is not stale, failed, or runtime-failed
- rebalance output now also carries an explicit controller decision layer (`propose`, `hold`, `defer`) so policy can distinguish between immediately acceptable moves, gated targets, and candidates that still require a controlled preemption workflow
- rebalance output also carries an explicit rebalance class (`safe-direct`, `rollout-class`, `gated`, `stable`, `no-candidate`), so direct-fit controller-managed moves are separated from rollout/preemption work and from purely blocked states
- safe-direct moves are filtered through a minimum usefulness threshold; technically possible but low-value direct moves remain `stable` with `state=below-threshold` instead of turning into actionable rebalance work
- rebalance output now also includes a controller-side `rebalance-policy` summary, so actionable moves, active-rollout blockers, in-flight assignment blockers, observation-gated targets, stable holds, deferred preemption cases, and no-candidate cases are visible at a glance
- the controller also emits a coarse `rebalance-loop-status` summary (`waiting-for-convergence`, `waiting-for-rollout`, `actionable`, `complete`) so the scheduler loop has an explicit stop/wait state instead of only per-worker recommendations
- safe-direct rebalance is now also bounded by a persisted controller-side iteration budget; each materialized direct move increments that counter, fresh bundle/rollout generations reset it, and once the budget is exhausted the remaining direct-fit proposals stay visible but no further controller-managed direct rebalance is launched for that loop
- direct-fit `propose` decisions can now be materialized into a new desired generation through `apply-rebalance-proposal`, making safe rebalance a controller-managed workflow instead of an import-time side effect
- the controller can also reconcile those direct-fit proposals automatically through `reconcile-rebalance-proposals`, selecting the highest-scoring actionable move after rollout and in-flight assignment gates clear
- controller-side rebalance now has an explicit cluster gate summary, so active rollout lifecycle, pending/claimed host assignments, and unconverged schedulable nodes can block new rebalance iterations until the cluster is stable again
- observed GPU telemetry now also participates in target admission: the scheduler can hold a candidate on `compute-pressure` or `observed-insufficient-vram` instead of trusting only static fractions
- drain state now feeds placement itself: when a worker currently lives on a `draining` node, controller-side rebalance prefers an active off-node target and can emit `ready-drain-move` instead of keeping same-node upgrades on the draining host
- the controller now persists scheduler runtime state of its own: plane-level active scheduler action, per-worker move/eviction bookkeeping, and per-node verification bookkeeping
- that runtime state now feeds controller-side anti-flap holds such as `min-residency`, `cooldown`, `active-scheduler-action`, and `manual-intervention-required`
- `scheduler-tick` is now the stepwise controller-managed scheduler loop entrypoint; a single tick advances exactly one safe orchestration step instead of chaining multiple moves at once
- direct-fit rebalance now enters an explicit verification phase (`verifying-move`) after materialization; it is only considered successful once host observations report the target generation applied, runtime readiness, and matching GPU ownership for the moved worker over multiple stable samples
- if that verification times out, the controller first records `rollback-planned`, and the next scheduler tick materializes rollback from the previously persisted desired state into `rollback-applied`; if rollback still does not verify, the worker is marked `manual-intervention-required`
- when a node leaves `active`, the controller can enqueue a node-specific drain assignment so `hostd` removes node-local workloads
- when a node transitions back to `active`, the controller can enqueue a node-specific resync assignment for the current desired generation if that node missed the latest rollout
- `hostd` reports node-local observed state and heartbeat data back into controller SQLite, including the last applied generation and last assignment id
- `comet-controller` derives a host-health view from those heartbeats so operators can distinguish online, stale, and never-seen nodes
- `hostd` shows the concrete local operations implied by SQLite-backed desired state
- `hostd` can apply node-local filesystem work from desired state, with optional `runtime-root` rebasing for safe local testing
- `hostd` persists node-local applied state so later runs can reconcile deltas instead of always applying from scratch

## Why Start Here

This path exercises the main architecture seam:

`desired state -> node plan -> compose artifact + infer runtime artifact -> host execution`

That seam should stay stable even after the following pieces are added:

- SQLite state and migrations application
- JSON bundle import and validation
- Docker API execution
- REST API, SSE, and UI
- scheduler logic for sharing and draining GPUs
