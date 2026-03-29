# comet-node

Initial implementation scaffold for `comet`, a Docker-oriented replacement for the legacy
Plane / Infer / Worker orchestration flow.

## Current MVP

- `comet-controller` builds a demo desired state and renders per-node `docker-compose.yml`
- `comet-controller` also renders an infer-side runtime manifest that starts to replace legacy `infer.conf`
- `comet-hostd` prints the local disk and container actions it would execute on a node
- `comet-hostd` now writes `infer-runtime.json` into the plane control path on the shared disk for infer nodes
- shared C++ models describe planes, nodes, instances, disks, GPU leases, and compose services
- controller SQLite now also keeps a separate runtime disk-state layer, so `show-state` can distinguish declared disks from realized host-side disk lifecycle
- `hostd` now has a real Linux disk lifecycle path for managed disks (`image -> loop -> mkfs -> mount`) when running with root privileges, while local dev/smoke environments continue through an explicit directory-backed fallback
- runtime entrypoint scripts and SQLite migrations are stubbed into the repository layout from day one

This first slice intentionally focuses on:

1. a clean repository structure
2. a buildable C++ control-plane skeleton
3. the path from desired state to `compose` artifacts
4. a SQLite-backed controller state store
5. the first migration step for legacy infer runtime configuration

Docker execution is now live, and Phase F has started with a native controller HTTP seam for
health/readiness. The broader REST surface and operator compatibility CLI are the next layers.

## Runtime Images

The repository now includes real runtime Dockerfiles:

- [runtime/base/Dockerfile](/mnt/e/dev/Repos/comet-node/runtime/base/Dockerfile)
- [runtime/infer/Dockerfile](/mnt/e/dev/Repos/comet-node/runtime/infer/Dockerfile)
- [runtime/worker/Dockerfile](/mnt/e/dev/Repos/comet-node/runtime/worker/Dockerfile)

The current base image is `debian:bookworm-slim`. That is intentionally a lightweight
glibc-based base, rather than Alpine, because the next phases need a safer path for
`llama.cpp`-based inference work and native C++ runtime integration.

The demo plane now targets local dev images:

- `comet/base-runtime:dev`
- `comet/infer-runtime:dev`
- `comet/worker-runtime:dev`

Build them with:

```bash
./scripts/build-runtime-images.sh
```

Run a live Docker/NVIDIA safe-direct scheduler check with a real GGUF:

```bash
./scripts/check-live-scheduler.sh --gguf /path/to/model.gguf
```

That script deploys a movable-worker plane, stages the GGUF on the shared disk, waits for
worker runtimes to consume real GPU memory through embedded `llama.cpp`, materializes one
safe-direct rebalance, and drives `scheduler-tick` until the move reaches `verified`.
It currently targets the cross-GPU safe-direct scenario and expects at least two visible
NVIDIA GPUs on the host.

Run a live Docker/NVIDIA single-GPU deferred-preemption check with a real GGUF:

```bash
./scripts/check-live-scheduler-single-gpu.sh --gguf /path/to/model.gguf
```

That script exercises the one-GPU scheduler shape: a movable worker competes with a
best-effort worker on the same GPU, controller enqueues a real `evict-workers` assignment,
`hostd` verifies that the victim really disappears from the GPU, and the rollout finishes by
materializing `retry-placement` onto the same device. This is the live harness to use on a host
with a single visible NVIDIA GPU.

Run a live storage validation campaign for mounted volumes:

```bash
./scripts/check-live-storage.sh
```

That harness uses compact test disk sizes, mounts real managed disk images through `hostd`,
verifies container visibility of mounted volumes, re-checks restart reconciliation, and then
validates teardown on a reduced bundle.

Phase G telemetry now also includes block-level disk IO counters for mounted disks:

- filesystem usage/capacity through `statvfs`
- block IO counters from `/sys/class/block/<device>/stat` when the realized mount source resolves
  to a block device
- normalized disk fault counters and availability markers:
  - `fault_count`
  - `warning_count`
  - `perf_counters_available`
  - `io_error_counters_available`
  - `io_error_count` when a device-specific sysfs counter exists
- controller-visible `read_bytes`, `write_bytes`, `read_ios`, `write_ios`, and `io_time_ms`
  through `show-host-observations`, `show-disk-state`, `/api/v1/host-observations`, and
  `/api/v1/disk-state`

## Dependencies

External C/C++ dependencies are managed through `vcpkg` in manifest mode.

Current manifest dependencies:

- `sqlite3`
- `nlohmann-json`
- `libsodium`

The project pulls `llama.cpp` through CMake `FetchContent` using the pinned revision in
`CMakeLists.txt`.

The host configure script searches for `vcpkg`, installs the manifest dependencies for the
current platform triplet, and only then runs CMake configure.

On macOS, install the build prerequisites first:

```bash
brew install pkg-config autoconf automake libtool libomp
```

## Build

```bash
./scripts/configure-host-build.sh Debug
cmake --build "$(./scripts/print-host-build-dir.sh)"
```

The configure step clears the previous CMake cache in the selected build folder so toolchain
and `vcpkg` dependency changes are picked up reliably.

Host builds are grouped by platform and architecture:

- Linux x64: `build/linux/x64`
- Linux arm64: `build/linux/arm64`
- Windows x64: `build/windows/x64`
- Windows arm64: `build/windows/arm64`
- macOS x64: `build/macos/x64`
- macOS arm64: `build/macos/arm64`

The executables and static libraries are emitted directly into that folder.

Windows builds use the Windows toolchain directly, even when started from a Remote WSL window:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts\windows\build-target.ps1 x64 Debug
powershell.exe -ExecutionPolicy Bypass -File scripts\windows\build-target.ps1 x64 Release
```

Windows release packaging:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts\windows\package-target.ps1 x64
```

If your Windows CMake setup needs a specific generator or executable path, you can override them with `COMET_WINDOWS_CMAKE_GENERATOR` and `COMET_WINDOWS_CMAKE`.

Current Windows builds focus on the control-plane binaries (`comet-node`, `comet-controller`, `comet-hostd`). The Linux-only runtime executables remain part of the Linux build path.

You can also configure any target layout manually:

```bash
cmake -S . -B build/linux/x64
cmake --build build/linux/x64
```

## VS Code Tasks

The repository ships VS Code build tasks similar to the `maglev` workflow:

- `Comet Linux x64: Build Debug`
- `Comet Linux x64: Build Release`
- `Comet Linux arm64: Build Debug`
- `Comet Linux arm64: Build Release`
- `Comet Windows x64: Build Debug`
- `Comet Windows x64: Build Release`
- `Comet Windows arm64: Build Debug`
- `Comet Windows arm64: Build Release`
- `Comet macOS x64: Build Debug`
- `Comet macOS x64: Build Release`
- `Comet macOS arm64: Build Debug`
- `Comet macOS arm64: Build Release`
- `Comet Linux x64: Package Release`
- `Comet Linux arm64: Package Release`
- `Comet Windows x64: Package Release`
- `Comet Windows arm64: Package Release`
- `Comet macOS x64: Package Release`
- `Comet macOS arm64: Package Release`
- `Comet: Package Release`
- `Comet: Check`
- `Comet macOS: Check`

From a Remote WSL window:

- Linux tasks use the native Linux toolchain inside WSL
- Windows tasks call `powershell.exe` and require WSL interop to be working, so `cmd.exe /C ver` must succeed inside WSL

Controller-side storage inspection now has two layers:

- `show-state` prints both declared disks and realized runtime disk state
- `show-disk-state` focuses just on storage and compares desired disk inventory with realized host lifecycle

## Run

Show the demo desired state:

```bash
./build/linux/x64/comet-controller show-demo-plan
```

Initialize the controller SQLite database:

```bash
./build/linux/x64/comet-controller init-db --db var/controller.sqlite
```

Seed the database with the demo desired state:

```bash
./build/linux/x64/comet-controller seed-demo --db var/controller.sqlite
```

Import a plane bundle from files:

```bash
./build/linux/x64/comet-controller import-bundle --bundle config/demo-plane --db var/controller.sqlite
```

Apply a plane desired-state file directly:

```bash
./build/linux/x64/comet-controller apply-state-file \
  --db var/controller.sqlite \
  --artifacts-root var/artifacts \
  --state ../lt-cypher-ai/deploy/comet-node/desired-state.json
```

Validate a plane bundle without applying it:

```bash
./build/linux/x64/comet-controller validate-bundle --bundle config/demo-plane
```

## GPU Scheduling Policy

Worker bundle entries can now declare scheduler-facing GPU policy fields:

- `placement_mode`: `manual`, `auto`, or `movable`
- `share_mode`: `exclusive`, `shared`, or `best-effort`
- `gpu_fraction`
- `priority`
- `preemptible`
- `memory_cap_mb`

Placement rules now work like this:

- `placement_mode=manual`: worker must specify both `node` and `gpu_device`
- `placement_mode=auto`: scheduler fills missing placement and can normalize the worker onto an
  idle GPU as `share_mode=exclusive` with `gpu_fraction=1`
- `placement_mode=movable`: scheduler may override the requested placement when there is a better
  candidate, but it keeps the current placement unless the alternative is materially better; the
  resulting direct-fit move is now controller-managed and must be materialized explicitly rather
  than being applied silently during import

Plane node inventory can also declare `gpu_memory_mb` per GPU so the controller can reject
GPU memory oversubscription before rollout. The current controller-side scheduler validates:

- explicit GPU pinning for workers
- legal `share_mode` and `gpu_fraction` combinations
- per-GPU fraction oversubscription
- per-GPU memory oversubscription when node inventory exposes `gpu_memory_mb`
- exclusive/shared/best-effort mixing rules
- per-GPU capacity/headroom summaries for idle GPUs, free fraction, and free VRAM
- rebalance hints when a partial/shared worker could move onto an idle GPU and run exclusively
- preemption hints that order `best-effort` workers as first eviction candidates under pressure
- ranked placement recommendations with per-candidate fit/score details for rollout decisions
- preemption-aware placement candidates that distinguish:
  - direct fit on free capacity
  - fit only after controlled eviction of `best-effort` workers

The current scheduler uses those preemption-aware candidates for controller-side admission and
operator guidance. It does not silently evict running work during bundle import or apply.

For `placement_mode=movable`, the controller now distinguishes:

- decisions it can apply immediately from full-state scheduler analysis
- deferred decisions that require controlled preemption before rollout can continue

Those decisions are surfaced in controller output as `scheduler-decisions` with
`decision=proposed` or `decision=deferred`, plus `next_target`, `next_action`, and `victims`.
They also produce explicit `rollout-actions` steps such as `evict-best-effort` and
`retry-placement`, so the operator-facing plan is no longer implicit.
Controller output now also derives a separate `rollout-lifecycle` view for each affected worker,
so the rollout path is visible as explicit phases such as `planned`,
`eviction-enqueued`, `eviction-applied`, `retry-materialized`, and `rollout-applied`.
For movable and auto-placed workers, the controller also exposes a separate `rebalance-plan`
surface that summarizes the best currently admissible move or upgrade, while honoring observed
health gates on target nodes. That surface now includes explicit controller-side decisions such as
`propose`, `hold`, and `defer`, so rebalance output distinguishes "good candidate, can propose",
"target is gated, hold", and "candidate only works after controlled preemption".
Each rebalance entry also carries an explicit class:

- `safe-direct`: direct-fit move or in-place upgrade that can be materialized without preemption
- `rollout-class`: worker currently tied to rollout/preemption workflow
- `gated`: move blocked by in-flight assignments or host observation gates
- `stable`: worker should stay where it is
- `no-candidate`: no admissible target exists

Safe-direct moves are also subject to a controller-side usefulness threshold. If a direct-fit move
is technically possible but too weak to justify churn, the worker is reported as
`state=below-threshold` instead of becoming actionable.

It also prints a `rebalance-policy` summary with actionable workers, workers blocked by active
rollouts, workers blocked by in-flight host assignments, observation-gated targets, stable holds,
deferred moves, and no-candidate cases, so the operator can tell whether the cluster is ready for
rebalance or still waiting on rollout/host/health gates.
When a movable worker reaches `decision=propose`, the controller can materialize that direct-fit
move with `comet-controller apply-rebalance-proposal --worker <name> --db ... [--artifacts-root ...]`.
There is also a controller-managed pass `comet-controller reconcile-rebalance-proposals --db ...`
that picks the highest-scoring actionable proposal once rollout and host-assignment gates clear.
That command now exposes a `rebalance-controller-gate` summary, so it is obvious when rebalance is
blocked because the cluster still has active rollout lifecycle, pending/claimed host assignments,
or schedulable nodes that have not yet converged to the current desired generation.
It also emits a `rebalance-loop-status` summary with coarse loop states such as
`waiting-for-convergence`, `waiting-for-rollout`, `actionable`, and `complete`, so it is clear
whether the scheduler is still blocked on rollout/convergence or has already drained all
worthwhile direct-fit moves for the current generation.
The controller now also uses observed GPU telemetry directly when evaluating a target GPU:

- a candidate can be held with `gate_reason=compute-pressure` when the observed target GPU is
  already under foreign live compute load
- a candidate can be held with `gate_reason=observed-insufficient-vram` when observed free VRAM is
  lower than the worker memory budget plus reserve

Phase G telemetry now also extends beyond GPU/runtime snapshots:

- `hostd` persists per-disk filesystem usage and mount health
- `hostd` persists per-interface network telemetry from `/sys/class/net`
- the controller now keeps a structured `event_log` in SQLite for important host and scheduler
  transitions
- operators can inspect that event stream with `comet-controller show-events --db ...` or
  `GET /api/v1/events`

Drain state is now also a placement input, not only a host lifecycle input:

- when a worker currently sits on a `draining` node, same-node rebalance targets on that node are
  no longer preferred
- if an active off-node target exists, the scheduler now surfaces `state=ready-drain-move`
- if the only available targets stay on the draining node, the worker is held with
  `state=draining-source`

On top of that, the controller now persists a `rebalance-iteration-budget` for safe-direct moves.
Fresh bundle imports and rollout-materialized generations reset that budget to `0`, while each
materialized direct rebalance increments it. Once the configured budget is exhausted, additional
safe-direct proposals remain visible in `show-rebalance-plan` but `reconcile-rebalance-proposals`
will stop and report `blocked by iteration budget` until a fresh non-rebalance generation resets
the loop.
The controller now also persists scheduler runtime metadata in SQLite:

- one plane-level active scheduler action
- per-worker `last_move_at`, `last_eviction_at`, and `manual_intervention_required`
- per-node `last_move_at` and `last_verified_generation`

That state is printed as `scheduler-runtime` in controller output and feeds anti-flap policy
directly. A rebalance candidate can now be held not only because of rollout or host gates, but
also because of:

- `min-residency`
- `cooldown`
- `active-scheduler-action`
- `manual-intervention-required`
- `telemetry-degraded`

`scheduler-tick` is now the stepwise controller-managed loop for this layer. One tick advances one
safe orchestration step in priority order:

1. active scheduler action verification / rollback
2. deferred rollout reconciliation
3. safe-direct rebalance reconciliation

Safe-direct rebalance is therefore no longer treated as “done” immediately after a new desired
generation is emitted. The controller records that move as an active scheduler action with
`phase=verifying-move` and waits for observed state to confirm:

- target node has applied the new desired generation
- target runtime reports the worker as `ready`
- target GPU telemetry shows owned process data for that worker
- source node no longer reports the worker on its previous GPU
- three stable verification samples in a row confirm the state

If that verification times out, the controller first records `phase=rollback-planned`, and the
next `scheduler-tick` materializes rollback from the previously persisted desired state into
`phase=rollback-applied`. If rollback also fails to verify, the worker is marked
`manual_intervention_required=yes` and automatic rebalance for that worker is blocked until an
operator resolves it.

When those deferred actions target a node, the corresponding `apply-node-state` host assignment is
now annotated with a scheduler-gate message so controller-side assignment planning stays aligned
with scheduler state.
You can inspect the persisted rollout workflow directly with
`comet-controller show-rollout-actions --db ...`.
Those persisted rollout actions now have their own lifecycle:

- `pending`
- `acknowledged`
- `ready-to-retry`

Use `comet-controller set-rollout-action-status --id ... --status ...` to move a deferred rollout
step through that controller-managed workflow.
Once the eviction step is complete and the retry step reaches `ready-to-retry`, you can materialize
the next scheduler generation with
`comet-controller apply-ready-rollout-action --id ... --db ... [--artifacts-root ...]`.
That controller command removes the recorded best-effort victims from desired state, applies the
deferred placement retry, bumps desired generation, regenerates rollout actions for the new
generation, and emits fresh host assignments without the old scheduler gate.

There is now an explicit controller/hostd eviction path for those deferred scheduler decisions:

- `comet-controller enqueue-rollout-eviction --id ... --db ...` enqueues node-local
  `evict-workers` host assignments for the recorded victim workers
- `comet-hostd apply-next-assignment --node ...` applies that temporary eviction state on the
  affected node
- `comet-controller reconcile-rollout-actions --db ... [--artifacts-root ...]` observes applied
  eviction assignments, advances rollout action lifecycle automatically, and materializes the
  deferred `retry-placement` once the eviction step is complete

That means the scheduler rollout path is no longer only a controller-side state transition: it now
has an explicit orchestration seam between persisted rollout actions and node-level host
assignments, with controller-managed progression after the eviction step has actually been applied.

Preemption selection is also stricter now: placement and rollout logic no longer assumes "evict
all best-effort workers on the GPU". Instead, the scheduler computes an ordered victim set and
chooses the minimal set of best-effort workers needed to satisfy fraction and memory requirements
for the candidate placement.

This is still a controller-side admission layer, not a full dynamic balancer yet, but it means
soft-share policy is now encoded in desired state rather than inferred only from `gpu_fraction`,
and the controller can already surface idle capacity next to active soft-share groups plus
candidate moves that would reduce sharing pressure.

Show the reconcile plan against the current SQLite state:

```bash
./build/linux/x64/comet-controller plan-bundle --bundle config/demo-plane --db var/controller.sqlite
```

Show the per-node host operations implied by the bundle:

```bash
./build/linux/x64/comet-controller plan-host-ops --bundle config/demo-plane --db var/controller.sqlite --artifacts-root var/artifacts
```

Show the desired state currently stored in SQLite:

```bash
./build/linux/x64/comet-controller show-state --db var/controller.sqlite
```

Render the compose file for all demo nodes:

```bash
./build/linux/x64/comet-controller render-demo-compose
```

Render the compose file from SQLite-backed desired state:

```bash
./build/linux/x64/comet-controller render-compose --db var/controller.sqlite
```

Preview the compose output directly from a bundle before import:

```bash
./build/linux/x64/comet-controller preview-bundle --bundle config/demo-plane --node node-a
```

Apply a bundle into controller state with a reconcile summary:

```bash
./build/linux/x64/comet-controller apply-bundle --bundle config/demo-plane --db var/controller.sqlite --artifacts-root var/artifacts
```

Render the infer runtime manifest from SQLite-backed desired state:

```bash
./build/linux/x64/comet-controller render-infer-runtime --db var/controller.sqlite
```

Run the controller in HTTP server mode and query the first health endpoints:

```bash
./build/linux/x64/comet-controller serve --db var/controller.sqlite --listen-host 127.0.0.1 --listen-port 18080
curl http://127.0.0.1:18080/health
curl http://127.0.0.1:18080/api/v1/health
curl http://127.0.0.1:18080/api/v1/state
curl http://127.0.0.1:18080/api/v1/host-assignments
curl http://127.0.0.1:18080/api/v1/host-observations
curl http://127.0.0.1:18080/api/v1/host-health
curl http://127.0.0.1:18080/api/v1/node-availability
curl http://127.0.0.1:18080/api/v1/disk-state
curl http://127.0.0.1:18080/api/v1/rollout-actions
curl http://127.0.0.1:18080/api/v1/rebalance-plan
curl http://127.0.0.1:18080/api/v1/events
curl -N http://127.0.0.1:18080/api/v1/events/stream
curl -X POST "http://127.0.0.1:18080/api/v1/scheduler-tick?artifacts_root=$(pwd)/var/artifacts"
curl -X POST "http://127.0.0.1:18080/api/v1/reconcile-rebalance-proposals?artifacts_root=$(pwd)/var/artifacts"
curl -X POST "http://127.0.0.1:18080/api/v1/reconcile-rollout-actions?artifacts_root=$(pwd)/var/artifacts"
curl -X POST "http://127.0.0.1:18080/api/v1/bundles/validate?bundle=$(pwd)/config/demo-plane"
curl -X POST "http://127.0.0.1:18080/api/v1/bundles/preview?bundle=$(pwd)/config/demo-plane&node=node-a"
curl -X POST "http://127.0.0.1:18080/api/v1/bundles/import?bundle=$(pwd)/config/demo-plane"
curl -X POST "http://127.0.0.1:18080/api/v1/bundles/apply?bundle=$(pwd)/config/demo-plane&artifacts_root=$(pwd)/var/artifacts"
curl -X POST "http://127.0.0.1:18080/api/v1/apply-rebalance-proposal?worker=worker-b&artifacts_root=$(pwd)/var/artifacts"
curl -X POST "http://127.0.0.1:18080/api/v1/set-rollout-action-status?id=1&status=acknowledged&message=api-http"
curl -X POST "http://127.0.0.1:18080/api/v1/enqueue-rollout-eviction?id=1"
curl -X POST "http://127.0.0.1:18080/api/v1/apply-ready-rollout-action?id=2&artifacts_root=$(pwd)/var/artifacts"
curl -X POST "http://127.0.0.1:18080/api/v1/node-availability?node=node-b&availability=unavailable&message=api-http"
curl -X POST "http://127.0.0.1:18080/api/v1/retry-host-assignment?id=7"
```

This is the first Phase F API seam: a native HTTP listener inside `comet-controller` with JSON
health output plus read-only controller surfaces for state, assignments, observations, health,
storage, rollout actions, and rebalance state. It now also exposes the first safe orchestration
mutations over HTTP for `scheduler-tick`, `reconcile-rebalance-proposals`,
`reconcile-rollout-actions`, bundle `validate/preview/import/apply`, `apply-rebalance-proposal`,
`set-rollout-action-status`, `enqueue-rollout-eviction`, `apply-ready-rollout-action`,
`node-availability`, and `retry-host-assignment`. The broader mutating REST surface still follows
in later Phase F slices.

The same binary now also works as a thin operator CLI over HTTP for the common inspection and
mutation flows:

```bash
./build/linux/x64/comet-controller show-state --controller http://127.0.0.1:18080
./build/linux/x64/comet-controller show-host-assignments --controller http://127.0.0.1:18080 --node node-a
./build/linux/x64/comet-controller show-node-availability --controller http://127.0.0.1:18080 --node node-b
./build/linux/x64/comet-controller validate-bundle --controller http://127.0.0.1:18080 --bundle "$(pwd)/config/demo-plane"
./build/linux/x64/comet-controller set-node-availability --controller http://127.0.0.1:18080 --node node-b --availability draining --message cli-http
COMET_CONTROLLER=http://127.0.0.1:18080 ./build/linux/x64/comet-controller show-state
```

Target resolution order is:

- `--controller <http://host:port>`
- `COMET_CONTROLLER`
- `~/.config/comet/controller`

If `--db` is provided, the command stays local and does not route through HTTP.

The current API contract now also includes:

- `api_version` on JSON responses
- `request.path` and `request.method` on JSON responses
- normalized error payloads through `status=error`, `error.code`, and `error.message`

Inspect the per-node assignments queued for `hostd`:

```bash
./build/linux/x64/comet-controller show-host-assignments --db var/controller.sqlite
```

Inspect the last observed heartbeat and applied state summary reported by each host:

```bash
./build/linux/x64/comet-controller show-host-observations --db var/controller.sqlite
```

Inspect the persisted structured event log for recent host and scheduler transitions:

```bash
./build/linux/x64/comet-controller show-events --db var/controller.sqlite --limit 20
```

There is also a dedicated live validation harness for Phase G telemetry and events:

```bash
./scripts/check-live-phase-g.sh --skip-build
```

The first Phase H realtime seam now exists through SSE on top of the persisted controller
`event_log`:

```bash
curl -N http://127.0.0.1:18080/api/v1/events/stream
curl -N -H "Last-Event-ID: 42" "http://127.0.0.1:18080/api/v1/events/stream?category=bundle"
```

The stream currently emits persisted controller/hostd events with:

- `id`
- `event: <category>.<event_type>`
- JSON `data:` payload matching the `/api/v1/events` item shape

Phase H also now has a first compact browser-facing aggregate read model:

```bash
curl http://127.0.0.1:18080/api/v1/dashboard
```

`GET /api/v1/dashboard` returns a compact JSON document with plane summary, node summary,
runtime readiness counts, assignment summary, rollout summary, and recent events so the first
browser UI does not need to stitch together many low-level endpoints.

The controller can now also serve static frontend assets directly:

```bash
./build/linux/x64/comet-controller serve --db var/controller.sqlite --ui-root ui/operator
```

When `--ui-root` contains `index.html`, non-API browser paths are resolved from that directory,
static assets such as `/assets/app.js` are served directly, and unknown non-API paths fall back
to `index.html`.

This seam is now considered a development fallback only.

The production React workspace now lives under
[ui/operator-react](/mnt/e/dev/Repos/comet-node/ui/operator-react). For local UI
iteration without the sidecar image:

```bash
cd ui/operator-react
npm install
npm run build
cd ../..
./build/linux/x64/comet-controller serve --db var/controller.sqlite --ui-root ui/operator-react/dist
```

The updated Phase H production direction is:

- a separate global `comet-web-ui` container on the controller host
- browser origin terminates at `comet-web-ui`, not at `comet-controller`
- `comet-web-ui` serves the operator UI and reverse-proxies `/api/*` and
  `/api/v1/events/stream` into the controller
- the production UI is now sourced from the React workspace, with `Node.js` allowed only as a build-time toolchain

The controller now also has the first sidecar-management seam for that production path:

```bash
./build/linux/x64/comet-controller ensure-web-ui --db var/controller.sqlite --web-ui-root var/web-ui --listen-port 18081 --controller-upstream http://host.docker.internal:18080 --compose-mode skip
./build/linux/x64/comet-controller show-web-ui-status --web-ui-root var/web-ui
./build/linux/x64/comet-controller stop-web-ui --db var/controller.sqlite --web-ui-root var/web-ui --compose-mode skip
```

`ensure-web-ui` materializes controller-local compose/config artifacts for the `comet/web-ui:dev`
sidecar. That image now packages a prebuilt React bundle from
`ui/operator-react/dist` into a minimal nginx runtime stage, so the standard build flow is:

```bash
cd ui/operator-react
npm run build
cd ../..
docker build -f runtime/web-ui/Dockerfile -t comet/web-ui:dev .
```

`--compose-mode exec` uses Docker Compose to start or stop the sidecar; `skip` keeps the same
code path but only renders lifecycle artifacts for smoke/dev environments.

There is now also a live validation harness for the full browser-facing path:

```bash
./scripts/check-live-phase-h.sh
```

It validates:

- `comet/web-ui:dev` image build
- controller-side sidecar lifecycle
- proxied REST through `comet-web-ui`
- proxied SSE through `comet-web-ui`
- browser-facing `stop-plane` / `start-plane` transitions through the sidecar path

The current [ui/operator/index.html](/mnt/e/dev/Repos/comet-node/ui/operator/index.html) remains useful for:

- local development
- smoke validation
- backend surface inspection as a plain non-React fallback

## Production Deployment (Phase K)

`comet-node` is now the public deployment entrypoint. The intended production shape is:

- machine A: `comet-node` in `controller` mode, optionally with local `hostd` and `comet-web-ui`
- machine B: `comet-node` in `hostd` mode on a GPU node
- remote `hostd` dials out to the controller and exchanges assignments, telemetry, events, and disk runtime state through controller-owned host-agent APIs

Primary user flow:

```bash
./scripts/install-single-node.sh
./scripts/run-plane.sh qwen35-9b-min
```

Then:

- the install command builds binaries and runtime images, installs `controller + local hostd`
  as system services, and starts them
- on GPU hosts, the install command also installs a CUDA toolkit automatically when `nvidia-smi`
  is present but `nvcc` is not yet available
- the run command loads `config/<plane>/desired-state.json` by default, but it can also take
  `--desired-state <path>` or auto-discover a sibling app-owned config at
  `../<plane>/deploy/comet-node/desired-state.json`
- this lets application repos such as `lt-cypher-ai` own their own canonical plane config
- a plane desired-state can optionally declare `post_deploy_script`; when present, `hostd`
  resolves that path from the plane-owning repository and runs it after infer and worker report
  ready, so app-specific restore/setup steps happen only after the runtime is warm
- `config/comet-node-config.json` is the machine-level storage config for managed disks and
  heavy cached model files

External agent/client integrations should target the controller-owned plane interaction contract
documented in [docs/external-inference-contract.md](./docs/external-inference-contract.md).
The raw plane `/v1/*` gateway is a runtime implementation detail, not the supported external API.

Single-node planes should use explicit node names in desired state and bundle configs:

- default local host name is `local-hostd` when `--node` is not provided
- set `inference.primary_infer_node`, `nodes[].name`, `disks[].node_name`,
  `instances[].node_name`, and `runtime_gpu_nodes[].node_name` explicitly
- `placement_target` is deprecated and no longer used by example configs

Large local models can now be referenced directly instead of copied into plane-owned storage.
For example, a plane can point at a preloaded shared-storage model directory with:

```json
"bootstrap_model": {
  "model_id": "Qwen/Qwen3.5-122B-A10B-FP8",
  "materialization_mode": "reference",
  "local_path": "/mnt/shared-storage/models/vllm/Qwen/Qwen3.5-122B-A10B-FP8",
  "served_model_name": "qwen3.5-122b-a10b"
}
```

`materialization_mode: "reference"` tells `hostd` to verify the path and publish it into
`active-model.json` without copying the model into the plane shared disk. This is the preferred
path for very large local directory models such as vLLM/Hugging Face snapshots. The existing
default remains `materialization_mode: "copy"` for GGUF files and smaller artifacts.

The operator UI now also has a model-library surface:

- list discovered local models through `GET /api/v1/model-library`
- enqueue downloads through `POST /api/v1/model-library/download`
- delete an unreferenced model through `DELETE /api/v1/model-library`
- enter one `source_url` or multiple `source_urls[]` values for multi-file model downloads
- protect models that are referenced by an active plane from accidental deletion

The plane detail UI now splits runtime data into `Status` and `Interaction` tabs for LLM planes.
The left sidebar also includes a `Models` panel for browsing discovered local models and active
download jobs.

For CPU-only validation on a host where the worker runtime should not offload layers to GPU yet:

```bash
./scripts/run-plane.sh qwen35-9b-min --cpu
```

For single-node local demo work, you no longer need to force a bundle-specific node name like
`node-a`. The default local install path uses `local-hostd`, and example planes should keep
their node references explicit in the desired state instead of using deprecated placement aliases.

Primary remote hostd flow:

```bash
./build/linux/x64/comet-node install controller --with-web-ui --skip-systemctl
./build/linux/x64/comet-node run controller

./build/linux/x64/comet-node install hostd --controller http://controller:18080 --node gpu-b --skip-systemctl
./build/linux/x64/comet-node connect-hostd --db /var/lib/comet-node/controller.sqlite --node gpu-b --public-key /var/lib/comet-node/keys/hostd.pub.b64
./build/linux/x64/comet-node run hostd
```

Service and diagnostics flow:

```bash
./build/linux/x64/comet-node service verify controller-hostd --skip-systemctl
./build/linux/x64/comet-node service verify hostd --skip-systemctl
./build/linux/x64/comet-controller show-hostd-hosts --db var/controller.sqlite
./build/linux/x64/comet-controller apply-bundle --bundle config/demo-plane --db var/controller.sqlite --artifacts-root var/artifacts
```

There is now also a dedicated live validation harness for the simplified startup UX:

```bash
./scripts/check-live-phase-l.sh --skip-build --skip-image-build
```

It validates the primary user flow:

- install controller with local `hostd` and `comet-web-ui`
- run controller with defaults
- open the Web UI
- preview and apply the first plane through the browser-facing API path
- see local `hostd` materialize state after plane apply

`--skip-image-build` is valid when `comet/web-ui:dev` is already present locally. The harness
is wrapped by a macOS-only entrypoint as well:

```bash
./scripts/check-macos.sh --skip-image-build
```

It builds the host debug target, verifies the launcher install flow with
`service verify --skip-systemctl`, and then runs the live Phase L validation.
uses `--hostd-compose-mode skip`, so it validates first-use onboarding and local state
materialization without requiring infer/worker runtime images.

Host-agent security includes:

- long-lived host identity keys
- signed session open
- encrypted request/response envelopes
- replay guards via sequence numbers
- session TTL and rekey threshold
- host revoke and host-key rotation

Registry inspection and control:

```bash
./build/linux/x64/comet-controller show-hostd-hosts --db var/controller.sqlite
./build/linux/x64/comet-controller revoke-hostd --db var/controller.sqlite --node gpu-b
./build/linux/x64/comet-controller rotate-hostd-key --db var/controller.sqlite --node gpu-b --public-key /path/to/new-hostd.pub.b64
```

The browser-facing runtime architecture is now:

- `Level 0`: `comet-web-ui`
- `Level 1`: `comet-controller`
- `Level 2`: infer runtime
- `Level 3`: worker runtime

The updated Phase H backend also needs:

- multi-plane controller/store/planner support
- plane lifecycle operations such as `start-plane` and `stop-plane`
- `stop-plane = scale-to-zero, keep config`
- CPU telemetry as part of the browser-facing observability surface

Show controller-side host health, including stale-heartbeat detection:

```bash
./build/linux/x64/comet-controller show-host-health --db var/controller.sqlite --stale-after 300
```

Inspect explicit operator overrides for node availability:

```bash
./build/linux/x64/comet-controller show-node-availability --db var/controller.sqlite
```

Mark a node as unavailable so new host assignments stop targeting it:

```bash
./build/linux/x64/comet-controller set-node-availability --db var/controller.sqlite --node node-b --availability unavailable --message "maintenance window"
```

Moving a node out of `active` now also queues a node-local drain assignment so `hostd` can
remove workloads from that node before it is considered fully out of rotation.

When a node returns to `active`, `comet-controller` now auto-enqueues a resync assignment for the
current desired generation if that node missed the latest rollout while it was out of rotation.

Requeue a failed assignment after fixing the underlying issue:

```bash
./build/linux/x64/comet-controller retry-host-assignment --db var/controller.sqlite --id 2
```

Render the compose file for a single node:

```bash
./build/linux/x64/comet-controller render-demo-compose --node node-a
```

Show the local operations `hostd` would perform on one node from demo state:

```bash
./build/linux/x64/comet-hostd show-demo-ops --node node-a
```

Show the local operations `hostd` would perform for one node from controller SQLite state:

```bash
./build/linux/x64/comet-hostd show-state-ops --node node-a --db var/controller.sqlite --artifacts-root var/artifacts --state-root var/hostd-state
```

Inspect the local applied-state snapshot for a node:

```bash
./build/linux/x64/comet-hostd show-local-state --node node-a --state-root var/hostd-state
```

Inspect the infer-side dry-run runtime snapshot materialized under the node's shared disk:

```bash
./build/linux/x64/comet-hostd show-runtime-status --node node-a --state-root var/hostd-state
```

Publish a manual heartbeat from `hostd` back into controller SQLite:

```bash
./build/linux/x64/comet-hostd report-observed-state --node node-a --db var/controller.sqlite --state-root var/hostd-state
```

Apply the local node operations with safe path rebasing for a test runtime root:

```bash
./build/linux/x64/comet-hostd apply-state-ops --node node-a --db var/controller.sqlite --artifacts-root var/artifacts --runtime-root var/runtime --state-root var/hostd-state --compose-mode skip
```

Apply the same node operations and let `hostd` run real `docker compose up -d`:

```bash
./scripts/build-runtime-images.sh
./build/linux/x64/comet-hostd apply-state-ops --node node-a --db var/controller.sqlite --artifacts-root var/artifacts --runtime-root var/runtime --state-root var/hostd-state --compose-mode exec
```

Claim and apply the next queued assignment for one node:

```bash
./build/linux/x64/comet-hostd apply-next-assignment --node node-a --db var/controller.sqlite --runtime-root var/runtime --state-root var/hostd-state --compose-mode skip
```

`apply-bundle` now materializes both per-node `docker-compose.yml` files and a plane-level
`infer-runtime.json` under `var/artifacts/<plane>/`. That JSON intentionally mirrors the
legacy `render-infer-config.sh` output shape closely enough for the next runtime migration
steps around native `llama.cpp` startup.

When `hostd` applies the infer node state, it also writes that runtime manifest into
`<shared-disk>/control/<plane>/infer-runtime.json`, which matches `control_root` inside the
container. The infer entrypoint now advertises `COMET_INFER_RUNTIME_CONFIG` /
`COMET_CONTROL_ROOT` and defaults to `COMET_INFER_BOOT_MODE=launch-runtime`. That path goes
through `comet-inferctl`, which now links `llama.cpp` directly as a library and can bring up
an in-process inference backend as soon as the active model resolves to a local GGUF file.

There is now a first infer-side runtime helper at `runtime/infer/inferctl.sh`:

```bash
./runtime/infer/inferctl.sh list-profiles
./runtime/infer/inferctl.sh validate-config --config var/artifacts/alpha/infer-runtime.json
./runtime/infer/inferctl.sh bootstrap-runtime --config var/artifacts/alpha/infer-runtime.json --profile generic
./runtime/infer/inferctl.sh doctor --config var/artifacts/alpha/infer-runtime.json
./runtime/infer/inferctl.sh preload-model --config var/artifacts/alpha/infer-runtime.json --alias qwen35 --source-model-id Qwen/Qwen3.5-7B-Instruct --local-model-path /tmp/qwen35
./runtime/infer/inferctl.sh cache-status --config var/artifacts/alpha/infer-runtime.json --alias qwen35 --local-model-path /tmp/qwen35
./runtime/infer/inferctl.sh switch-model --config var/artifacts/alpha/infer-runtime.json --model-id Qwen/Qwen3.5-7B-Instruct --runtime-profile qwen3_5 --tp 1 --pp 1
./runtime/infer/inferctl.sh show-active-model --config var/artifacts/alpha/infer-runtime.json
./runtime/infer/inferctl.sh gateway-plan --config var/artifacts/alpha/infer-runtime.json
./runtime/infer/inferctl.sh gateway-status --config var/artifacts/alpha/infer-runtime.json
./runtime/infer/inferctl.sh launch-runtime --config var/artifacts/alpha/infer-runtime.json --backend llama
./runtime/infer/inferctl.sh status --config var/artifacts/alpha/infer-runtime.json
./runtime/infer/inferctl.sh stop --config var/artifacts/alpha/infer-runtime.json
./runtime/infer/inferctl.sh prepare-runtime --config var/artifacts/alpha/infer-runtime.json
./runtime/infer/inferctl.sh plan-launch --config var/artifacts/alpha/infer-runtime.json
```

For host-local `--apply` runs, use a writable copy of `infer-runtime.json` whose `control_root`
and cache/log paths point at a local directory such as `/tmp/comet-infer-model-state/`. When the
host-visible GGUF path differs from the path seen by the runtime container, pass
`--runtime-model-path /comet/shared/.../model.gguf` to `preload-model` so the active model state
stores the runtime-visible mount path instead of the host staging path. `cache-status` and
`show-active-model` become useful after those state files are materialized.

Those commands now cover:
- bundled runtime profile discovery
- config validation
- dry-run bootstrap/profile planning
- local preflight checks
- model-cache registry planning under `control_root`
- active-model manifest planning under `control_root`
- gateway manifest planning under `control_root`
- combined runtime status/readiness reporting
- dry-run stop/reset of active runtime manifests
- directory preparation
- `llama.cpp` / gateway launch planning
- native C++ launch with real `/health`, `/v1/models`, `/v1/completions`, and `/v1/chat/completions` endpoints

`preload-model --apply` and `switch-model --apply` only write local state files under
`control_root` plus a cache marker under the requested model directory. `gateway-plan --apply`
and `status --apply` only write local gateway/runtime manifests. `stop --apply` only clears
those runtime manifests and refreshes the shared runtime snapshot. They still do not download
models by themselves; they prepare the state consumed by the native `llama.cpp` runtime.

When `status --apply` writes `runtime-status.json` under the infer node shared disk, `hostd`
can read the same snapshot through `show-runtime-status`, so the node agent and infer-side
helper now look at one shared runtime state. A follow-up
`comet-hostd report-observed-state` publishes that same runtime snapshot back into controller
SQLite, so `comet-controller show-host-observations` and `show-host-health` can surface
runtime readiness alongside the normal applied-state heartbeat.

`launch-runtime` is now the bridge between dry-run and full runtime. It prefers the in-process
`llama.cpp` backend whenever the active model points at a local GGUF file and keeps
`runtime-status.json` updated with fields such as `runtime_backend`, `runtime_phase`,
`inference_ready`, `gateway_ready`, `started_at`, and `supervisor_pid`.

`hostd` tracks assignment attempts in SQLite. A failed claimed assignment is returned to
`pending` until it exhausts its `max_attempts`, after which it becomes `failed` and can be
manually requeued with `retry-host-assignment`. `hostd` also reports observed status and
heartbeat data back into controller SQLite, so `comet-controller` can show the last known
applied generation and node-local state summary for each host, plus a derived host-health
view that marks stale heartbeats after a configurable threshold. Operator-side node
availability overrides (`active|draining|unavailable`) are stored separately and keep new
assignments away from nodes that were taken out of rotation.

## Repository Layout

- `controller/` - global control-plane executable
- `hostd/` - per-node host agent executable
- `common/` - shared domain models, planner helpers, compose renderer, infer runtime renderer
- `runtime/base/` - shared runtime container base image
- `runtime/infer/` - infer container entrypoint assets
- `runtime/worker/` - worker container entrypoint assets
- `scripts/` - low-level shell helpers
- `config/` - example plane bundles
- `migrations/` - SQLite schema migrations
- `docs/` - architecture notes
