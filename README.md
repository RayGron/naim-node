# naim-node

`naim-node` is the implementation repository for the `naim` platform.
At the product level, the platform is split into:

- `naim`: the control point and operator-facing management surface
- `naim-node`: the managed host agent package that runs on connected nodes

This repository currently contains both sides of that split through `naim-controller`,
`naim-hostd`, and the `naim-node` launcher. Together they manage plane state, node realization,
model workflows, telemetry, and authenticated interaction endpoints for both application-backed
planes and external clients such as Maglev.

The current primary runtime path is `llama.cpp + llama_rpc` with support for:

- single-replica LLM planes
- replica-parallel LLM planes with an aggregator head and leaf infer replicas
- backend-only inference planes for external clients
- application-attached LLM planes
- GPU worker planes without an infer service

`desired-state.v2.json` is the canonical plane format. The legacy bundle importer has been removed.

## What It Does

`naim-node` currently provides:

- a SQLite-backed `naim-controller` that implements the `naim` control point and stores desired
  state, rollout state, model-library jobs, auth data, plane metadata, and the connected-node
  registry
- a `naim-hostd` agent that implements `naim-node` and realizes compose artifacts, managed
  disks, runtime configs, and host telemetry on managed nodes
- secure outbound `naim-node -> naim` connectivity, so managed nodes can connect from behind NAT
  or firewalls without needing a permanent public IP
- node inventory scans on connect plus periodic rescans, with controller-derived `Storage` /
  `Worker` role assignment
- `llama.cpp + llama_rpc` orchestration for co-located, same-host, and split-host GPU layouts
- controller-owned interaction endpoints:
  - `/api/v1/planes/<plane>/interaction/status`
  - `/api/v1/planes/<plane>/interaction/models`
  - `/api/v1/planes/<plane>/interaction/chat/completions`
  - `/api/v1/planes/<plane>/interaction/chat/completions/stream`
- hidden-thinking support with request-level overrides and plane-level defaults
- an operator web UI with:
  - plane lifecycle controls
  - connected-node inventory, role, and plane-participation views
  - host and plane telemetry
  - live charts
  - model library management
  - plane creation and editing for `desired-state.v2`
- authenticated access through username/password bootstrap, SSH keys, and WebAuthn
- persistent model-library jobs with stop, resume, hide, delete, and restart recovery

## Current Architecture

At a high level:

- `naim` owns onboarding, desired state, scheduling, plane lifecycle, model-library workflows,
  auth, APIs, and the operator UI
- `naim-node` runs on managed hosts, scans local inventory, dials out to `naim`, and applies the
  control point's host assignments
- `naim-controller` and `naim-hostd` are the current implementation binaries behind that product
  split
- co-locating `naim` and `naim-node` on the same machine is a supported normal deployment shape
- runtime containers are materialized from rendered compose artifacts and per-instance runtime configs
- LLM planes use `llama.cpp` as the inference runtime and `llama_rpc` as the distributed backend by default

Canonical node-role rules are currently:

- `Storage`: no GPU, disk `> 100 GB`
- `Worker`: one or more GPUs, RAM `>= 32 GB`, disk `> 100 GB`

Storage capability is tracked separately from the single `derived_role`, so a `Worker` with
sufficient storage can also be storage-role eligible and operate in both roles when that storage
role is enabled.

Nodes that do not meet either rule stay connected and observable, but they are not eligible for
role-dependent placement until a later scan changes their inventory classification.

For replica-parallel `llama_rpc`, the effective instance topology is:

- `app` (optional)
- `aggregator`
- `infer` leaf replicas
- `worker` GPU containers

## Repository Layout

- `common/`: shared state models, JSON/projector/renderer code, planning, importing, SQLite support
- `controller/`: controller HTTP surface, auth, model library, interaction proxying, rollout orchestration
- `hostd/`: host agent, telemetry collection, disk lifecycle, local realization logic
- `runtime/`: runtime images and binaries for controller, hostd, infer, worker, skills, webgateway, base, and web UI
- `ui/operator-react/`: operator frontend
- `config/`: shipped example plane configs and runtime examples
- `scripts/`: build, smoke, benchmark, and convenience scripts
- `docs/`: benchmark notes and design/process documents

## Canonical Plane Format

The only supported plane spec is `desired-state.v2.json`.

Shipped examples live under [`config/`](./config):

- [`v2-llm-backend-only`](./config/v2-llm-backend-only)
- [`v2-llm-with-app`](./config/v2-llm-with-app)
- [`v2-llama-rpc-backend`](./config/v2-llama-rpc-backend)
- [`v2-llama-rpc-replicas`](./config/v2-llama-rpc-replicas)
- [`v2-maglev-35b-2gpu`](./config/v2-maglev-35b-2gpu)
- [`v2-gpu-worker`](./config/v2-gpu-worker)

See [`config/v2-examples.README.md`](./config/v2-examples.README.md) for a compact overview.

## Runtime Images

The repo ships runtime Dockerfiles for the current stack:

- [`runtime/base/Dockerfile`](./runtime/base/Dockerfile)
- [`runtime/controller/Dockerfile`](./runtime/controller/Dockerfile)
- [`runtime/hostd/Dockerfile`](./runtime/hostd/Dockerfile)
- [`runtime/infer/Dockerfile`](./runtime/infer/Dockerfile)
- [`runtime/worker/Dockerfile`](./runtime/worker/Dockerfile)
- [`runtime/skills/Dockerfile`](./runtime/skills/Dockerfile)
- [`runtime/browsing/Dockerfile`](./runtime/browsing/Dockerfile)
- [`runtime/web-ui/Dockerfile`](./runtime/web-ui/Dockerfile)

Build local dev images with:

```bash
./scripts/build-runtime-images.sh
```

The common dev image names are:

- `naim/base-runtime:dev`
- `naim/controller:dev`
- `naim/hostd:dev`
- `naim/infer-runtime:dev`
- `naim/worker-runtime:dev`
- `naim/skills-runtime:dev`
- `naim/webgateway-runtime:dev`
- `naim/web-ui:dev`

## Dependencies

External C/C++ dependencies are managed through `vcpkg` manifest mode.
Current manifest dependencies include:

- `sqlite3`
- `nlohmann-json`
- `libsodium`

The controller also links against OpenSSL Crypto for native C++ WebAuthn verification. On
Debian/Ubuntu hosts, `scripts/install-single-node.sh` installs `libssl-dev` with the other local
build prerequisites.

`llama.cpp` is pulled through CMake `FetchContent` at the revision pinned in `CMakeLists.txt`.

On macOS, install prerequisites first:

```bash
brew install pkg-config autoconf automake libtool libomp
```

## Build

Configure and build with the host-aware scripts:

```bash
./scripts/build-target.sh
```

That default is equivalent to a host build in `Debug`. The scripts now resolve host OS,
architecture, build directory, `vcpkg` toolchain, and CUDA/OpenMP hints automatically.

Build output paths are grouped by platform and architecture. By default they live under
`build/`, but you can relocate the build root with `NAIM_BUILD_ROOT`.

Examples:

- Linux x64: `build/linux/x64`
- Linux arm64: `build/linux/arm64`
- Windows x64: `build/windows/x64`
- macOS arm64: `build/macos/arm64`

You can also resolve a target build directory directly:

```bash
./scripts/print-build-dir.sh
```

Manual configure/build is still supported:

```bash
"$(./scripts/find-cmake.sh)" -S . -B "$(./scripts/print-build-dir.sh)"
"$(./scripts/find-cmake.sh)" --build "$(./scripts/print-build-dir.sh)"
```

Common variants:

```bash
./scripts/build-target.sh Release
./scripts/configure-build.sh
./scripts/package-target.sh
```

Explicit non-host targets still work:

```bash
./scripts/build-target.sh linux x64
./scripts/build-target.sh macos arm64 Release
./scripts/package-target.sh linux arm64
```

For the current multi-repo workspace and VS Code user-settings split, see
[`../naim-docs/process/build-vscode-setup.md`](../naim-docs/process/build-vscode-setup.md).

## Windows Builds

Windows builds use the native Windows toolchain even when started from a Remote WSL session:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts\windows\build-target.ps1
powershell.exe -ExecutionPolicy Bypass -File scripts\windows\build-target.ps1 -BuildType Release
powershell.exe -ExecutionPolicy Bypass -File scripts\windows\build-target.ps1 -TargetArch arm64 -BuildType Release
```

Windows packaging:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts\windows\package-target.ps1
```

## Quick Start

Resolve the current host build directory once:

```bash
BUILD_DIR="$(./scripts/print-build-dir.sh)"
```

Initialize a local controller DB:

```bash
"${BUILD_DIR}/naim-controller" init-db --db var/controller.sqlite
```

Validate a `desired-state.v2.json` bundle:

```bash
"${BUILD_DIR}/naim-controller" validate-bundle --bundle config/v2-llama-rpc-backend
```

Apply a v2 plane state directly:

```bash
"${BUILD_DIR}/naim-controller" apply-state-file \
  --db var/controller.sqlite \
  --artifacts-root var/artifacts \
  --state config/v2-llama-rpc-backend/desired-state.v2.json
```

Import a bundle directory that contains `desired-state.v2.json`:

```bash
"${BUILD_DIR}/naim-controller" import-bundle \
  --bundle config/v2-llama-rpc-backend \
  --db var/controller.sqlite
```

Run a plane and wait for interaction readiness:

```bash
./scripts/run-plane.sh v2-llama-rpc-backend
```

That helper applies the plane, starts it through the controller, and waits until
`interaction/status` reports ready.

## Interaction Contract

LLM planes expose a controller-owned OpenAI-compatible interaction surface.
Use the controller as the stable entrypoint rather than talking to raw infer gateways directly.

Core endpoints:

- `GET /api/v1/planes/<plane>/interaction/status`
- `GET /api/v1/planes/<plane>/interaction/models`
- `POST /api/v1/planes/<plane>/interaction/chat/completions`
- `POST /api/v1/planes/<plane>/interaction/chat/completions/stream`

Interaction policy is part of `desired-state.v2` and includes things such as:

- `thinking_enabled`
- default and supported response languages
- hidden-thinking behavior
- completion-policy limits
- system and analysis prompts

## Authentication And Access

The controller includes built-in auth and access control.
Current supported paths include:

- initial admin bootstrap
- username/password login
- SSH key registration and auth flows
- WebAuthn registration and login
- invite-based user registration

The operator UI and controller APIs use the same auth system. The UI uses
`@simplewebauthn/browser` for browser passkey calls only; server-side WebAuthn generation and
verification live in the C++ controller. Local deploys should not require manual npm installation
for authentication.

## Model Library

The model library is controller-backed and persistent.
It currently supports:

- discovery of cached models
- direct downloads into configured roots
- multipart downloads
- placement-aware model import onto connected `Storage` or `Worker` nodes with sufficient free
  capacity
- worker-only quantization target selection, where the selected `Worker` both stores and quantizes
  the model artifact
- stop, resume, hide, and delete operations
- persistent download jobs across controller restart
- progress reporting in the UI

The model-library HTTP routes are part of the controller API surface.

Current placement contract:

- model import requires an explicit destination node choice
- only nodes with enough free capacity for the full resulting artifact are shown as eligible
- non-quantized imports may target either `Storage` or `Worker`
- quantized imports may target only `Worker`

For plane-local support services, the current target contract is:

- the canonical `SkillsFactory` container lives on `naim`
- replicated `skills-<plane>` runtime containers live on the same machine as the plane's `app`
  container

## Telemetry And Dashboard

The operator UI now includes a real dashboard rather than a stub status page.
Current dashboard capabilities include:

- server telemetry summaries
- host cards with runtime, memory, GPU, disk, and temperature posture
- connected-node role, inventory, and plane participation visibility
- active plane overview
- plane detail modal with status and interaction views
- live charts for key server and plane metrics
- expandable telemetry charts that keep updating while open

Host telemetry includes CPU, memory, GPU, disk, and network summaries when available.

## Benchmarks And Smoke Checks

Useful current scripts:

- [`scripts/llama-rpc-smoke.sh`](./scripts/llama-rpc-smoke.sh)
- [`scripts/llama-rpc-replica-benchmark.sh`](./scripts/llama-rpc-replica-benchmark.sh)
- [`scripts/benchmark-data-parallel-throughput.sh`](./scripts/benchmark-data-parallel-throughput.sh)
- [`scripts/check-external-inference-contract.sh`](./scripts/check-external-inference-contract.sh)

These are the preferred paths for validating the active `llama_rpc` contract.

## Notes On Legacy Material

The repository still contains some older configs, scripts, and benchmark harnesses from earlier
phases. They remain useful as historical reference or narrow diagnostics, but they are not the
primary path anymore.

For new work:

- use `desired-state.v2.json`
- prefer `llama.cpp + llama_rpc`
- prefer controller-owned interaction endpoints
- treat the operator UI, node registry, and model library as first-class project surfaces
