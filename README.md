# comet-node

`comet-node` is a C++ control plane for Docker-based LLM and GPU workloads.
It manages plane state, host realization, model downloads, telemetry, and authenticated
interaction endpoints for both application-backed planes and external clients such as Maglev.

The current primary runtime path is `llama.cpp + llama_rpc` with support for:

- single-replica LLM planes
- replica-parallel LLM planes with an aggregator head and leaf infer replicas
- backend-only inference planes for external clients
- application-attached LLM planes
- GPU worker planes without an infer service

`desired-state.v2.json` is the canonical plane format. The legacy bundle importer has been removed.

## What It Does

`comet-node` currently provides:

- a SQLite-backed `comet-controller` that stores desired state, rollout state, model-library jobs, auth data, and plane metadata
- a `comet-hostd` agent that realizes compose artifacts, managed disks, runtime configs, and host telemetry
- `llama.cpp + llama_rpc` orchestration for same-host and split-host GPU layouts
- controller-owned interaction endpoints:
  - `/api/v1/planes/<plane>/interaction/status`
  - `/api/v1/planes/<plane>/interaction/models`
  - `/api/v1/planes/<plane>/interaction/chat/completions`
  - `/api/v1/planes/<plane>/interaction/chat/completions/stream`
- hidden-thinking support with request-level overrides and plane-level defaults
- an operator web UI with:
  - plane lifecycle controls
  - host and plane telemetry
  - live charts
  - model library management
  - plane creation and editing for `desired-state.v2`
- authenticated access through username/password bootstrap, SSH keys, and WebAuthn
- persistent model-library jobs with stop, resume, hide, delete, and restart recovery

## Current Architecture

At a high level:

- `comet-controller` owns state, HTTP APIs, auth, plane interaction proxying, and the operator UI
- `comet-hostd` runs on nodes and applies the controller's host assignments
- runtime containers are materialized from rendered compose artifacts and per-instance runtime configs
- LLM planes use `llama.cpp` as the inference runtime and `llama_rpc` as the distributed backend by default

For replica-parallel `llama_rpc`, the effective instance topology is:

- `app` (optional)
- `aggregator`
- `infer` leaf replicas
- `worker` GPU containers

## Repository Layout

- `common/`: shared state models, JSON/projector/renderer code, planning, importing, SQLite support
- `controller/`: controller HTTP surface, auth, model library, interaction proxying, rollout orchestration
- `hostd/`: host agent, telemetry collection, disk lifecycle, local realization logic
- `runtime/`: runtime images and binaries for infer, worker, base, and web UI
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
- [`runtime/infer/Dockerfile`](./runtime/infer/Dockerfile)
- [`runtime/worker/Dockerfile`](./runtime/worker/Dockerfile)
- [`runtime/web-ui/Dockerfile`](./runtime/web-ui/Dockerfile)

Build local dev images with:

```bash
./scripts/build-runtime-images.sh
```

The common dev image names are:

- `comet/base-runtime:dev`
- `comet/infer-runtime:dev`
- `comet/worker-runtime:dev`
- `comet/web-ui:dev`

## Dependencies

External C/C++ dependencies are managed through `vcpkg` manifest mode.
Current manifest dependencies include:

- `sqlite3`
- `nlohmann-json`
- `libsodium`

`llama.cpp` is pulled through CMake `FetchContent` at the revision pinned in `CMakeLists.txt`.

On macOS, install prerequisites first:

```bash
brew install pkg-config autoconf automake libtool libomp
```

## Build

Configure and build with the host scripts:

```bash
./scripts/configure-host-build.sh Debug
cmake --build "$(./scripts/print-host-build-dir.sh)"
```

Build output paths are grouped by platform and architecture. By default they live under `build/`,
but you can relocate the build root with `COMET_BUILD_ROOT`.

Examples:

- Linux x64: `build/linux/x64`
- Linux arm64: `build/linux/arm64`
- Windows x64: `build/windows/x64`
- macOS arm64: `build/macos/arm64`

You can also resolve a target build directory directly:

```bash
./scripts/print-build-dir.sh linux x64
```

Manual configure/build is still supported:

```bash
cmake -S . -B build/linux/x64
cmake --build build/linux/x64
```

## Windows Builds

Windows builds use the native Windows toolchain even when started from a Remote WSL session:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts\windows\build-target.ps1 x64 Debug
powershell.exe -ExecutionPolicy Bypass -File scripts\windows\build-target.ps1 x64 Release
```

Windows packaging:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts\windows\package-target.ps1 x64
```

## Quick Start

Initialize a local controller DB:

```bash
./build/linux/x64/comet-controller init-db --db var/controller.sqlite
```

Validate a `desired-state.v2.json` bundle:

```bash
./build/linux/x64/comet-controller validate-bundle --bundle config/v2-llama-rpc-backend
```

Apply a v2 plane state directly:

```bash
./build/linux/x64/comet-controller apply-state-file \
  --db var/controller.sqlite \
  --artifacts-root var/artifacts \
  --state config/v2-llama-rpc-backend/desired-state.v2.json
```

Import a bundle directory that contains `desired-state.v2.json`:

```bash
./build/linux/x64/comet-controller import-bundle \
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

The operator UI and controller APIs use the same auth system.

## Model Library

The model library is controller-backed and persistent.
It currently supports:

- discovery of cached models
- direct downloads into configured roots
- multipart downloads
- stop, resume, hide, and delete operations
- persistent download jobs across controller restart
- progress reporting in the UI

The model-library HTTP routes are part of the controller API surface.

## Telemetry And Dashboard

The operator UI now includes a real dashboard rather than a stub status page.
Current dashboard capabilities include:

- server telemetry summaries
- host cards with runtime, memory, GPU, and temperature posture
- active plane overview
- plane detail modal with status and interaction views
- live charts for key server and plane metrics
- expandable telemetry charts that keep updating while open

Host telemetry includes CPU, memory, GPU, disk, and network summaries when available.

## Benchmarks And Smoke Checks

Useful current scripts:

- [`scripts/llama-rpc-smoke.sh`](./scripts/llama-rpc-smoke.sh)
- [`scripts/llama-rpc-replica-benchmark.sh`](./scripts/llama-rpc-replica-benchmark.sh)
- [`scripts/benchmark-data-parallel-throughput.py`](./scripts/benchmark-data-parallel-throughput.py)
- [`scripts/check-external-inference-contract.py`](./scripts/check-external-inference-contract.py)

These are the preferred paths for validating the active `llama_rpc` contract.

## Notes On Legacy Material

The repository still contains some older configs, scripts, and benchmark harnesses from earlier
phases. They remain useful as historical reference or narrow diagnostics, but they are not the
primary path anymore.

For new work:

- use `desired-state.v2.json`
- prefer `llama.cpp + llama_rpc`
- prefer controller-owned interaction endpoints
- treat the operator UI and model library as first-class project surfaces
