# Live Single-GPU Scheduler Report

Date: 2026-03-21

## Scope

Live validation of the Phase D deferred-preemption path on a host with one visible NVIDIA GPU.

Command executed:

```bash
./scripts/check-live-scheduler-single-gpu.sh --gguf /tmp/comet-gguf/tinyllama-15M-stories-Q3_K_M.gguf --skip-build-images
```

Host GPU:

- `NVIDIA GeForce RTX 5090`
- `32607 MiB`

Model:

- `tinyllama-15M-stories-Q3_K_M.gguf`

## Scenario

- `worker-a` started on `node-b:0` as `placement_mode=movable`, `share_mode=shared`, `gpu_fraction=0.75`
- `worker-b` started on `node-a:0` as `share_mode=best-effort`, `gpu_fraction=0.25`
- both workers consumed the same physical GPU through live `llama.cpp` worker runtimes
- controller exposed a deferred rollout for `worker-a` targeting `node-a:0`
- controller enqueued `evict-workers` for `worker-b`
- `hostd` applied and verified eviction using live GPU telemetry
- controller materialized `retry-placement` into desired generation `2`
- `hostd` applied generation `2` and converged the runtime to the winning placement

## Result

Status: `PASS`

Observed final state:

- `worker-a` running and ready on `node-a:0`
- `worker-b` absent from desired state and absent from live worker runtime observations
- `node-b` converged to an empty local instance set
- live GPU telemetry remained available through `nvml`

## Notes

- This validates the one-GPU scheduler shape from the architecture: multiple workers can contend for the same GPU, the controller can drive deferred preemption, and rollout can complete without requiring a second visible GPU.
- The separate two-GPU `safe-direct` live script is still useful, but it is no longer the only live Phase D harness.
