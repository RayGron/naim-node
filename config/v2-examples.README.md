# Desired State v2 Examples

These compact `desired-state.v2.json` examples exercise the current renderer breadth.
All shipped LLM examples now use the `llama.cpp + llama_rpc` replica-parallel contract:

- `v2-llm-backend-only`:
  pure LLM backend with no application container, suitable for external clients such as Maglev
- `v2-gpu-worker`:
  compute plane with custom GPU workers and no model or infer service
- `v2-llm-with-app`:
  conventional LLM plane with an attached application container
- `v2-llama-rpc-backend`:
  single-replica `llama.cpp` head/runtime with RPC compute workers for local or remote GPU nodes
- `v2-llama-rpc-replicas`:
  replica-parallel `llama.cpp` RPC layout with one aggregator head and multiple leaf infer replicas

For a tiny same-host live smoke of the new `llama_rpc` path, run:

- `scripts/llama-rpc-smoke.sh`

For direct replica-parallel throughput experiments outside the full plane path, use:

- `scripts/llama-rpc-replica-benchmark.sh`

The renderer expands these compact specs into the current internal `DesiredState` shape in
`common/src/state/state_json.cpp`.
