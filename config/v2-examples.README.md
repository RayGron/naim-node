# Desired State v2 Examples

These compact `desired-state.v2.json` examples exercise the current renderer breadth:

- `v2-llm-backend-only`:
  pure LLM backend with no application container, suitable for external clients such as Maglev
- `v2-gpu-worker`:
  compute plane with custom GPU workers and no model or infer service
- `v2-llm-with-app`:
  conventional LLM plane with an attached application container

The renderer expands these compact specs into the current internal `DesiredState` shape in
`common/src/state/state_json.cpp`.
