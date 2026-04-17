# Architecture Notes

`naim-node` now sits inside a two-part product architecture:

- `naim` is the control point and operator-facing management surface
- `naim-node` is the managed host agent package that realizes work on connected nodes

This repository contains the current implementation of both sides:

- `naim-controller` is the current implementation of the `naim` control point
- `naim-hostd` is the current implementation of the `naim-node` host agent
- `naim-node` is the launcher that can install and run either role, including a co-located setup

The default remote-host pattern is no longer "controller reaches directly into every host". The
canonical deployment model is that `naim-node` dials out to `naim`, which allows managed nodes to
live behind NAT, firewalls, or dynamic addressing without requiring a permanent public IP.

## Product Split

`naim` owns:

- node onboarding and registry
- desired state, plane lifecycle, scheduling, and reconciliation
- model-library workflows, including download, conversion, and quantization orchestration
- authenticated HTTP APIs, operator UI, and browser-facing read models
- cluster telemetry aggregation, node role derivation, and plane participation views

`naim-node` owns:

- local inventory scans for CPU, RAM, disk, GPU, and runtime posture
- local realization of compose artifacts, runtime configs, disks, and containers
- node-local telemetry and observed-state reporting
- the secure outbound connection back to `naim`

## Node Onboarding And Roles

Managed node lifecycle:

1. the operator adds a node in `naim`
2. `naim` generates a random onboarding key for that node
3. the operator starts `naim-node` with that key in its local configuration
4. `naim-node` authenticates and opens the outbound channel to `naim`
5. if TLS/SSL already protects that channel, no extra stream-encryption layer is required
6. otherwise the host-agent channel must apply its own stream encryption
7. the node is scanned on connect and rescanned every hour
8. `naim` derives the node role from the latest observed inventory and may change that role after
   later rescans

Canonical role rules:

- `Storage`
  - no GPU
  - disk capacity `> 100 GB`
- `Worker`
  - one or more GPUs
  - RAM `>= 32 GB`
  - disk capacity `> 100 GB`

Storage capability is tracked independently from the single `derived_role`. A node can therefore
be derived as `Worker` and also be storage-role eligible when it has sufficient storage capacity.

Nodes that do not match either rule remain connected and observable, but they are not eligible for
role-dependent placement until later scans show a matching inventory.

## Placement Contracts

### Model Library

Model import and quantization are `naim` workflows, but they are realized on connected
`naim-node` hosts.

Current architectural contract:

- when a model is uploaded or imported, the operator chooses a destination node
- only nodes with enough free capacity for the full resulting artifact are eligible targets
- without quantization, eligible targets may be either `Storage` or `Worker`
- with quantization, eligible targets must be `Worker`
- when quantization is requested, the same `Worker` both stores and quantizes the model
- the node list shown in `naim` must expose role, capacity, telemetry, and current plane
  participation so that placement is explainable

### Plane Placement

Current plane deployment contract:

- each plane chooses an execution node when it is created or edited
- desired-state v2 stores this selection as `placement.execution_node`
- execution-node selection is not exclusive; multiple planes may run on the same connected node
- any connected node that passes role, capacity, and policy checks may accept a plane
- by default all plane containers run on that selected execution node
- `app` containers are the exception and may be deployed to an external host over SSH
- when a plane runs replicated `skills-<plane>` containers, they must live on the same machine as
  the plane's `app` container, or on the selected execution node when there is no external app host
- plane creation must capture the SSH address plus either a key path or username/password for that
  external app host
- plane creation still captures worker count and soft GPU allocation intent
- workers may share GPUs; the allocation is soft rather than exclusive

The current implementation keeps worker count and infer topology tightly coupled, but the hard
validator guarantee today is narrower: for replica-parallel `llama.cpp + llama_rpc`, `runtime.workers`
must be divisible by `infer.replicas`. Documentation should not overstate this as a universal
`worker_count == infer_count` invariant.

## Runtime And Data Flow

The main architecture seam is:

`desired state -> node registry + scheduler -> host assignments -> naim-node realization -> telemetry/observations -> controller-derived readiness`

The interaction seam remains controller-owned:

`client request -> naim controller session lookup/restore -> prompt reconstruction -> runtime inference -> controller persistence + response shaping`

Runtime containers are still materialized from controller-rendered artifacts and node-local runtime
configs, but the networked node registry is now a first-class architectural layer between
controller policy and runtime execution.

## Supported Topologies

The architecture must support all of these as normal cases:

- remote `naim` with many outbound-connected `naim-node` agents
- mixed clusters where some nodes are `Storage` and others are `Worker`
- a co-located install where `naim` and `naim-node` run on the same machine

In the co-located case, the machine participates in the node registry like any other managed node.
Co-location is a supported deployment shape, not a special debug-only shortcut.

## Related Long-Form Docs

The long-form canonical architecture set lives in [`../naim-docs/`](../naim-docs):

- [`overview/naim-node-overview.md`](../naim-docs/overview/naim-node-overview.md)
- [`design/naim-node-design.md`](../naim-docs/design/naim-node-design.md)
- [`architecture/detailed-architecture.md`](../naim-docs/architecture/detailed-architecture.md)
- [`architecture/component-reference.md`](../naim-docs/architecture/component-reference.md)
