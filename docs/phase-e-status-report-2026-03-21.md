# Phase E Status Report

Date: 2026-03-21

## Scope Reference

GitHub issue: `#5` `Phase E: Virtual disk lifecycle and storage productionization`

Phase E goal:

- replace the current directory-backed storage execution path with a real virtual disk lifecycle
- preserve the existing disk model and shared/private semantics
- keep SQLite authoritative for disk state

## Executive Summary

Phase E is not complete and should remain `planned`.

The storage model is already present in `comet`:

- desired state contains plane shared disks and infer/worker private disks
- SQLite persists those disk objects
- reconcile and host execution planning already understand per-node disk deltas
- `hostd` can create and remove managed disk paths during apply

But the actual execution layer is still directory-backed, not virtual-disk-backed:

- no loop image creation
- no filesystem formatting
- no attach/detach lifecycle
- no real mount management
- no mount verification or recovery logic

So the current codebase contains good Phase E foundations, but almost none of the production storage execution required by the issue acceptance criteria.

## What Is Already Implemented

### 1. Disk domain model exists

Implemented:

- `DiskKind` distinguishes `PlaneShared`, `InferPrivate`, `WorkerPrivate`
- `DiskSpec` models `name`, `plane_name`, `owner_name`, `node_name`, `host_path`, `container_path`, `size_gb`
- `DesiredState` carries `plane_shared_disk_name` and `disks`
- instances reference `private_disk_name` and `shared_disk_name`

Relevant files:

- [models.h](/mnt/e/dev/Repos/comet-node/common/include/comet/models.h)
- [state_json.cpp](/mnt/e/dev/Repos/comet-node/common/src/state_json.cpp)
- [import_bundle.cpp](/mnt/e/dev/Repos/comet-node/common/src/import_bundle.cpp)

Assessment:

- `completed` as Phase E foundation

### 2. SQLite is already authoritative for declared disk state

Implemented:

- `virtual_disks` table exists
- controller persists disk objects with `name`, `kind`, `node_name`, `host_path`, `container_path`, `size_gb`
- desired state reload reconstructs the disk list from SQLite

Relevant files:

- [0001_initial.sql](/mnt/e/dev/Repos/comet-node/migrations/0001_initial.sql)
- [sqlite_store.h](/mnt/e/dev/Repos/comet-node/common/include/comet/sqlite_store.h)
- [sqlite_store.cpp](/mnt/e/dev/Repos/comet-node/common/src/sqlite_store.cpp)

Assessment:

- `completed` for declared/controller state
- `not completed` for runtime attach/mount state, because SQLite does not yet track real device/mount lifecycle

### 3. Reconcile and node execution planning already understand disk deltas

Implemented:

- reconcile compares desired vs current `DiskSpec`
- node execution plan emits `EnsureDisk` and `RemoveDisk`
- planner binds disk host paths into compose volumes

Relevant files:

- [reconcile.cpp](/mnt/e/dev/Repos/comet-node/common/src/reconcile.cpp)
- [execution_plan.h](/mnt/e/dev/Repos/comet-node/common/include/comet/execution_plan.h)
- [execution_plan.cpp](/mnt/e/dev/Repos/comet-node/common/src/execution_plan.cpp)
- [planner.cpp](/mnt/e/dev/Repos/comet-node/common/src/planner.cpp)

Assessment:

- `completed` as orchestration groundwork

### 4. Hostd can materialize managed disk directories

Implemented:

- `EnsureDiskDirectory(...)` creates directories and writes `.comet-disk-info`
- `RemoveDiskDirectory(...)` deletes managed paths under `runtime_root`
- `hostd` persists local applied state and can reconcile later runs

Relevant files:

- [hostd/src/main.cpp](/mnt/e/dev/Repos/comet-node/hostd/src/main.cpp)

Assessment:

- `partial`
- this is lifecycle for directories, not for disk images or mounts

## What Is Only Partially Implemented

### 1. Disk lifecycle across restart and cleanup

What exists:

- local applied state is persisted
- execution planning can emit `EnsureDisk` and `RemoveDisk`
- cleanup removes managed directories under `runtime_root`

What is missing:

- detect and recover existing loop devices
- detect existing filesystems
- detect and repair missing mounts after restart
- distinguish image file removal from mountpoint cleanup
- detect stale attachments vs active mounts

Relevant files:

- [hostd/src/main.cpp](/mnt/e/dev/Repos/comet-node/hostd/src/main.cpp)
- [execution_plan.cpp](/mnt/e/dev/Repos/comet-node/common/src/execution_plan.cpp)

Assessment:

- `partial`

### 2. Disk tooling surface

What exists:

- helper script `comet-diskctl.sh`

What it actually does:

- `ensure-dir`
- `cleanup-dir`

What is missing:

- image creation
- filesystem formatting
- loop attach/detach
- mount/unmount
- state inspection

Relevant file:

- [comet-diskctl.sh](/mnt/e/dev/Repos/comet-node/scripts/comet-diskctl.sh)

Assessment:

- `partial`, but only as placeholder tooling

## What Is Not Implemented Yet

### 1. Real loop-image creation on Linux

Missing:

- image file allocation
- sparse/non-sparse policy
- deterministic image path layout
- size reconciliation for existing images

Issue scope status:

- `not completed`

### 2. Filesystem formatting and mount lifecycle

Missing:

- `mkfs` execution
- mountpoint creation and mount
- remount/idempotent mount detection
- unmount and teardown

Issue scope status:

- `not completed`

### 3. Attach/detach lifecycle tracking

Missing:

- loop device attach
- loop device detach
- persisted runtime state for device association
- reconciliation after host restart

Issue scope status:

- `not completed`

### 4. Mount ownership and cleanup

Missing:

- ownership metadata for mounted disk per node/instance
- cleanup rules for busy mounts
- safety checks preventing destructive detach
- stale mount recovery

Issue scope status:

- `not completed`

### 5. Acceptance criteria for real mounted volumes

Current state:

- infer private disks are directories
- worker private disks are directories
- plane shared disks are directories

Issue acceptance:

- they must become real mounted volumes

Issue scope status:

- `not completed`

## Phase E Acceptance Audit

### Acceptance: infer private disks are real mounted volumes

Status:

- `not met`

Reason:

- current infer private disk path is a managed directory, not a mounted filesystem

### Acceptance: worker private disks are real mounted volumes

Status:

- `not met`

Reason:

- current worker private disk path is a managed directory, not a mounted filesystem

### Acceptance: plane shared disks are real mounted volumes

Status:

- `not met`

Reason:

- current plane shared disk path is a managed directory, not a mounted filesystem

### Acceptance: hostd can reconcile disk lifecycle across restart and cleanup paths

Status:

- `partially met`

Reason:

- restart reconciliation exists for desired/local state
- real disk attach/mount reconciliation does not exist

### Acceptance: disk state stays queryable from controller state

Status:

- `met` for declared state
- `not met` for runtime disk/mount state

Reason:

- controller knows desired disk objects
- controller does not yet know image path, loop device, mount state, filesystem state, or attach owner at runtime

## Overall Assessment

### Completed

- disk model in desired state
- disk persistence in SQLite
- reconcile awareness of disks
- per-node execution planning for disks
- directory-backed host apply/remove path

### Partially completed

- restart/cleanup reconciliation, but only for managed directories and local state
- controller queryability, but only for declared disk objects

### Not completed

- loop-image lifecycle
- filesystem lifecycle
- attach/detach lifecycle
- mount lifecycle
- runtime disk state persistence
- mounted-volume acceptance criteria

## Recommended Phase E Plan

### E1. Introduce real disk runtime state

Add persisted runtime state for each disk:

- `image_path`
- `filesystem_type`
- `loop_device`
- `mount_point`
- `runtime_state`
- `attached_node`
- `attached_at`
- `mounted_at`
- `last_verified_at`

Store this in SQLite separately from desired disk spec, so controller and hostd can distinguish:

- declared disk object
- current realized disk lifecycle

### E2. Replace directory-backed `EnsureDisk` with disk lifecycle operations

Add Linux disk operations to `hostd`:

- create image file
- attach loop device
- format filesystem if absent
- mount filesystem
- verify mounted path

Replace `EnsureDiskDirectory(...)` with a real lifecycle function, while keeping the same node execution planning seam.

### E3. Add safe `RemoveDisk` teardown

For disk teardown:

- stop dependent services first
- unmount
- detach loop device
- optionally retain or remove image according to policy
- clear runtime state in SQLite

This should stay idempotent and restart-safe.

### E4. Add restart reconciliation

On `hostd` startup/apply:

- inspect existing image files
- inspect mounted filesystems
- inspect loop devices
- reconcile them back into runtime disk state
- repair missing mount/attach state when safe

This is the main productionization step for restart behavior.

### E5. Add controller-visible runtime disk reporting

Expose in controller output:

- per-disk desired state
- current runtime state
- mountpoint
- loop device
- health / drift

Commands that should surface this:

- `show-state`
- `show-host-observations`
- possibly a new `show-disk-state`

### E6. Add live Phase E validation

Live tests should prove:

- infer private disk mounts successfully
- worker private disk mounts successfully
- plane shared disk mounts successfully
- restart reconciliation works
- teardown and cleanup work
- containers see mounted volumes at stable container paths

## Proposed Sequencing

1. `E1` runtime disk state in SQLite
2. `E2` real ensure/create/attach/format/mount path
3. `E3` safe teardown/unmount/detach path
4. `E4` restart reconciliation
5. `E5` controller-visible reporting
6. `E6` live storage test campaign

## Recommended GitHub Status

Issue `#5` should remain:

- `state`: `open`
- `label`: `planned`

Reason:

- the production storage execution path has not started yet in the core acceptance areas
- existing code is best described as foundational Phase E groundwork, not Phase E implementation proper
