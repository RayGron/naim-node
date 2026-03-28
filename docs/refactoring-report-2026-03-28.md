# Refactoring Report 2026-03-28

## Scope

Этот проход рефакторинга был выполнен по правилам из `docs/refactoring-rules.md` без изменения внешнего поведения:

- CLI surface сохранён
- HTTP endpoints не менялись
- DB schema не менялась
- desired-state schema не менялась

## Main Findings

Перед началом работ кодовая база нарушала несколько зафиксированных правил не локально, а системно:

1. В entrypoint-слое ещё оставался bootstrap inside `main`, особенно в `runtime/worker`.
2. В `controller/src/app/controller_composition_root.cpp` жили устойчивые policy/time helpers, что размывало границу между wiring и support logic.
3. В `hostd/src/app/hostd_app.cpp` были локальные orchestration classes, а также мёртвый дублирующий `DefaultHttpHostdBackendSupport`.
4. `runtime/infer` нарушал mirrored include/src layout: публичный заголовок `InferApp` жил в `src/`.

## Completed Refactoring Slices

### 1. Worker entrypoint normalized

`comet-workerd` переведён на явный object entrypoint:

- добавлен `WorkerApp`
- `runtime/worker/src/main.cpp` стал тонким
- bootstrap и обработка ошибок ушли из `main`

Это приводит `runtime/worker` к правилу:

- `main` только создаёт верхнеуровневый объект
- `main` только вызывает `Run()`

### 2. Controller composition root trimmed

Из `controller/src/app/controller_composition_root.cpp` вынесены устойчивые helper responsibilities:

- `ControllerLanguageSupport`
- `ControllerTimeSupport`

Теперь в composition root осталось меньше policy/support logic, а сами правила языка и времени стали явными именованными владельцами ответственности.

### 3. Hostd orchestration class extracted

Из giant file `hostd/src/app/hostd_app.cpp` вынесен отдельный orchestration class:

- `HostdCliActions`

Во втором срезе из того же giant file дополнительно вынесены отдельные владельцы ответственности:

- `HostdCompositionRoot`
- `DefaultHttpHostdBackendSupport`
- `DefaultHostdObservationSupport`
- `DefaultHostdAssignmentSupport`

Также удалён неиспользуемый локальный дубликат:

- `DefaultHttpHostdBackendSupport`

Это уменьшило объём локальной мини-архитектуры внутри `hostd_app.cpp` и убрало один очевидный structural smell.

### 4. Infer include layout normalized

Заголовок `InferApp` перенесён из:

- `runtime/infer/src/infer_app.h`

в mirrored layout:

- `runtime/infer/include/app/infer_app.h`

Сборка синхронизирована через `CMakeLists.txt`.

Во втором срезе из `runtime/infer/src/infer_app.cpp` также вынесен отдельный runtime component:

- `InferSignalService`
- `LlamaLibraryEngine`
- `LocalHttpServer`
- `LocalRuntime`

И в include layer вынесен явный runtime value owner:

- `RuntimeConfig`

Для этой декомпозиции также выделены отдельные shared runtime/http types и support seams:

- `HttpRequest`
- `SimpleResponse`
- `UpstreamTarget`
- `runtime_support::*`

### 5. SQLite support owner extracted

Из `common/src/state/sqlite_store.cpp` вынесен отдельный infrastructural owner:

- `SqliteStatement`

Следующим срезом из того же giant file вынесен первый repository owner:

- `AuthRepository`
- `EventRepository`
- `ObservationRepository`
- `AssignmentRepository`
- `SchedulerRepository`
- `DiskRuntimeRepository`
- `PlaneRepository`

Это переводит `sqlite_store` в более явный facade + repositories shape.

### 6. Controller scheduler factory extracted

Из `controller/src/app/controller_composition_root.cpp` вынесен отдельный factory owner:

- `ControllerSchedulerServiceFactory`

Дополнительно появился явный builder seam:

- `BuildControllerSchedulerService(...)`

### 7. Controller component factory extracted

В следующем срезе из `controller/src/app/controller_composition_root.cpp` вынесен локальный object-graph factory:

- `ControllerComponentFactory`

Чтобы не ломать существующий composition flow, рядом выделен явный support seam:

- `component_factory_support::*`

Это убрало локальный nested factory class из composition root и сделало controller wiring более именованным и проверяемым на уровне include/src layout.

### 8. Desired-state SQLite codec extracted

Из `common/src/state/sqlite_store.cpp` вынесен отдельный codec owner:

- `DesiredStateSqliteCodec`

Он взял на себя:

- serialization/deserialization для `BootstrapModelSpec`
- serialization/deserialization для `InferenceRuntimeSettings`
- serialization/deserialization для `GatewaySettings`
- serialization/deserialization для `RuntimeGpuNode`
- parse logic для `DiskKind`
- parse logic для `InstanceRole`

После этого `sqlite_store.cpp` стал ближе к роли facade + storage orchestration, а shared JSON/enum codec logic получила собственного владельца ответственности.

### 9. Composition and SQLite support helpers extracted

В следующем срезе из `controller/src/app/controller_composition_root.cpp` вынесен именованный support layer:

- `composition_support::*`

Он взял на себя:

- request query parsing
- JSON HTTP response normalization
- controller event append helpers
- plane-scoped observation filtering
- delete-plane finalization checks

Параллельно из `common/src/state/sqlite_store.cpp` вынесен отдельный SQLite utility/support layer:

- `sqlite_store_support::*`

Он взял на себя:

- column text/int extraction helpers
- raw sqlite exec helper
- schema column presence checks
- lazy column migration helper
- row mapping для `NodeAvailabilityOverride`
- raw handle cast helper

Это ещё сильнее сократило объём локального infrastructural glue в двух backlog-файлах.

### 10. HTTP serve orchestration and SQLite schema init extracted

В следующем срезе из `controller/src/app/controller_composition_root.cpp` вынесен отдельный HTTP serve owner:

- `serve_support::*`

Он взял на себя:

- pre-auth/post-auth route handler assembly
- `ControllerHttpRouter` wiring
- `ControllerHttpServer` wiring
- top-level HTTP serve config assembly

Параллельно из `common/src/state/sqlite_store.cpp` вынесен отдельный schema/bootstrap owner:

- `sqlite_store_schema::*`

Он взял на себя:

- schema bootstrap SQL execution orchestration
- lazy column migration orchestration
- backfill `desired_state_json`
- rollout-action `plane_name` backfill

После этого `ControllerCompositionRoot::Serve()` и `ControllerStore::Initialize()` стали заметно тоньше и ближе к orchestration-only роли.

### 11. Read-model builders and registered-host persistence extracted

В следующем срезе из `controller/src/app/controller_composition_root.cpp` вынесен отдельный read-model service-builder owner:

- `read_model_support::*`

Он взял на себя:

- `BundleHttpService` assembly
- `ReadModelService` assembly
- `ReadModelHttpService` wiring
- `SchedulerHttpService` wiring
- `ReadModelCliService` assembly

Параллельно из `common/src/state/sqlite_store.cpp` вынесен отдельный persistence owner:

- `RegisteredHostRepository`

Он взял на себя:

- `registered_hosts` upsert persistence
- single-host load persistence
- host-list load persistence

После этого `controller_composition_root.cpp` потерял ещё один cluster локальных `Make*` builders, а `sqlite_store.cpp` перестал держать inline SQL для `registered_hosts`.

### 12. Plane service builders and desired-state row loading extracted

В следующем срезе из `controller/src/app/controller_composition_root.cpp` вынесен отдельный plane/service-builder owner:

- `plane_support::*`

Он взял на себя:

- `ControllerPrintService` assembly
- `BundleCliService` assembly
- `PlaneMutationService` assembly
- `PlaneService` assembly
- `PlaneHttpService` wiring
- `HostRegistryService` assembly

Параллельно из `common/src/state/sqlite_store.cpp` вынесен отдельный persistence owner:

- `DesiredStateRepository`

Он взял на себя:

- latest-plane desired-state lookup
- full `DesiredState` row graph loading for planes, nodes, disks, instances, dependencies, labels, environment and published ports
- desired-state list loading
- desired-generation loading

После этого `controller_composition_root.cpp` потерял ещё один mutation/service-builder cluster, а `sqlite_store.cpp` избавился от основного desired-state row-loader слоя.

### 13. sqlite_store completed as a thin facade

В финальном срезе по `common/src/state/sqlite_store.cpp` вынесены оставшиеся persistence owners:

- `NodeAvailabilityRepository`
- `DesiredStateRepository` в расширенной роли
- `AssignmentRepository` в расширенной роли
- `sqlite_store_status_codec.cpp`

Они взяли на себя:

- `node_availability_overrides` upsert/load/list persistence
- full desired-state replace transaction
- latest-plane rebalance-iteration lookup
- supersede update для pending/claimed host assignments по plane
- status/enum string codecs для `HostAssignmentStatus`, `HostObservationStatus`, `NodeAvailability` и `RolloutActionStatus`

После этого `sqlite_store.cpp` больше не держит business SQL paths и фактически стал thin façade над repository/support слоями плюс lifecycle/bootstrap boundary.

### 14. controller/infer/hostd support slices

В следующем structural срезе закрыты три оставшихся helper-cluster фронта:

- из `controller/src/app/controller_composition_root.cpp` вынесен HTTP/service-builder support layer
- из `runtime/infer/src/infer_app.cpp` вынесен control/runtime metadata support layer
- из `hostd/src/app/hostd_app.cpp` вынесен local-state support layer

Новые owners:

- `comet::controller::http_service_support::*`
- `comet::infer::control_support::*`
- `comet::hostd::local_state_support::*`

Они взяли на себя:

- interaction/auth/hostd/model-library HTTP service assembly
- runtime profile/control-path loading и runtime metadata loading
- local applied-state, generation и runtime-status file orchestration на стороне hostd

После этого:

- `controller_composition_root.cpp` потерял ещё один HTTP builder cluster
- `infer_app.cpp` потерял control/runtime metadata helper cluster
- `hostd_app.cpp` перестал держать local-state persistence/orchestration cluster внутри giant file

## New/Updated Architectural Owners

После этого прохода явными owner-классами стали:

- `comet::worker::WorkerApp`
- `comet::controller::ControllerLanguageSupport`
- `comet::controller::ControllerComponentFactory`
- `comet::controller::ControllerSchedulerServiceFactory`
- `comet::controller::ControllerTimeSupport`
- `comet::controller::composition_support::*`
- `comet::controller::http_service_support::*`
- `comet::controller::plane_support::*`
- `comet::controller::read_model_support::*`
- `comet::controller::serve_support::*`
- `comet::hostd::HostdCliActions`
- `comet::hostd::HostdCompositionRoot`
- `comet::hostd::DefaultHttpHostdBackendSupport`
- `comet::hostd::DefaultHostdObservationSupport`
- `comet::hostd::DefaultHostdAssignmentSupport`
- `comet::hostd::local_state_support::*`
- `comet::infer::InferApp` in mirrored include layout
- `comet::infer::control_support::*`
- `comet::infer::InferSignalService`
- `comet::infer::LocalHttpServer`
- `comet::infer::LocalRuntime`
- `comet::infer::LlamaLibraryEngine`
- `comet::infer::RuntimeConfig`
- `comet::infer::cli_output_support::*`
- `comet::controller::component_factory_support::*`
- `ModelLibrarySupport`
- `ModelLibraryHttpSupport`
- `comet::hostd::HostdBootstrapModelSupport`
- `comet::AuthRepository`
- `comet::AssignmentRepository`
- `comet::DesiredStateRepository`
- `comet::DesiredStateSqliteCodec`
- `comet::DiskRuntimeRepository`
- `comet::EventRepository`
- `comet::NodeAvailabilityRepository`
- `comet::ObservationRepository`
- `comet::PlaneRepository`
- `comet::RegisteredHostRepository`
- `comet::SchedulerRepository`
- `comet::SqliteStatement`
- `comet::sqlite_store_schema::*`
- `comet::sqlite_store_support::*`

## Validation

### Build

Кумулятивно по проходам были успешно собраны таргеты:

- `comet-controller`
- `comet-hostd`
- `comet-node`
- `comet-inferctl`
- `comet-workerd`

Build tree:

- `build/local-debug`

В текущем срезе повторно подтверждены:

- `comet-controller`
- `comet-hostd`
- `comet-inferctl`
- `comet-node`

### Smoke

Проверены минимальные smoke scenarios:

- `comet-node version` -> success
- `comet-controller` -> usage output
- `comet-hostd` -> usage output
- `comet-inferctl` -> usage output
- `comet-workerd` -> стартует entrypoint и корректно падает на runtime environment issue (`/comet` read-only), что подтверждает рабочий bootstrap path

В текущем срезе повторно подтверждены usage-path smoke scenarios для:

- `comet-controller`
- `comet-hostd` (`error: --node is required`, что подтверждает рабочий CLI entry path)
- `comet-inferctl` (`error: config not found: /comet/shared/control/unknown/infer-runtime.json`, что подтверждает рабочий CLI/config path)
- `comet-node version`

## Remaining Refactoring Backlog

Этот проход уменьшил самые безопасные structural violations, но не закрывает весь backlog по правилам. Наибольшие оставшиеся hotspots:

1. `controller/src/app/controller_composition_root.cpp`
   `ControllerComponentFactory`, `composition_support::*`, `http_service_support::*`, `plane_support::*`, `serve_support::*`, `read_model_support::*` и `component_factory_support::*` уже вынесены; файл сокращён до thin composition root (`90` строк). Главный остаток теперь не в root, а в части helper/service surfaces внутри controller module.
2. `runtime/infer/src/infer_app.cpp`
   `InferSignalService`, `InferCommandLine`, `LlamaLibraryEngine`, `LocalHttpServer`, `LocalRuntime`, `control_support::*`, `cli_output_support::*`, `status_support::*` и `model_cache_support::*` уже вынесены, но HTTP handling, prompt shaping и runtime orchestration всё ещё живут в giant file (`1623` строки).
3. `hostd/src/app/hostd_app.cpp`
   `HostdCompositionRoot`, `HostdCliActions`, `local_state_support::*`, `HostdBootstrapModelSupport`, `controller_transport_support::*` и `telemetry_support::*` уже вынесены, файл уменьшен с `4531` до `2696` строк, но disk lifecycle и assignment execution orchestration всё ещё концентрируются в giant file.

## Latest Pass

В этом проходе дополнительно выполнено:

- `controller/src/app/controller_composition_root.cpp` распилен до thin root; service-builder graph вынесен в `controller/src/app/controller_component_factory_support.cpp`
- `controller/include/auth/auth_http_service.h` и `controller/include/interaction/interaction_http_service.h` больше не держат public callback-bag `Deps`; для них введены named support owners `AuthHttpSupport` и `InteractionHttpSupport`
- `controller/include/model/model_library_service.h` и `controller/include/model/model_library_http_service.h` больше не держат public callback-bag `Deps`; для них введены named support owners `ModelLibrarySupport` и `ModelLibraryHttpSupport`
- `controller/include/plane/plane_http_service.h`, `controller/include/read_model/read_model_http_service.h` и `controller/include/host/hostd_http_service.h` больше не держат public callback-bag `Deps`; для них введены named support owners `PlaneHttpSupport`, `ReadModelHttpSupport` и `HostdHttpSupport`
- `hostd/src/app/hostd_app.cpp` перестал держать bootstrap/download/checksum flow для plane model; этот orchestration owner вынесен в `hostd/src/app/hostd_bootstrap_model_support.cpp`
- `hostd/src/app/hostd_app.cpp` отдал controller transport layer в `hostd/src/app/hostd_controller_transport_support.cpp`
- `hostd/src/app/hostd_app.cpp` отдал telemetry/runtime-inspection layer в `hostd/src/app/hostd_telemetry_support.cpp` и уменьшился до `2696` строк
- `runtime/infer/src/infer_app.cpp` отдал CLI output/bootstrap display cluster в `runtime/infer/src/app/infer_cli_output_support.cpp`
- `runtime/infer/src/infer_app.cpp` отдал command/status/doctor/model-cache handlers в `runtime/infer/src/app/infer_status_support.cpp` и `runtime/infer/src/app/infer_model_cache_support.cpp` и уменьшился до `1623` строк

## Recommended Next Slice

Следующий логичный этап:

1. разрезать оставшиеся HTTP/prompt/runtime orchestration clusters в `runtime/infer/src/infer_app.cpp`
2. затем дожать `hostd/src/app/hostd_app.cpp` на disk lifecycle и assignment execution owners
3. после этого продолжить cleanup controller service surfaces, где ещё остались function-style helper seams
