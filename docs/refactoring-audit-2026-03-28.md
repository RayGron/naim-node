# Comet-Node Refactoring Audit

Дата: `2026-03-28`

Этот файл фиксирует оставшиеся места в `comet-node`, которые после текущих срезов всё ещё не полностью соответствуют `docs/refactoring-rules.md`.

## Primary Remaining Hotspots

### 1. `controller/src/app/controller_composition_root.cpp`

Файл: `/Users/vladislavhalasiuk/Projects/Repos/comet-node/controller/src/app/controller_composition_root.cpp`

Текущее состояние:

- файл уже сокращён до thin composition root (`90` строк)
- main builder graph вынесен в `controller/src/app/controller_component_factory_support.cpp`
- главный остаток модуля теперь не giant root, а distributed helper/service seams вокруг него

Нарушения правил:

- Rule 4: controller composition уже значительно улучшен, но часть composition/service helper seams всё ещё живёт как free-function style support layer
- Rule 10: factory path всё ещё partially relies on helper namespaces instead of final owner objects

Следующий шаг:

- дожимать helper namespaces в сторону финальных owner objects там, где роли уже стабилизировались

### 2. `runtime/infer/src/infer_app.cpp`

Файл: `/Users/vladislavhalasiuk/Projects/Repos/comet-node/runtime/infer/src/infer_app.cpp`

Текущее состояние:

- файл остаётся giant file (`1623` строки)
- `Args` уже вынесен в `runtime/infer/src/app/infer_command_line.cpp`
- CLI output/bootstrap display cluster уже вынесен в `runtime/infer/src/app/infer_cli_output_support.cpp`
- command/status/doctor/model-cache handlers уже вынесены в `runtime/infer/src/app/infer_status_support.cpp` и `runtime/infer/src/app/infer_model_cache_support.cpp`
- внутри до сих пор живёт локальный `AssistantTextFilterState`
- orchestration flow, HTTP handling и prompt shaping всё ещё сосредоточены в одном месте

Нарушения правил:

- Rule 5: локальные классы/структуры внутри `.cpp` ещё остались
- Rule 7: слишком много свободных функций помимо `main`
- Rule 15: runtime roles всё ещё не до конца разрезаны на named components

Следующий шаг:

- выделить text/prompt processing owner
- выделить remaining HTTP request handling owner
- сузить `InferApp` до orchestration поверх named runtime components

### 3. `hostd/src/app/hostd_app.cpp`

Файл: `/Users/vladislavhalasiuk/Projects/Repos/comet-node/hostd/src/app/hostd_app.cpp`

Текущее состояние:

- файл остаётся самым тяжёлым hotspot (`2696` строк), но уже не держит transport/telemetry clusters
- bootstrap model acquisition/checksum/materialization cluster уже вынесен в `hostd/src/app/hostd_bootstrap_model_support.cpp`
- controller transport вынесен в `hostd/src/app/hostd_controller_transport_support.cpp`
- telemetry/runtime-inspection cluster вынесен в `hostd/src/app/hostd_telemetry_support.cpp`
- в giant file всё ещё смешаны disk lifecycle, assignment execution orchestration и часть CLI execution flow

Нарушения правил:

- Rule 5: локальные классы/структуры внутри `.cpp` запрещены
- Rule 6: stable helper classes всё ещё не вынесены
- Rule 7: большая часть поведения существует как free-function islands
- Rule 13: transport/domain/host execution logic всё ещё частично перемешаны
- Rule 15: runtime roles не разделены на отдельные owners

Следующий шаг:

- вынести disk runtime owners
- разрезать assignment execution flow на app/service slices
- сузить giant file до orchestration boundary и disk-specific owners

## Secondary Remaining Violations

### 4. Callback-bag APIs в `controller`

Файлы-примеры:

- `/Users/vladislavhalasiuk/Projects/Repos/comet-node/controller/include/plane/plane_http_service.h`
- `/Users/vladislavhalasiuk/Projects/Repos/comet-node/controller/include/read_model/read_model_http_service.h`
- `/Users/vladislavhalasiuk/Projects/Repos/comet-node/controller/include/host/hostd_http_service.h`

Текущее состояние:

- `interaction_http_service.h` и `auth_http_service.h` уже переведены на named support owners
- `model_library_service.h` и `model_library_http_service.h` тоже уже переведены на named support owners
- `plane_http_service.h`, `read_model_http_service.h` и `hostd_http_service.h` тоже уже переведены на named support owners
- callback-bag pattern больше не является главным hotspot для перечисленных controller HTTP surfaces

Нарушения правил:

- Rule 10: большие `Deps`-мешки и callback-based public APIs должны уходить в named dependencies и интерфейсы

Следующий шаг:

- дочищать оставшиеся function-style helper seams и callback-like adapters там, где роли уже стабилизировались

### 5. Callback seams в shared/runtime helpers

Файлы-примеры:

- `/Users/vladislavhalasiuk/Projects/Repos/comet-node/hostd/include/app/hostd_local_state_support.h`
- `/Users/vladislavhalasiuk/Projects/Repos/comet-node/runtime/infer/include/runtime/llama_library_engine.h`
- `/Users/vladislavhalasiuk/Projects/Repos/comet-node/common/include/comet/state/sqlite_store_schema.h`

Текущее состояние:

- после выносов часть support layers всё ещё использует function-style seams

Нарушения правил:

- Rule 10: это лучше текущего giant file, но ещё не финальная форма

Следующий шаг:

- заменять callback seams на named collaborators там, где роли уже стабилизировались

## Recommended Priority Order

1. `infer_app.cpp`
2. `hostd_app.cpp`
3. cleanup function-style seams в `controller` и `hostd`
