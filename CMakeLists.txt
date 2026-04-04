cmake_minimum_required(VERSION 3.22)

option(COMET_SKIP_VCPKG_TOOLCHAIN "Skip automatic vcpkg toolchain detection" OFF)

if(DEFINED VCPKG_INSTALLED_DIR AND DEFINED VCPKG_TARGET_TRIPLET)
  list(PREPEND CMAKE_PREFIX_PATH "${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}")
  if(NOT DEFINED _VCPKG_INSTALLED_DIR)
    set(_VCPKG_INSTALLED_DIR "${VCPKG_INSTALLED_DIR}")
  endif()
endif()

if(NOT DEFINED CMAKE_DISABLE_FIND_PACKAGE_ICU)
  set(CMAKE_DISABLE_FIND_PACKAGE_ICU TRUE)
endif()

if(NOT COMET_SKIP_VCPKG_TOOLCHAIN AND NOT DEFINED CMAKE_TOOLCHAIN_FILE)
  set(_comet_vcpkg_candidates
      "$ENV{VCPKG_ROOT}"
      "${CMAKE_CURRENT_SOURCE_DIR}/.tools/vcpkg"
      "/mnt/e/dev/tools/vcpkg"
      "$ENV{HOME}/vcpkg")

  find_program(_comet_vcpkg_executable NAMES vcpkg)
  if(_comet_vcpkg_executable)
    get_filename_component(_comet_vcpkg_bin_dir "${_comet_vcpkg_executable}" DIRECTORY)
    get_filename_component(_comet_vcpkg_root_from_path "${_comet_vcpkg_bin_dir}" DIRECTORY)
    list(APPEND _comet_vcpkg_candidates "${_comet_vcpkg_root_from_path}")
  endif()

  foreach(_comet_vcpkg_root IN LISTS _comet_vcpkg_candidates)
    if(_comet_vcpkg_root AND EXISTS "${_comet_vcpkg_root}/scripts/buildsystems/vcpkg.cmake")
      set(
        CMAKE_TOOLCHAIN_FILE
        "${_comet_vcpkg_root}/scripts/buildsystems/vcpkg.cmake"
        CACHE STRING "Vcpkg toolchain file"
      )
      message(STATUS "Using vcpkg toolchain: ${CMAKE_TOOLCHAIN_FILE}")
      break()
    endif()
  endforeach()

  unset(_comet_vcpkg_candidates)
  unset(_comet_vcpkg_executable CACHE)
  unset(_comet_vcpkg_bin_dir)
  unset(_comet_vcpkg_root)
  unset(_comet_vcpkg_root_from_path)
endif()

project(
  comet-node
  VERSION 0.1.0
  LANGUAGES CXX
)

find_package(Threads REQUIRED)

function(comet_enable_strict_warnings target_name)
  if(MSVC)
    target_compile_options(${target_name} PRIVATE /W4)
  else()
    target_compile_options(${target_name} PRIVATE -Wall -Wextra -Wpedantic)
  endif()
endfunction()

include(FetchContent)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")

set(COMET_LLAMA_CPP_GIT_TAG "7c7d6ce5c7caad3714ad1cd37ef057dfe0f317d0" CACHE STRING
    "Pinned ggml-org/llama.cpp revision used for comet runtime binaries")
set(COMET_LLAMA_CPP_SOURCE_DIR "" CACHE PATH
    "Optional local llama.cpp source directory override for comet runtime binaries")

if(DEFINED BUILD_SHARED_LIBS)
  set(_comet_saved_build_shared_libs "${BUILD_SHARED_LIBS}")
  set(_comet_restore_build_shared_libs ON)
else()
  set(_comet_restore_build_shared_libs OFF)
endif()

set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build static llama.cpp for comet runtime binaries" FORCE)
set(LLAMA_BUILD_COMMON ON CACHE BOOL "" FORCE)
set(LLAMA_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_TOOLS ON CACHE BOOL "" FORCE)
set(LLAMA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_SERVER ON CACHE BOOL "" FORCE)
set(LLAMA_OPENSSL OFF CACHE BOOL "" FORCE)
set(LLAMA_USE_SYSTEM_GGML OFF CACHE BOOL "" FORCE)
set(GGML_NATIVE OFF CACHE BOOL "" FORCE)
set(GGML_RPC ON CACHE BOOL "" FORCE)
set(_comet_cuda_available OFF)
if(DEFINED CUDAToolkit_ROOT AND EXISTS "${CUDAToolkit_ROOT}/include/cuda_runtime.h")
  set(_comet_cuda_available ON)
elseif(DEFINED CMAKE_CUDA_COMPILER)
  get_filename_component(_comet_cuda_bin_dir "${CMAKE_CUDA_COMPILER}" DIRECTORY)
  get_filename_component(_comet_cuda_root_from_compiler "${_comet_cuda_bin_dir}" DIRECTORY)
  if(EXISTS "${_comet_cuda_root_from_compiler}/include/cuda_runtime.h")
    set(_comet_cuda_available ON)
  endif()
endif()
if(NOT _comet_cuda_available)
  message(FATAL_ERROR
    "CUDA toolkit is required for comet-node builds. "
    "Provide CUDAToolkit_ROOT or CMAKE_CUDA_COMPILER so GGML_CUDA can be enabled.")
endif()
set(GGML_CUDA ON CACHE BOOL "" FORCE)

if(COMET_LLAMA_CPP_SOURCE_DIR AND EXISTS "${COMET_LLAMA_CPP_SOURCE_DIR}/CMakeLists.txt")
  FetchContent_Declare(
    llama_cpp
    SOURCE_DIR "${COMET_LLAMA_CPP_SOURCE_DIR}"
  )
else()
  FetchContent_Declare(
    llama_cpp
    GIT_REPOSITORY https://github.com/ggml-org/llama.cpp.git
    GIT_TAG ${COMET_LLAMA_CPP_GIT_TAG}
    GIT_SHALLOW TRUE
    GIT_PROGRESS TRUE
  )
endif()
FetchContent_MakeAvailable(llama_cpp)

if(_comet_restore_build_shared_libs)
  set(BUILD_SHARED_LIBS "${_comet_saved_build_shared_libs}" CACHE BOOL "Build shared libs" FORCE)
else()
  unset(BUILD_SHARED_LIBS CACHE)
endif()

find_package(unofficial-sqlite3 CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(unofficial-sodium CONFIG REQUIRED)

add_library(
  comet-common
  common/src/planning/compose_renderer.cpp
  common/src/security/crypto_utils.cpp
  common/src/state/demo_state.cpp
  common/src/planning/execution_plan.cpp
  common/src/importing/import_bundle.cpp
  common/src/runtime/infer_runtime_config.cpp
  common/src/runtime/model_adapter.cpp
  common/src/planning/planner.cpp
  common/src/planning/reconcile.cpp
  common/src/runtime/runtime_status.cpp
  common/src/state/auth_repository.cpp
  common/src/state/assignment_repository.cpp
  common/src/state/controller_settings_repository.cpp
  common/src/state/desired_state_repository.cpp
  common/src/state/desired_state_sqlite_codec.cpp
  common/src/state/desired_state_v2_projector.cpp
  common/src/state/desired_state_v2_renderer.cpp
  common/src/state/desired_state_v2_validator.cpp
  common/src/state/disk_runtime_repository.cpp
  common/src/state/event_repository.cpp
  common/src/state/node_availability_repository.cpp
  common/src/state/observation_repository.cpp
  common/src/state/plane_repository.cpp
  common/src/state/registered_host_repository.cpp
  common/src/state/scheduler_repository.cpp
  common/src/state/sqlite_statement.cpp
  common/src/state/sqlite_store_schema.cpp
  common/src/state/sqlite_store_status_codec.cpp
  common/src/state/sqlite_store_support.cpp
  common/src/state/skills_factory_repository.cpp
  common/src/planning/scheduling_policy.cpp
  common/src/state/state_json.cpp
  common/src/state/sqlite_store.cpp
)

target_include_directories(comet-common PUBLIC common/include)
target_link_libraries(
  comet-common
  PUBLIC
    nlohmann_json::nlohmann_json
    unofficial-sodium::sodium
    unofficial::sqlite3::sqlite3
)
comet_enable_strict_warnings(comet-common)

add_executable(
  comet-model-adapter-tests
  common/src/runtime/model_adapter_tests.cpp
)
target_include_directories(comet-model-adapter-tests PRIVATE common/include)
target_link_libraries(comet-model-adapter-tests PRIVATE comet-common)
comet_enable_strict_warnings(comet-model-adapter-tests)

add_executable(
  comet-state-v2-tests
  common/src/state/desired_state_v2_validator_tests.cpp
)
target_include_directories(comet-state-v2-tests PRIVATE common/include)
target_link_libraries(comet-state-v2-tests PRIVATE comet-common)
comet_enable_strict_warnings(comet-state-v2-tests)

add_executable(
  comet-state-v2-projector-tests
  common/src/state/desired_state_v2_projector_tests.cpp
)
target_include_directories(comet-state-v2-projector-tests PRIVATE common/include)
target_link_libraries(comet-state-v2-projector-tests PRIVATE comet-common)
comet_enable_strict_warnings(comet-state-v2-projector-tests)

add_executable(
  comet-compose-renderer-tests
  common/src/planning/compose_renderer_tests.cpp
)
target_include_directories(comet-compose-renderer-tests PRIVATE common/include)
target_link_libraries(comet-compose-renderer-tests PRIVATE comet-common)
comet_enable_strict_warnings(comet-compose-renderer-tests)

add_executable(
  comet-worker-group-topology-tests
  common/src/state/worker_group_topology_tests.cpp
)
target_include_directories(comet-worker-group-topology-tests PRIVATE common/include)
target_link_libraries(comet-worker-group-topology-tests PRIVATE comet-common)
comet_enable_strict_warnings(comet-worker-group-topology-tests)

add_executable(
  comet-plane-request-tests
  controller/src/plane/plane_desired_state_request_parser.cpp
  controller/src/plane/plane_desired_state_request_parser_tests.cpp
)
target_include_directories(comet-plane-request-tests PRIVATE common/include controller/include)
target_link_libraries(comet-plane-request-tests PRIVATE comet-common)
comet_enable_strict_warnings(comet-plane-request-tests)

add_executable(
  comet-model-library-tests
  controller/src/app/controller_composition_support.cpp
  controller/src/app/controller_request_context.cpp
  controller/src/app/controller_time_support.cpp
  controller/src/infra/controller_request_support.cpp
  controller/src/infra/controller_network_manager.cpp
  controller/src/model/model_conversion_service.cpp
  controller/src/model/model_library_service.cpp
  controller/src/model/model_library_service_tests.cpp
  controller/src/model/model_library_support.cpp
)
target_include_directories(comet-model-library-tests PRIVATE common/include controller/include)
target_link_libraries(comet-model-library-tests PRIVATE comet-common)
target_compile_definitions(
  comet-model-library-tests
  PRIVATE
    COMET_LLAMA_CPP_SOURCE_TREE="${llama_cpp_SOURCE_DIR}"
    COMET_LLAMA_CPP_RUNTIME_OUTPUT_DIR="${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
)
comet_enable_strict_warnings(comet-model-library-tests)

add_executable(
  comet-controller-apply-state-tests
  controller/src/bundle/bundle_cli_service.cpp
  controller/src/bundle/bundle_cli_service_tests.cpp
  controller/src/infra/controller_action.cpp
  controller/src/infra/controller_print_service.cpp
  controller/src/infra/controller_runtime_support_service.cpp
  controller/src/plane/desired_state_policy_service.cpp
  controller/src/plane/plane_realization_service.cpp
)
target_include_directories(
  comet-controller-apply-state-tests PRIVATE common/include controller/include)
target_link_libraries(comet-controller-apply-state-tests PRIVATE comet-common)
comet_enable_strict_warnings(comet-controller-apply-state-tests)

add_executable(
  comet-controller-plane-deletion-tests
  controller/src/host/host_assignment_reconciliation_service.cpp
  controller/src/plane/controller_state_service.cpp
  controller/src/plane/plane_deletion_support.cpp
  controller/src/plane/plane_service.cpp
  controller/src/plane/plane_deletion_support_tests.cpp
)
target_include_directories(
  comet-controller-plane-deletion-tests PRIVATE common/include controller/include)
target_link_libraries(comet-controller-plane-deletion-tests PRIVATE comet-common)
comet_enable_strict_warnings(comet-controller-plane-deletion-tests)

add_executable(
  comet-plane-skills-tests
  controller/src/infra/controller_network_manager.cpp
  controller/src/http/controller_http_transport.cpp
  controller/src/interaction/interaction_service.cpp
  controller/src/plane/plane_dashboard_skills_summary_service.cpp
  controller/src/skills/plane_skill_contextual_resolver_service.cpp
  controller/src/skills/plane_skill_runtime_sync_service.cpp
  controller/src/skills/plane_skills_service.cpp
  controller/src/skills/plane_skills_service_tests.cpp
)
target_include_directories(
  comet-plane-skills-tests PRIVATE common/include controller/include)
target_link_libraries(
  comet-plane-skills-tests PRIVATE comet-common Threads::Threads)
comet_enable_strict_warnings(comet-plane-skills-tests)

add_executable(
  comet-skills-factory-tests
  controller/src/infra/controller_action.cpp
  controller/src/http/controller_http_transport.cpp
  controller/src/infra/controller_network_manager.cpp
  controller/src/host/host_assignment_reconciliation_service.cpp
  controller/src/plane/plane_mutation_service.cpp
  controller/src/plane/controller_state_service.cpp
  controller/src/plane/plane_deletion_support.cpp
  controller/src/plane/plane_service.cpp
  controller/src/skills/plane_skill_contextual_resolver_service.cpp
  controller/src/skills/plane_skill_catalog_service.cpp
  controller/src/skills/plane_skill_runtime_sync_service.cpp
  controller/src/skills/plane_skills_service.cpp
  controller/src/skills/skills_factory_service.cpp
  controller/src/skills/skills_factory_service_tests.cpp
)
target_include_directories(
  comet-skills-factory-tests PRIVATE common/include controller/include)
target_link_libraries(comet-skills-factory-tests PRIVATE comet-common)
comet_enable_strict_warnings(comet-skills-factory-tests)

add_executable(
  comet-hostd-local-state-tests
  hostd/src/app/hostd_local_state_support.cpp
  hostd/src/app/hostd_local_state_support_tests.cpp
)
target_include_directories(comet-hostd-local-state-tests PRIVATE common/include hostd/include)
target_link_libraries(comet-hostd-local-state-tests PRIVATE comet-common)
comet_enable_strict_warnings(comet-hostd-local-state-tests)

add_executable(
  comet-hostd-assignment-lock-tests
  hostd/src/state_apply/hostd_assignment_lock.cpp
  hostd/src/state_apply/hostd_assignment_lock_tests.cpp
)
target_include_directories(comet-hostd-assignment-lock-tests PRIVATE common/include hostd/include)
target_link_libraries(comet-hostd-assignment-lock-tests PRIVATE comet-common)
comet_enable_strict_warnings(comet-hostd-assignment-lock-tests)

add_executable(
  comet-controller
  controller/src/auth/auth_http_service.cpp
  controller/src/auth/auth_http_support.cpp
  controller/src/auth/auth_payload_service.cpp
  controller/src/auth/auth_support_service.cpp
  controller/src/host/host_assignment_reconciliation_service.cpp
  controller/src/scheduler/assignment_orchestration_service.cpp
  controller/src/bundle/bundle_cli_service.cpp
  controller/src/bundle/bundle_http_service.cpp
  controller/src/app/controller_cli.cpp
  controller/src/infra/controller_action.cpp
  controller/src/app/controller_command_line.cpp
  controller/src/app/controller_component_factory.cpp
  controller/src/app/controller_component_factory_support.cpp
  controller/src/app/controller_composition_support.cpp
  controller/src/app/controller_http_service_support.cpp
  controller/src/app/controller_plane_support.cpp
  controller/src/app/controller_read_model_support.cpp
  controller/src/app/controller_serve_support.cpp
  controller/src/app/controller_language_support.cpp
  controller/src/app/controller_request_context.cpp
  controller/src/app/controller_scheduler_service_factory.cpp
  controller/src/app/controller_time_support.cpp
  controller/src/infra/controller_event_service.cpp
  controller/src/infra/controller_health_service.cpp
  controller/src/infra/controller_print_service.cpp
  controller/src/infra/controller_request_support.cpp
  controller/src/infra/controller_runtime_support_service.cpp
  controller/src/plane/controller_state_service.cpp
  controller/src/plane/plane_deletion_support.cpp
  controller/src/http/controller_http_server.cpp
  controller/src/http/controller_http_route_handlers.cpp
  controller/src/http/controller_http_router.cpp
  controller/src/infra/controller_network_manager.cpp
  controller/src/http/controller_http_server_support.cpp
  controller/src/http/controller_http_transport.cpp
  controller/src/infra/controller_remote_client.cpp
  controller/src/plane/desired_state_policy_service.cpp
  controller/src/plane/dashboard_service.cpp
  controller/src/plane/plane_desired_state_request_parser.cpp
  controller/src/infra/controller_ui_service.cpp
  controller/src/host/host_registry_service.cpp
  controller/src/host/hostd_http_service.cpp
  controller/src/host/hostd_http_support.cpp
  controller/src/interaction/interaction_http_service.cpp
  controller/src/interaction/interaction_http_support.cpp
  controller/src/interaction/interaction_payload_builder.cpp
  controller/src/interaction/interaction_runtime_support_service.cpp
  controller/src/interaction/interaction_service.cpp
  controller/src/model/model_library_http_service.cpp
  controller/src/model/model_library_http_support.cpp
  controller/src/model/model_conversion_service.cpp
  controller/src/model/model_library_service.cpp
  controller/src/model/model_library_support.cpp
  controller/src/plane/plane_http_service.cpp
  controller/src/plane/plane_http_support.cpp
  controller/src/plane/plane_dashboard_skills_summary_service.cpp
  controller/src/plane/plane_mutation_service.cpp
  controller/src/plane/plane_realization_service.cpp
  controller/src/plane/plane_registry_service.cpp
  controller/src/read_model/read_model_cli_service.cpp
  controller/src/read_model/read_model_http_service.cpp
  controller/src/read_model/read_model_http_support.cpp
  controller/src/read_model/read_model_service.cpp
  controller/src/app/remote_controller_cli_service.cpp
  controller/src/scheduler/scheduler_cli_service.cpp
  controller/src/scheduler/scheduler_domain_service.cpp
  controller/src/scheduler/scheduler_execution_support.cpp
  controller/src/scheduler/scheduler_http_service.cpp
  controller/src/scheduler/scheduler_view_service.cpp
  controller/src/plane/plane_service.cpp
  controller/src/skills/plane_skill_contextual_resolver_service.cpp
  controller/src/skills/plane_skill_catalog_service.cpp
  controller/src/skills/plane_skill_runtime_sync_service.cpp
  controller/src/skills/plane_skills_service.cpp
  controller/src/skills/skills_factory_http_service.cpp
  controller/src/skills/skills_factory_service.cpp
  controller/src/scheduler/scheduler_service.cpp
  controller/src/read_model/state_aggregate_loader.cpp
  controller/src/web/web_ui_service.cpp
  controller/src/app/controller_app.cpp
  controller/src/app/controller_composition_root.cpp
  controller/src/main.cpp
)
target_include_directories(comet-controller PRIVATE controller/include)
target_link_libraries(comet-controller PRIVATE comet-common)
target_compile_definitions(
  comet-controller
  PRIVATE
    COMET_LLAMA_CPP_SOURCE_TREE="${llama_cpp_SOURCE_DIR}"
    COMET_LLAMA_CPP_RUNTIME_OUTPUT_DIR="${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
)
if(TARGET llama-quantize)
  add_dependencies(comet-controller llama-quantize)
endif()
if(WIN32)
  target_link_libraries(comet-controller PRIVATE ws2_32)
endif()
comet_enable_strict_warnings(comet-controller)

add_executable(
  comet-hostd
  hostd/src/app/hostd_composition_root.cpp
  hostd/src/app/hostd_cli_actions.cpp
  hostd/src/app/hostd_bootstrap_model_support.cpp
  hostd/src/app/hostd_controller_transport_support.cpp
  hostd/src/app/hostd_local_state_support.cpp
  hostd/src/app/hostd_telemetry_support.cpp
  hostd/src/backend/default_http_hostd_backend_support.cpp
  hostd/src/backend/hostd_backend_factory.cpp
  hostd/src/backend/http_hostd_backend.cpp
  hostd/src/backend/local_db_hostd_backend.cpp
  hostd/src/observation/default_hostd_observation_support.cpp
  hostd/src/observation/hostd_observation_service.cpp
  hostd/src/state_apply/default_hostd_assignment_support.cpp
  hostd/src/state_apply/hostd_assignment_lock.cpp
  hostd/src/state_apply/hostd_assignment_service.cpp
  hostd/src/cli/hostd_cli.cpp
  hostd/src/cli/hostd_command_line.cpp
  hostd/src/config/node_config_loader.cpp
  hostd/src/main.cpp
  hostd/src/app/hostd_app.cpp
)
target_include_directories(comet-hostd PRIVATE hostd/include)
target_link_libraries(comet-hostd PRIVATE comet-common)
if(UNIX AND NOT APPLE)
  target_link_libraries(comet-hostd PRIVATE dl)
endif()
if(WIN32)
  target_link_libraries(comet-hostd PRIVATE ws2_32)
endif()
comet_enable_strict_warnings(comet-hostd)

add_executable(
  comet-node
  launcher/src/app/hostd_registration_service.cpp
  launcher/src/app/launcher_composition_root.cpp
  launcher/src/app/launcher_doctor_service.cpp
  launcher/src/cli/launcher_command_line.cpp
  launcher/src/config/generated_config_loader.cpp
  launcher/src/config/install_layout.cpp
  launcher/src/install/launcher_install_service.cpp
  launcher/src/platform/launcher_path_resolver.cpp
  launcher/src/platform/process_runner.cpp
  launcher/src/platform/signal_manager.cpp
  launcher/src/run/launcher_run_service.cpp
  launcher/src/main.cpp
  launcher/src/app/launcher_app.cpp
)
target_include_directories(comet-node PRIVATE launcher/include)
target_link_libraries(comet-node PRIVATE comet-common)
comet_enable_strict_warnings(comet-node)

if(NOT WIN32)
  add_executable(
    comet-devtool
    tools/devtool/src/http_client.cpp
    tools/devtool/src/main.cpp
  )
  target_include_directories(comet-devtool PRIVATE tools/devtool/include)
  target_link_libraries(
    comet-devtool
    PRIVATE
      nlohmann_json::nlohmann_json
      Threads::Threads
  )
  comet_enable_strict_warnings(comet-devtool)

  add_executable(
    comet-skillsd
    controller/src/http/controller_http_server_support.cpp
    controller/src/infra/controller_network_manager.cpp
    runtime/skills/src/skills_server.cpp
    runtime/skills/src/skills_store.cpp
    runtime/skills/src/main.cpp
    common/src/runtime/runtime_status.cpp
    common/src/state/sqlite_statement.cpp
  )
  target_include_directories(
    comet-skillsd PRIVATE common/include controller/include runtime/skills/include)
  target_link_libraries(
    comet-skillsd
    PRIVATE
      nlohmann_json::nlohmann_json
      unofficial::sqlite3::sqlite3
  )
  comet_enable_strict_warnings(comet-skillsd)

  add_executable(
    comet-inferctl
    runtime/infer/src/app/infer_command_line.cpp
    runtime/infer/src/app/infer_cli_output_support.cpp
    runtime/infer/src/app/infer_model_cache_support.cpp
    runtime/infer/src/app/infer_status_support.cpp
    runtime/infer/src/http/local_http_server.cpp
    runtime/infer/src/platform/infer_signal_service.cpp
    runtime/infer/src/runtime/infer_control_support.cpp
    runtime/infer/src/runtime/infer_prewarm_support.cpp
    runtime/infer/src/runtime/infer_replica_support.cpp
    runtime/infer/src/runtime/llama_library_engine.cpp
    runtime/infer/src/runtime/llama_rpc_runtime.cpp
    runtime/infer/src/runtime/local_runtime.cpp
    runtime/infer/src/infer_app.cpp
    runtime/infer/src/main.cpp
    common/src/runtime/runtime_status.cpp
  )
  target_include_directories(comet-inferctl PRIVATE common/include runtime/infer/include)
  target_include_directories(
    comet-inferctl
    BEFORE PRIVATE
      "${llama_cpp_SOURCE_DIR}/include"
      "${llama_cpp_SOURCE_DIR}/ggml/include"
  )
  target_link_libraries(
    comet-inferctl
    PRIVATE
      comet-common
      llama
  )
  comet_enable_strict_warnings(comet-inferctl)

  add_executable(
    comet-workerd
    runtime/worker/src/worker_app.cpp
    runtime/worker/src/worker_config_loader.cpp
    runtime/worker/src/worker_engine_host.cpp
    runtime/worker/src/worker_model_resolver.cpp
    runtime/worker/src/worker_signal_service.cpp
    runtime/worker/src/worker_status_service.cpp
    # runtime/worker/src/llama_library_engine.cpp
    runtime/worker/src/main.cpp
    common/src/runtime/runtime_status.cpp
  )
  target_include_directories(comet-workerd PRIVATE common/include runtime/worker/include)
  # target_include_directories(
  #   comet-workerd
  #   BEFORE PRIVATE
  #     "${llama_cpp_SOURCE_DIR}/include"
  #     "${llama_cpp_SOURCE_DIR}/ggml/include"
  # )
  target_link_libraries(
    comet-workerd
    PRIVATE
      nlohmann_json::nlohmann_json
      # llama
  )
  comet_enable_strict_warnings(comet-workerd)
endif()
