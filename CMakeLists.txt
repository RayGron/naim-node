cmake_minimum_required(VERSION 3.22)

option(NAIM_SKIP_VCPKG_TOOLCHAIN "Skip automatic vcpkg toolchain detection" OFF)
set(NAIM_CUDA_NVCC_JOBS "2" CACHE STRING
    "Maximum number of concurrent nvcc compile jobs for ggml-cuda when using Ninja")
option(NAIM_CONTROLLER_BUILD_LLAMA_QUANTIZE
       "Build llama-quantize before naim-controller for local model conversion tooling"
       OFF)
option(NAIM_ENABLE_LLAMA_RUNTIME
       "Configure llama.cpp, CUDA, and inference runtime targets"
       ON)
option(NAIM_ENABLE_CEF_BROWSING "Enable CEF-backed browsing runtime support" OFF)
set(NAIM_CEF_ROOT "" CACHE PATH "Path to an unpacked CEF binary distribution")
set(NAIM_CEF_DOWNLOAD_URL "" CACHE STRING "Optional URL to a CEF binary distribution archive")

if(DEFINED VCPKG_INSTALLED_DIR AND DEFINED VCPKG_TARGET_TRIPLET)
  list(PREPEND CMAKE_PREFIX_PATH "${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}")
  if(NOT DEFINED _VCPKG_INSTALLED_DIR)
    set(_VCPKG_INSTALLED_DIR "${VCPKG_INSTALLED_DIR}")
  endif()
endif()

if(NOT DEFINED CMAKE_DISABLE_FIND_PACKAGE_ICU)
  set(CMAKE_DISABLE_FIND_PACKAGE_ICU TRUE)
endif()

if(NOT NAIM_SKIP_VCPKG_TOOLCHAIN AND NOT DEFINED CMAKE_TOOLCHAIN_FILE)
  set(_naim_vcpkg_candidates
      "$ENV{VCPKG_ROOT}"
      "${CMAKE_CURRENT_SOURCE_DIR}/.tools/vcpkg"
      "/mnt/e/dev/tools/vcpkg"
      "$ENV{HOME}/vcpkg")

  find_program(_naim_vcpkg_executable NAMES vcpkg)
  if(_naim_vcpkg_executable)
    get_filename_component(_naim_vcpkg_bin_dir "${_naim_vcpkg_executable}" DIRECTORY)
    get_filename_component(_naim_vcpkg_root_from_path "${_naim_vcpkg_bin_dir}" DIRECTORY)
    list(APPEND _naim_vcpkg_candidates "${_naim_vcpkg_root_from_path}")
  endif()

  foreach(_naim_vcpkg_root IN LISTS _naim_vcpkg_candidates)
    if(_naim_vcpkg_root AND EXISTS "${_naim_vcpkg_root}/scripts/buildsystems/vcpkg.cmake")
      set(
        CMAKE_TOOLCHAIN_FILE
        "${_naim_vcpkg_root}/scripts/buildsystems/vcpkg.cmake"
        CACHE STRING "Vcpkg toolchain file"
      )
      message(STATUS "Using vcpkg toolchain: ${CMAKE_TOOLCHAIN_FILE}")
      break()
    endif()
  endforeach()

  unset(_naim_vcpkg_candidates)
  unset(_naim_vcpkg_executable CACHE)
  unset(_naim_vcpkg_bin_dir)
  unset(_naim_vcpkg_root)
  unset(_naim_vcpkg_root_from_path)
endif()

project(
  naim-node
  VERSION 0.1.0
  LANGUAGES CXX
)

find_package(Threads REQUIRED)

function(naim_enable_strict_warnings target_name)
  if(MSVC)
    target_compile_options(${target_name} PRIVATE /W4)
  else()
    target_compile_options(${target_name} PRIVATE -Wall -Wextra -Wpedantic)
  endif()
endfunction()

include(cmake/NaimCEF.cmake)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")

if(NAIM_ENABLE_LLAMA_RUNTIME)
  include(FetchContent)

  set(NAIM_LLAMA_CPP_GIT_TAG "7c7d6ce5c7caad3714ad1cd37ef057dfe0f317d0" CACHE STRING
      "Pinned ggml-org/llama.cpp revision used for naim runtime binaries")
  set(NAIM_LLAMA_CPP_SOURCE_DIR "" CACHE PATH
      "Optional local llama.cpp source directory override for naim runtime binaries")

  if(DEFINED BUILD_SHARED_LIBS)
    set(_naim_saved_build_shared_libs "${BUILD_SHARED_LIBS}")
    set(_naim_restore_build_shared_libs ON)
  else()
    set(_naim_restore_build_shared_libs OFF)
  endif()

  set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build static llama.cpp for naim runtime binaries" FORCE)
  set(LLAMA_BUILD_COMMON ON CACHE BOOL "" FORCE)
  set(LLAMA_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(LLAMA_BUILD_TOOLS ON CACHE BOOL "" FORCE)
  set(LLAMA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
  set(LLAMA_BUILD_SERVER ON CACHE BOOL "" FORCE)
  set(LLAMA_OPENSSL OFF CACHE BOOL "" FORCE)
  set(LLAMA_USE_SYSTEM_GGML OFF CACHE BOOL "" FORCE)
  # llama.cpp needs Blackwell FP4/MXFP4 kernels to be compiled for the
  # architecture-specific 12Xa targets. CMake's native CUDA detection reports
  # RTX PRO 6000 Blackwell as plain sm_120, which makes ptxas reject those
  # kernels. Keep native detection opt-in and let llama.cpp choose its supported
  # explicit CUDA architecture list by default. For host-specific images, set
  # NAIM_CUDA_ARCHITECTURES explicitly, for example 120a-real on hpc1.
  set(NAIM_CUDA_ARCHITECTURES "" CACHE STRING
      "Explicit CUDA architectures for ggml-cuda builds; empty lets llama.cpp choose defaults")
  option(NAIM_CUDA_NATIVE "Prefer native CUDA architectures for ggml-cuda builds" OFF)
  if(NAIM_CUDA_ARCHITECTURES)
    set(NAIM_CUDA_NATIVE OFF CACHE BOOL "" FORCE)
    set(CMAKE_CUDA_ARCHITECTURES
        "${NAIM_CUDA_ARCHITECTURES}"
        CACHE STRING "Explicit CUDA architectures for naim ggml-cuda builds" FORCE)
  endif()
  set(GGML_NATIVE ${NAIM_CUDA_NATIVE} CACHE BOOL "" FORCE)
  set(GGML_RPC ON CACHE BOOL "" FORCE)

  if(NOT DEFINED CUDAToolkit_ROOT OR "${CUDAToolkit_ROOT}" STREQUAL "")
    set(_naim_cuda_root_candidates
        "$ENV{CUDA_TOOLKIT_ROOT_DIR}"
        "$ENV{CUDA_HOME}"
        "$ENV{CUDA_PATH}"
        "/usr/local/cuda-13.1"
        "/usr/local/cuda"
        "/usr/local/cuda-13.2"
        "/usr/local/cuda-13.0"
        "/usr/local/cuda-12.9"
        "/usr/local/cuda-12.8"
        "/usr/local/cuda-12.7"
        "/usr/local/cuda-12.6"
        "/usr/local/cuda-12.5"
        "/usr/local/cuda-12.4"
        "/usr/local/cuda-12.3"
        "/usr/local/cuda-12.2"
        "/usr/local/cuda-12.1"
        "/usr/local/cuda-12.0"
        "/usr/lib/nvidia-cuda-toolkit")

    find_program(_naim_nvcc_executable NAMES nvcc)
    if(_naim_nvcc_executable)
      get_filename_component(_naim_nvcc_bin_dir "${_naim_nvcc_executable}" DIRECTORY)
      get_filename_component(_naim_nvcc_root_from_path "${_naim_nvcc_bin_dir}" DIRECTORY)
      list(PREPEND _naim_cuda_root_candidates "${_naim_nvcc_root_from_path}")
    endif()

    foreach(_naim_cuda_root IN LISTS _naim_cuda_root_candidates)
      if(_naim_cuda_root AND EXISTS "${_naim_cuda_root}/include/cuda_runtime.h")
        set(CUDAToolkit_ROOT "${_naim_cuda_root}" CACHE PATH "Detected CUDA toolkit root")
        if(NOT DEFINED CMAKE_CUDA_COMPILER AND EXISTS "${_naim_cuda_root}/bin/nvcc")
          set(CMAKE_CUDA_COMPILER
              "${_naim_cuda_root}/bin/nvcc"
              CACHE FILEPATH "Detected CUDA compiler")
        endif()
        message(STATUS "Using detected CUDA toolkit: ${CUDAToolkit_ROOT}")
        break()
      endif()
    endforeach()

    unset(_naim_cuda_root_candidates)
    unset(_naim_nvcc_executable CACHE)
    unset(_naim_nvcc_bin_dir)
    unset(_naim_nvcc_root_from_path)
    unset(_naim_cuda_root)
  endif()

  set(_naim_cuda_available OFF)
  if(DEFINED CUDAToolkit_ROOT AND EXISTS "${CUDAToolkit_ROOT}/include/cuda_runtime.h")
    set(_naim_cuda_available ON)
  elseif(DEFINED CMAKE_CUDA_COMPILER)
    get_filename_component(_naim_cuda_bin_dir "${CMAKE_CUDA_COMPILER}" DIRECTORY)
    get_filename_component(_naim_cuda_root_from_compiler "${_naim_cuda_bin_dir}" DIRECTORY)
    if(EXISTS "${_naim_cuda_root_from_compiler}/include/cuda_runtime.h")
      set(_naim_cuda_available ON)
    endif()
  endif()
  if(NOT _naim_cuda_available)
    message(FATAL_ERROR
      "CUDA toolkit is required for naim-node inference runtime builds. "
      "Provide CUDAToolkit_ROOT or CMAKE_CUDA_COMPILER so GGML_CUDA can be enabled.")
  endif()
  set(GGML_CUDA ON CACHE BOOL "" FORCE)
  message(STATUS "Naim CUDA native architectures: ${NAIM_CUDA_NATIVE}")
  message(STATUS "Naim CUDA explicit architectures: ${NAIM_CUDA_ARCHITECTURES}")

  if(NAIM_LLAMA_CPP_SOURCE_DIR AND EXISTS "${NAIM_LLAMA_CPP_SOURCE_DIR}/CMakeLists.txt")
    FetchContent_Declare(
      llama_cpp
      SOURCE_DIR "${NAIM_LLAMA_CPP_SOURCE_DIR}"
    )
  else()
    FetchContent_Declare(
      llama_cpp
      GIT_REPOSITORY https://github.com/ggml-org/llama.cpp.git
      GIT_TAG ${NAIM_LLAMA_CPP_GIT_TAG}
      GIT_SHALLOW TRUE
      GIT_PROGRESS TRUE
    )
  endif()
  FetchContent_MakeAvailable(llama_cpp)

  if(TARGET ggml-cuda AND CMAKE_GENERATOR STREQUAL "Ninja")
    if(NAIM_CUDA_NVCC_JOBS MATCHES "^[1-9][0-9]*$")
      set_property(GLOBAL APPEND PROPERTY JOB_POOLS "naim_cuda_pool=${NAIM_CUDA_NVCC_JOBS}")
      set_property(TARGET ggml-cuda PROPERTY JOB_POOL_COMPILE naim_cuda_pool)
      message(STATUS "Using ggml-cuda compile pool size: ${NAIM_CUDA_NVCC_JOBS}")
    else()
      message(WARNING
        "NAIM_CUDA_NVCC_JOBS='${NAIM_CUDA_NVCC_JOBS}' is not a positive integer; "
        "ggml-cuda will use the default global compile pool")
    endif()
  endif()

  if(_naim_restore_build_shared_libs)
    set(BUILD_SHARED_LIBS "${_naim_saved_build_shared_libs}" CACHE BOOL "Build shared libs" FORCE)
  else()
    unset(BUILD_SHARED_LIBS CACHE)
  endif()
endif()

find_package(unofficial-sqlite3 CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(RocksDB CONFIG REQUIRED)
find_package(unofficial-sodium CONFIG REQUIRED)
find_package(zstd CONFIG REQUIRED)
naim_configure_cef_support()

add_library(
  naim-common
  common/src/planning/compose_renderer.cpp
  common/src/security/crypto_utils.cpp
  common/src/state/demo_state.cpp
  common/src/planning/execution_plan.cpp
  common/src/importing/import_bundle.cpp
  common/src/knowledge/knowledge_interfaces.cpp
  common/src/knowledge/knowledge_types.cpp
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
  common/src/state/desired_state_placement_resolver.cpp
  common/src/state/desired_state_v2_projector.cpp
  common/src/state/desired_state_v2_projector_support.cpp
  common/src/state/desired_state_v2_renderer.cpp
  common/src/state/desired_state_v2_validator.cpp
  common/src/state/disk_runtime_repository.cpp
  common/src/state/event_repository.cpp
  common/src/state/interaction_repository.cpp
  common/src/state/model_library_repository.cpp
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
  common/src/state/state_json_runtime_codecs.cpp
  common/src/state/state_json_settings_codecs.cpp
  common/src/state/state_json.cpp
  common/src/state/sqlite_store.cpp
)

target_include_directories(naim-common PUBLIC common/include)
target_link_libraries(
  naim-common
  PUBLIC
    nlohmann_json::nlohmann_json
    unofficial-sodium::sodium
    unofficial::sqlite3::sqlite3
)
naim_enable_strict_warnings(naim-common)

add_executable(
  naim-knowledge-contract-tests
  common/src/knowledge/knowledge_contract_tests.cpp
)
target_include_directories(naim-knowledge-contract-tests PRIVATE common/include)
target_link_libraries(naim-knowledge-contract-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-knowledge-contract-tests)

add_executable(
  naim-knowledge-store-tests
  runtime/knowledge/src/knowledge_store_keys.cpp
  runtime/knowledge/src/knowledge_text_processor.cpp
  runtime/knowledge/src/rocksdb_json_repository.cpp
  runtime/knowledge/src/knowledge_store.cpp
  runtime/knowledge/src/knowledge_store_tests.cpp
)
target_include_directories(
  naim-knowledge-store-tests PRIVATE common/include controller/include runtime/knowledge/include)
target_link_libraries(
  naim-knowledge-store-tests
  PRIVATE
    naim-common
    RocksDB::rocksdb
)
naim_enable_strict_warnings(naim-knowledge-store-tests)

add_executable(
  naim-model-adapter-tests
  common/src/runtime/model_adapter_tests.cpp
)
target_include_directories(naim-model-adapter-tests PRIVATE common/include)
target_link_libraries(naim-model-adapter-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-model-adapter-tests)

add_executable(
  naim-state-v2-tests
  common/src/state/desired_state_v2_validator_tests.cpp
)
target_include_directories(naim-state-v2-tests PRIVATE common/include)
target_link_libraries(naim-state-v2-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-state-v2-tests)

add_executable(
  naim-state-v2-projector-tests
  common/src/state/desired_state_v2_projector_tests.cpp
)
target_include_directories(naim-state-v2-projector-tests PRIVATE common/include)
target_link_libraries(naim-state-v2-projector-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-state-v2-projector-tests)

add_executable(
  naim-compose-renderer-tests
  common/src/planning/compose_renderer_tests.cpp
)
target_include_directories(naim-compose-renderer-tests PRIVATE common/include)
target_link_libraries(naim-compose-renderer-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-compose-renderer-tests)

add_executable(
  naim-worker-group-topology-tests
  common/src/state/worker_group_topology_tests.cpp
)
target_include_directories(naim-worker-group-topology-tests PRIVATE common/include)
target_link_libraries(naim-worker-group-topology-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-worker-group-topology-tests)

add_executable(
  naim-plane-request-tests
  controller/src/plane/plane_desired_state_request_parser.cpp
  controller/src/plane/plane_desired_state_request_parser_tests.cpp
)
target_include_directories(naim-plane-request-tests PRIVATE common/include controller/include)
target_link_libraries(naim-plane-request-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-plane-request-tests)

add_executable(
  naim-model-library-tests
  controller/src/app/controller_composition_support.cpp
  controller/src/app/controller_request_context.cpp
  controller/src/app/controller_time_support.cpp
  controller/src/infra/controller_request_support.cpp
  controller/src/infra/controller_network_manager.cpp
  controller/src/model/model_conversion_service.cpp
  controller/src/model/model_library_node_placement.cpp
  controller/src/model/model_library_service.cpp
  controller/src/model/model_library_service_tests.cpp
  controller/src/model/model_library_support.cpp
  controller/src/observation/plane_observation_matcher.cpp
)
target_include_directories(naim-model-library-tests PRIVATE common/include controller/include)
target_link_libraries(naim-model-library-tests PRIVATE naim-common)
target_compile_definitions(
  naim-model-library-tests
  PRIVATE
    NAIM_LLAMA_CPP_SOURCE_TREE="${llama_cpp_SOURCE_DIR}"
    NAIM_LLAMA_CPP_RUNTIME_OUTPUT_DIR="${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
)
naim_enable_strict_warnings(naim-model-library-tests)

add_executable(
  naim-controller-apply-state-tests
  controller/src/bundle/bundle_cli_service.cpp
  controller/src/bundle/bundle_cli_service_tests.cpp
  controller/src/infra/controller_action.cpp
  controller/src/infra/controller_print_service.cpp
  controller/src/infra/controller_runtime_support_service.cpp
  controller/src/plane/desired_state_policy_service.cpp
  controller/src/plane/plane_realization_service.cpp
)
target_include_directories(
  naim-controller-apply-state-tests PRIVATE common/include controller/include)
target_link_libraries(naim-controller-apply-state-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-controller-apply-state-tests)

add_executable(
  naim-controller-plane-deletion-tests
  controller/src/host/host_assignment_reconciliation_service.cpp
  controller/src/plane/controller_state_service.cpp
  controller/src/plane/plane_deletion_support.cpp
  controller/src/plane/plane_service.cpp
  controller/src/plane/plane_deletion_support_tests.cpp
)
target_include_directories(
  naim-controller-plane-deletion-tests PRIVATE common/include controller/include)
target_link_libraries(naim-controller-plane-deletion-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-controller-plane-deletion-tests)

add_executable(
  naim-plane-skills-tests
  controller/src/app/controller_language_support.cpp
  controller/src/browsing/interaction_browsing_service.cpp
  controller/src/browsing/plane_browsing_service.cpp
  controller/src/infra/controller_network_manager.cpp
  controller/src/http/controller_http_transport.cpp
  controller/src/interaction/interaction_completion_policy_support.cpp
  controller/src/interaction/interaction_contract_responder.cpp
  controller/src/interaction/interaction_model_identity_builder.cpp
  controller/src/interaction/interaction_payload_builder.cpp
  controller/src/interaction/interaction_request_contract_support.cpp
  controller/src/interaction/interaction_request_identity_support.cpp
  controller/src/interaction/interaction_request_heuristics.cpp
  controller/src/interaction/interaction_replica_group_summary_builder.cpp
  controller/src/interaction/interaction_runtime_text_support.cpp
  controller/src/interaction/interaction_text_post_processor.cpp
  controller/src/interaction/interaction_upstream_event_parser.cpp
  controller/src/interaction/interaction_utf8_payload_sanitizer.cpp
  controller/src/interaction/interaction_service.cpp
  controller/src/interaction/interaction_target_relay_policy.cpp
  controller/src/plane/plane_dashboard_skills_summary_service.cpp
  controller/src/skills/plane_skill_contextual_resolver_service.cpp
  controller/src/skills/plane_skill_runtime_sync_service.cpp
  controller/src/skills/plane_skills_target_resolver.cpp
  controller/src/skills/plane_skills_service.cpp
  controller/src/skills/plane_skills_service_tests.cpp
)
target_include_directories(
  naim-plane-skills-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-plane-skills-tests PRIVATE naim-common Threads::Threads)
naim_enable_strict_warnings(naim-plane-skills-tests)

add_executable(
  naim-skills-factory-tests
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
  controller/src/skills/plane_skills_target_resolver.cpp
  controller/src/skills/plane_skills_service.cpp
  controller/src/skills/skills_factory_service.cpp
  controller/src/skills/skills_factory_service_tests.cpp
)
target_include_directories(
  naim-skills-factory-tests PRIVATE common/include controller/include)
target_link_libraries(naim-skills-factory-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-skills-factory-tests)

add_executable(
  naim-dashboard-service-tests
  controller/src/app/controller_time_support.cpp
  controller/src/browsing/plane_browsing_service.cpp
  controller/src/http/controller_http_transport.cpp
  controller/src/infra/controller_network_manager.cpp
  controller/src/infra/controller_runtime_support_service.cpp
  controller/src/host/host_assignment_reconciliation_service.cpp
  controller/src/observation/plane_observation_matcher.cpp
  controller/src/plane/dashboard_service.cpp
  controller/src/plane/plane_placement_payload_builder.cpp
  controller/src/plane/dashboard_service_tests.cpp
  controller/src/plane/plane_dashboard_skills_summary_service.cpp
  controller/src/read_model/state_aggregate_loader.cpp
  controller/src/scheduler/scheduler_domain_service.cpp
  controller/src/scheduler/scheduler_view_service.cpp
  controller/src/web/web_ui_service.cpp
)
target_include_directories(
  naim-dashboard-service-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-dashboard-service-tests PRIVATE naim-common Threads::Threads)
naim_enable_strict_warnings(naim-dashboard-service-tests)

add_executable(
  naim-plane-registry-tests
  controller/src/infra/controller_runtime_support_service.cpp
  controller/src/host/host_assignment_reconciliation_service.cpp
  controller/src/observation/plane_observation_matcher.cpp
  controller/src/plane/plane_placement_payload_builder.cpp
  controller/src/plane/plane_deletion_support.cpp
  controller/src/plane/plane_registry_query_support.cpp
  controller/src/plane/plane_registry_service.cpp
  controller/src/plane/plane_registry_service_tests.cpp)
target_include_directories(
  naim-plane-registry-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-plane-registry-tests PRIVATE naim-common Threads::Threads)
naim_enable_strict_warnings(naim-plane-registry-tests)

add_executable(
  naim-plane-observation-matcher-tests
  controller/src/observation/plane_observation_matcher.cpp
  controller/src/observation/plane_observation_matcher_tests.cpp
)
target_include_directories(
  naim-plane-observation-matcher-tests PRIVATE common/include controller/include)
target_link_libraries(naim-plane-observation-matcher-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-plane-observation-matcher-tests)

add_executable(
  naim-read-model-tests
  controller/src/observation/plane_observation_matcher.cpp
  controller/src/infra/controller_runtime_support_service.cpp
  controller/src/read_model/read_model_service.cpp
  controller/src/read_model/read_model_service_tests.cpp
)
target_include_directories(
  naim-read-model-tests PRIVATE common/include controller/include)
target_link_libraries(naim-read-model-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-read-model-tests)

add_executable(
  naim-auth-http-tests
  controller/src/auth/auth_http_service.cpp
  controller/src/auth/auth_http_service_tests.cpp
  controller/src/auth/auth_http_support.cpp
  controller/src/auth/auth_payload_service.cpp
  controller/src/auth/auth_support_service.cpp
  controller/src/app/controller_composition_support.cpp
  controller/src/app/controller_request_context.cpp
  controller/src/app/controller_time_support.cpp
  controller/src/infra/controller_network_manager.cpp
  controller/src/observation/plane_observation_matcher.cpp
)
target_include_directories(
  naim-auth-http-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-auth-http-tests PRIVATE naim-common Threads::Threads)
naim_enable_strict_warnings(naim-auth-http-tests)

add_executable(
  naim-host-registry-tests
  controller/src/app/controller_time_support.cpp
  controller/src/host/host_registry_service.cpp
  controller/src/host/host_registry_service_tests.cpp
)
target_include_directories(
  naim-host-registry-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-host-registry-tests PRIVATE naim-common Threads::Threads)
naim_enable_strict_warnings(naim-host-registry-tests)

add_executable(
  naim-interaction-browsing-tests
  controller/src/browsing/interaction_browsing_service.cpp
  controller/src/browsing/plane_browsing_service.cpp
  controller/src/http/controller_http_transport.cpp
  controller/src/infra/controller_network_manager.cpp
  controller/src/browsing/interaction_browsing_service_tests.cpp
)
target_include_directories(
  naim-interaction-browsing-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-browsing-tests PRIVATE naim-common Threads::Threads)
naim_enable_strict_warnings(naim-interaction-browsing-tests)

add_executable(
  naim-interaction-request-heuristics-tests
  controller/src/interaction/interaction_request_heuristics.cpp
  controller/src/interaction/interaction_request_heuristics_tests.cpp
)
target_include_directories(
  naim-interaction-request-heuristics-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-request-heuristics-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-interaction-request-heuristics-tests)

add_executable(
  naim-interaction-completion-policy-support-tests
  controller/src/interaction/interaction_completion_policy_support.cpp
  controller/src/interaction/interaction_completion_policy_support_tests.cpp
  controller/src/interaction/interaction_request_heuristics.cpp
  controller/src/interaction/interaction_text_post_processor.cpp
)
target_include_directories(
  naim-interaction-completion-policy-support-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-completion-policy-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-interaction-completion-policy-support-tests)

add_executable(
  naim-interaction-request-contract-support-tests
  controller/src/interaction/interaction_request_contract_support.cpp
  controller/src/interaction/interaction_request_contract_support_tests.cpp
)
target_include_directories(
  naim-interaction-request-contract-support-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-request-contract-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-interaction-request-contract-support-tests)

add_executable(
  naim-interaction-request-identity-support-tests
  controller/src/interaction/interaction_request_identity_support.cpp
  controller/src/interaction/interaction_request_identity_support_tests.cpp
)
target_include_directories(
  naim-interaction-request-identity-support-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-request-identity-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-interaction-request-identity-support-tests)

add_executable(
  naim-interaction-model-identity-builder-tests
  controller/src/interaction/interaction_model_identity_builder.cpp
  controller/src/interaction/interaction_model_identity_builder_tests.cpp
  controller/src/interaction/interaction_request_contract_support.cpp
)
target_include_directories(
  naim-interaction-model-identity-builder-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-model-identity-builder-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-interaction-model-identity-builder-tests)

add_executable(
  naim-interaction-runtime-text-support-tests
  controller/src/interaction/interaction_runtime_text_support.cpp
  controller/src/interaction/interaction_runtime_text_support_tests.cpp
)
target_include_directories(
  naim-interaction-runtime-text-support-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-runtime-text-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-interaction-runtime-text-support-tests)

add_executable(
  naim-interaction-text-post-processor-tests
  controller/src/interaction/interaction_text_post_processor.cpp
  controller/src/interaction/interaction_text_post_processor_tests.cpp
)
target_include_directories(
  naim-interaction-text-post-processor-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-text-post-processor-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-interaction-text-post-processor-tests)

add_executable(
  naim-interaction-upstream-event-parser-tests
  controller/src/interaction/interaction_upstream_event_parser.cpp
  controller/src/interaction/interaction_upstream_event_parser_tests.cpp
)
target_include_directories(
  naim-interaction-upstream-event-parser-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-upstream-event-parser-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-interaction-upstream-event-parser-tests)

add_executable(
  naim-interaction-conversation-payload-builder-tests
  controller/src/interaction/interaction_conversation_payload_builder.cpp
  controller/src/interaction/interaction_conversation_payload_builder_tests.cpp
)
target_include_directories(
  naim-interaction-conversation-payload-builder-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-conversation-payload-builder-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-interaction-conversation-payload-builder-tests)

add_executable(
  naim-interaction-conversation-record-builder-tests
  controller/src/interaction/interaction_conversation_payload_builder.cpp
  controller/src/interaction/interaction_conversation_record_builder.cpp
  controller/src/interaction/interaction_conversation_record_builder_tests.cpp
)
target_include_directories(
  naim-interaction-conversation-record-builder-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-conversation-record-builder-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-interaction-conversation-record-builder-tests)

add_executable(
  naim-interaction-conversation-archive-service-tests
  controller/src/app/controller_time_support.cpp
  controller/src/interaction/interaction_conversation_archive_service.cpp
  controller/src/interaction/interaction_conversation_payload_builder.cpp
  controller/src/interaction/interaction_conversation_record_builder.cpp
  controller/src/interaction/interaction_conversation_archive_service_tests.cpp
)
target_include_directories(
  naim-interaction-conversation-archive-service-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-conversation-archive-service-tests PRIVATE naim-common)
if(TARGET zstd::libzstd_shared)
  target_link_libraries(
    naim-interaction-conversation-archive-service-tests PRIVATE zstd::libzstd_shared)
elseif(TARGET zstd::libzstd_static)
  target_link_libraries(
    naim-interaction-conversation-archive-service-tests PRIVATE zstd::libzstd_static)
endif()
naim_enable_strict_warnings(naim-interaction-conversation-archive-service-tests)

add_executable(
  naim-interaction-stream-http-error-response-builder-tests
  controller/src/app/controller_composition_support.cpp
  controller/src/app/controller_request_context.cpp
  controller/src/infra/controller_network_manager.cpp
  controller/src/interaction/interaction_completion_policy_support.cpp
  controller/src/interaction/interaction_contract_responder.cpp
  controller/src/interaction/interaction_request_identity_support.cpp
  controller/src/interaction/interaction_request_heuristics.cpp
  controller/src/interaction/interaction_stream_http_error_response_builder.cpp
  controller/src/interaction/interaction_stream_http_error_response_builder_tests.cpp
  controller/src/interaction/interaction_request_contract_support.cpp
  controller/src/interaction/interaction_text_post_processor.cpp
  controller/src/interaction/interaction_upstream_event_parser.cpp
  controller/src/observation/plane_observation_matcher.cpp
)
target_include_directories(
  naim-interaction-stream-http-error-response-builder-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-stream-http-error-response-builder-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-interaction-stream-http-error-response-builder-tests)

add_executable(
  naim-interaction-sse-frame-builder-tests
  controller/src/interaction/interaction_sse_frame_builder.cpp
  controller/src/interaction/interaction_sse_frame_builder_tests.cpp
)
target_include_directories(
  naim-interaction-sse-frame-builder-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-sse-frame-builder-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-interaction-sse-frame-builder-tests)

add_executable(
  naim-interaction-stream-http-request-preparation-service-tests
  controller/src/app/controller_composition_support.cpp
  controller/src/app/controller_request_context.cpp
  controller/src/app/controller_time_support.cpp
  controller/src/auth/auth_support_service.cpp
  controller/src/browsing/plane_browsing_service.cpp
  controller/src/http/controller_http_transport.cpp
  controller/src/infra/controller_network_manager.cpp
  controller/src/interaction/interaction_completion_policy_support.cpp
  controller/src/interaction/interaction_conversation_archive_service.cpp
  controller/src/interaction/interaction_conversation_payload_builder.cpp
  controller/src/interaction/interaction_conversation_record_builder.cpp
  controller/src/interaction/interaction_conversation_service.cpp
  controller/src/interaction/interaction_contract_responder.cpp
  controller/src/interaction/interaction_model_identity_builder.cpp
  controller/src/interaction/interaction_request_contract_support.cpp
  controller/src/interaction/interaction_request_identity_support.cpp
  controller/src/interaction/interaction_request_heuristics.cpp
  controller/src/interaction/interaction_replica_group_summary_builder.cpp
  controller/src/interaction/interaction_runtime_text_support.cpp
  controller/src/interaction/interaction_service.cpp
  controller/src/interaction/interaction_target_relay_policy.cpp
  controller/src/interaction/interaction_stream_http_error_response_builder.cpp
  controller/src/interaction/interaction_stream_http_request_preparation_service.cpp
  controller/src/interaction/interaction_stream_http_request_preparation_service_tests.cpp
  controller/src/interaction/interaction_text_post_processor.cpp
  controller/src/interaction/interaction_upstream_event_parser.cpp
  controller/src/observation/plane_observation_matcher.cpp
  controller/src/skills/plane_skill_contextual_resolver_service.cpp
  controller/src/skills/plane_skill_runtime_sync_service.cpp
  controller/src/skills/plane_skills_target_resolver.cpp
  controller/src/skills/plane_skills_service.cpp
)
target_include_directories(
  naim-interaction-stream-http-request-preparation-service-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-stream-http-request-preparation-service-tests PRIVATE naim-common)
if(TARGET zstd::libzstd_shared)
  target_link_libraries(
    naim-interaction-stream-http-request-preparation-service-tests PRIVATE zstd::libzstd_shared)
elseif(TARGET zstd::libzstd_static)
  target_link_libraries(
    naim-interaction-stream-http-request-preparation-service-tests PRIVATE zstd::libzstd_static)
endif()
naim_enable_strict_warnings(
  naim-interaction-stream-http-request-preparation-service-tests)

add_executable(
  naim-interaction-continuation-payload-builder-tests
  controller/src/interaction/interaction_continuation_payload_builder.cpp
  controller/src/interaction/interaction_continuation_payload_builder_tests.cpp
  controller/src/interaction/interaction_completion_policy_support.cpp
  controller/src/interaction/interaction_request_identity_support.cpp
  controller/src/interaction/interaction_request_heuristics.cpp
  controller/src/interaction/interaction_text_post_processor.cpp
  controller/src/interaction/interaction_upstream_event_parser.cpp
  controller/src/interaction/interaction_request_contract_support.cpp
)
target_include_directories(
  naim-interaction-continuation-payload-builder-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-continuation-payload-builder-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-interaction-continuation-payload-builder-tests)

add_executable(
  naim-interaction-http-support-tests
  controller/src/app/controller_language_support.cpp
  controller/src/app/controller_composition_support.cpp
  controller/src/app/controller_request_context.cpp
  controller/src/http/controller_http_transport.cpp
  controller/src/infra/controller_network_manager.cpp
  controller/src/infra/controller_runtime_support_service.cpp
  controller/src/interaction/interaction_continuation_payload_builder.cpp
  controller/src/interaction/interaction_completion_policy_support.cpp
  controller/src/interaction/interaction_hostd_runtime_relay_service.cpp
  controller/src/interaction/interaction_http_support.cpp
  controller/src/interaction/interaction_model_identity_builder.cpp
  controller/src/interaction/interaction_payload_builder.cpp
  controller/src/interaction/interaction_request_contract_support.cpp
  controller/src/interaction/interaction_request_identity_support.cpp
  controller/src/interaction/interaction_request_heuristics.cpp
  controller/src/interaction/interaction_replica_group_summary_builder.cpp
  controller/src/interaction/interaction_text_post_processor.cpp
  controller/src/interaction/interaction_utf8_payload_sanitizer.cpp
  controller/src/interaction/interaction_http_support_tests.cpp
  controller/src/interaction/interaction_runtime_support_service.cpp
  controller/src/interaction/interaction_upstream_event_parser.cpp
  controller/src/observation/plane_observation_matcher.cpp
  controller/src/plane/desired_state_policy_service.cpp
)
target_include_directories(
  naim-interaction-http-support-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-http-support-tests PRIVATE naim-common Threads::Threads)
naim_enable_strict_warnings(naim-interaction-http-support-tests)

add_executable(
  naim-interaction-replica-group-summary-builder-tests
  controller/src/interaction/interaction_replica_group_summary_builder.cpp
  controller/src/interaction/interaction_replica_group_summary_builder_tests.cpp
)
target_include_directories(
  naim-interaction-replica-group-summary-builder-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-replica-group-summary-builder-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-interaction-replica-group-summary-builder-tests)

add_executable(
  naim-interaction-utf8-payload-sanitizer-tests
  controller/src/interaction/interaction_utf8_payload_sanitizer.cpp
  controller/src/interaction/interaction_utf8_payload_sanitizer_tests.cpp
)
target_include_directories(
  naim-interaction-utf8-payload-sanitizer-tests PRIVATE common/include controller/include)
target_link_libraries(
  naim-interaction-utf8-payload-sanitizer-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-interaction-utf8-payload-sanitizer-tests)

add_library(
  naim-webgateway-runtime
  controller/src/http/controller_http_server_support.cpp
  controller/src/infra/controller_network_manager.cpp
  runtime/browsing/src/cef_browser_backend.cpp
  runtime/browsing/src/cef_support.cpp
  runtime/browsing/src/process_support.cpp
  runtime/browsing/src/browsing_server.cpp
)
target_include_directories(
  naim-webgateway-runtime PUBLIC common/include controller/include runtime/browsing/include)
target_link_libraries(
  naim-webgateway-runtime PRIVATE naim-common Threads::Threads)
if(NAIM_WITH_CEF)
  target_link_libraries(naim-webgateway-runtime PRIVATE naim-cef naim-cef-wrapper)
  target_compile_definitions(
    naim-webgateway-runtime PRIVATE NAIM_WITH_CEF=1 USING_CEF_SHARED=1)
endif()
naim_enable_strict_warnings(naim-webgateway-runtime)

add_executable(
  naim-webgateway-tests
  runtime/browsing/src/browsing_server_tests.cpp
)
target_include_directories(
  naim-webgateway-tests PRIVATE common/include controller/include runtime/browsing/include)
target_link_libraries(
  naim-webgateway-tests PRIVATE naim-webgateway-runtime naim-common Threads::Threads)
naim_enable_strict_warnings(naim-webgateway-tests)

add_executable(
  naim-hostd-local-state-tests
  hostd/src/app/hostd_command_support.cpp
  hostd/src/app/hostd_desired_state_path_support.cpp
  hostd/src/app/hostd_local_runtime_state_support.cpp
  hostd/src/app/hostd_local_state_path_support.cpp
  hostd/src/app/hostd_local_state_repository.cpp
  hostd/src/app/hostd_runtime_telemetry_support.cpp
  hostd/src/app/hostd_system_telemetry_collector.cpp
  hostd/src/app/hostd_local_state_support_tests.cpp
)
target_include_directories(naim-hostd-local-state-tests PRIVATE common/include hostd/include)
target_link_libraries(naim-hostd-local-state-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-local-state-tests)

add_executable(
  naim-hostd-command-support-tests
  hostd/src/app/hostd_command_support.cpp
  hostd/src/app/hostd_command_support_tests.cpp
)
target_include_directories(naim-hostd-command-support-tests PRIVATE common/include hostd/include)
target_link_libraries(naim-hostd-command-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-command-support-tests)

add_executable(
  naim-hostd-post-deploy-support-tests
  hostd/src/app/hostd_command_support.cpp
  hostd/src/app/hostd_local_state_path_support.cpp
  hostd/src/app/hostd_post_deploy_support.cpp
  hostd/src/app/hostd_repo_root_support.cpp
  hostd/src/app/hostd_post_deploy_support_tests.cpp
)
target_include_directories(naim-hostd-post-deploy-support-tests PRIVATE common/include hostd/include)
target_link_libraries(naim-hostd-post-deploy-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-post-deploy-support-tests)

add_executable(
  naim-hostd-disk-runtime-support-tests
  hostd/src/app/hostd_command_support.cpp
  hostd/src/app/hostd_desired_state_path_support.cpp
  hostd/src/app/hostd_disk_runtime_support.cpp
  hostd/src/app/hostd_file_support.cpp
  hostd/src/app/hostd_disk_runtime_support_tests.cpp
)
target_include_directories(
  naim-hostd-disk-runtime-support-tests PRIVATE common/include hostd/include)
target_link_libraries(naim-hostd-disk-runtime-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-disk-runtime-support-tests)

add_executable(
  naim-hostd-file-support-tests
  hostd/src/app/hostd_file_support.cpp
  hostd/src/app/hostd_file_support_tests.cpp
)
target_include_directories(naim-hostd-file-support-tests PRIVATE common/include hostd/include)
target_link_libraries(naim-hostd-file-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-file-support-tests)

add_executable(
  naim-hostd-desired-state-path-support-tests
  hostd/src/app/hostd_desired_state_path_support.cpp
  hostd/src/app/hostd_desired_state_path_support_tests.cpp
)
target_include_directories(
  naim-hostd-desired-state-path-support-tests PRIVATE common/include hostd/include)
target_link_libraries(naim-hostd-desired-state-path-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-desired-state-path-support-tests)

add_executable(
  naim-hostd-desired-state-display-support-tests
  hostd/src/app/hostd_command_support.cpp
  hostd/src/app/hostd_desired_state_display_support.cpp
  hostd/src/app/hostd_desired_state_path_support.cpp
  hostd/src/app/hostd_local_runtime_state_support.cpp
  hostd/src/app/hostd_local_state_path_support.cpp
  hostd/src/app/hostd_local_state_repository.cpp
  hostd/src/app/hostd_runtime_telemetry_support.cpp
  hostd/src/app/hostd_system_telemetry_collector.cpp
  hostd/src/app/hostd_desired_state_display_support_tests.cpp
)
target_include_directories(
  naim-hostd-desired-state-display-support-tests PRIVATE common/include hostd/include)
target_link_libraries(naim-hostd-desired-state-display-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-desired-state-display-support-tests)

add_executable(
  naim-hostd-desired-state-apply-plan-support-tests
  hostd/src/app/hostd_command_support.cpp
  hostd/src/app/hostd_compose_runtime_support.cpp
  hostd/src/app/hostd_desired_state_apply_plan_support.cpp
  hostd/src/app/hostd_desired_state_path_support.cpp
  hostd/src/app/hostd_disk_runtime_support.cpp
  hostd/src/app/hostd_file_support.cpp
  hostd/src/app/hostd_repo_root_support.cpp
  hostd/src/app/hostd_desired_state_apply_plan_support_tests.cpp
)
target_include_directories(
  naim-hostd-desired-state-apply-plan-support-tests PRIVATE common/include hostd/include)
target_link_libraries(
  naim-hostd-desired-state-apply-plan-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-desired-state-apply-plan-support-tests)

add_executable(
  naim-hostd-bootstrap-model-artifact-support-tests
  hostd/src/app/hostd_bootstrap_model_artifact_support.cpp
  hostd/src/app/hostd_desired_state_path_support.cpp
  hostd/src/app/hostd_bootstrap_model_artifact_support_tests.cpp
)
target_include_directories(
  naim-hostd-bootstrap-model-artifact-support-tests PRIVATE common/include hostd/include)
target_link_libraries(
  naim-hostd-bootstrap-model-artifact-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-bootstrap-model-artifact-support-tests)

add_executable(
  naim-hostd-bootstrap-active-model-support-tests
  hostd/src/app/hostd_bootstrap_active_model_support.cpp
  hostd/src/app/hostd_bootstrap_model_artifact_support.cpp
  hostd/src/app/hostd_desired_state_path_support.cpp
  hostd/src/app/hostd_file_support.cpp
  hostd/src/app/hostd_local_state_path_support.cpp
  hostd/src/app/hostd_local_state_repository.cpp
  hostd/src/app/hostd_bootstrap_active_model_support_tests.cpp
)
target_include_directories(
  naim-hostd-bootstrap-active-model-support-tests PRIVATE common/include hostd/include)
target_link_libraries(
  naim-hostd-bootstrap-active-model-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-bootstrap-active-model-support-tests)

add_executable(
  naim-hostd-bootstrap-transfer-support-tests
  hostd/src/app/hostd_bootstrap_transfer_support.cpp
  hostd/src/app/hostd_command_support.cpp
  hostd/src/app/hostd_observed_state_snapshot_builder.cpp
  hostd/src/app/hostd_desired_state_path_support.cpp
  hostd/src/app/hostd_file_support.cpp
  hostd/src/app/hostd_local_runtime_state_support.cpp
  hostd/src/app/hostd_local_state_path_support.cpp
  hostd/src/app/hostd_local_state_repository.cpp
  hostd/src/app/hostd_reporting_support.cpp
  hostd/src/app/hostd_runtime_telemetry_support.cpp
  hostd/src/app/hostd_system_telemetry_collector.cpp
  hostd/src/app/hostd_controller_transport_support.cpp
  hostd/src/app/hostd_bootstrap_transfer_support_tests.cpp
)
target_include_directories(
  naim-hostd-bootstrap-transfer-support-tests PRIVATE common/include hostd/include)
target_link_libraries(
  naim-hostd-bootstrap-transfer-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-bootstrap-transfer-support-tests)

add_executable(
  naim-hostd-bootstrap-model-support-factory-tests
  hostd/src/app/hostd_bootstrap_active_model_support.cpp
  hostd/src/app/hostd_bootstrap_model_artifact_support.cpp
  hostd/src/app/hostd_bootstrap_model_support.cpp
  hostd/src/app/hostd_bootstrap_model_support_factory.cpp
  hostd/src/app/hostd_bootstrap_transfer_support.cpp
  hostd/src/app/hostd_command_support.cpp
  hostd/src/app/hostd_observed_state_snapshot_builder.cpp
  hostd/src/app/hostd_desired_state_path_support.cpp
  hostd/src/app/hostd_file_support.cpp
  hostd/src/app/hostd_local_runtime_state_support.cpp
  hostd/src/app/hostd_local_state_path_support.cpp
  hostd/src/app/hostd_local_state_repository.cpp
  hostd/src/app/hostd_reporting_support.cpp
  hostd/src/app/hostd_runtime_telemetry_support.cpp
  hostd/src/app/hostd_system_telemetry_collector.cpp
  hostd/src/app/hostd_controller_transport_support.cpp
  hostd/src/app/hostd_bootstrap_model_support_factory_tests.cpp
)
target_include_directories(
  naim-hostd-bootstrap-model-support-factory-tests PRIVATE common/include hostd/include)
target_link_libraries(
  naim-hostd-bootstrap-model-support-factory-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-bootstrap-model-support-factory-tests)

add_executable(
  naim-hostd-desired-state-apply-support-tests
  hostd/src/app/hostd_bootstrap_active_model_support.cpp
  hostd/src/app/hostd_bootstrap_model_artifact_support.cpp
  hostd/src/app/hostd_bootstrap_model_support.cpp
  hostd/src/app/hostd_bootstrap_model_support_factory.cpp
  hostd/src/app/hostd_bootstrap_transfer_support.cpp
  hostd/src/app/hostd_command_support.cpp
  hostd/src/app/hostd_compose_runtime_support.cpp
  hostd/src/app/hostd_container_name_support.cpp
  hostd/src/app/hostd_controller_transport_support.cpp
  hostd/src/app/hostd_desired_state_apply_plan_support.cpp
  hostd/src/app/hostd_desired_state_apply_support.cpp
  hostd/src/app/hostd_desired_state_display_support.cpp
  hostd/src/app/hostd_desired_state_path_support.cpp
  hostd/src/app/hostd_disk_runtime_support.cpp
  hostd/src/app/hostd_file_support.cpp
  hostd/src/app/hostd_local_runtime_state_support.cpp
  hostd/src/app/hostd_local_state_path_support.cpp
  hostd/src/app/hostd_local_state_repository.cpp
  hostd/src/app/hostd_model_artifact_request_support.cpp
  hostd/src/app/hostd_observed_state_snapshot_builder.cpp
  hostd/src/app/hostd_post_deploy_support.cpp
  hostd/src/app/hostd_reporting_support.cpp
  hostd/src/app/hostd_repo_root_support.cpp
  hostd/src/app/hostd_runtime_http_proxy.cpp
  hostd/src/app/hostd_runtime_telemetry_support.cpp
  hostd/src/app/hostd_system_telemetry_collector.cpp
  hostd/src/app/hostd_desired_state_apply_support_tests.cpp
)
target_include_directories(
  naim-hostd-desired-state-apply-support-tests PRIVATE common/include hostd/include)
target_link_libraries(
  naim-hostd-desired-state-apply-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-desired-state-apply-support-tests)

add_executable(
  naim-hostd-reporting-support-tests
  hostd/src/app/hostd_command_support.cpp
  hostd/src/app/hostd_controller_transport_support.cpp
  hostd/src/app/hostd_desired_state_path_support.cpp
  hostd/src/app/hostd_local_runtime_state_support.cpp
  hostd/src/app/hostd_local_state_path_support.cpp
  hostd/src/app/hostd_local_state_repository.cpp
  hostd/src/app/hostd_observed_state_snapshot_builder.cpp
  hostd/src/app/hostd_reporting_support.cpp
  hostd/src/app/hostd_runtime_telemetry_support.cpp
  hostd/src/app/hostd_system_telemetry_collector.cpp
  hostd/src/app/hostd_reporting_support_tests.cpp
)
target_include_directories(
  naim-hostd-reporting-support-tests PRIVATE common/include hostd/include)
target_link_libraries(naim-hostd-reporting-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-reporting-support-tests)

add_executable(
  naim-hostd-repo-root-support-tests
  hostd/src/app/hostd_repo_root_support.cpp
  hostd/src/app/hostd_repo_root_support_tests.cpp
)
target_include_directories(
  naim-hostd-repo-root-support-tests PRIVATE common/include hostd/include)
target_link_libraries(naim-hostd-repo-root-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-repo-root-support-tests)

add_executable(
  naim-hostd-assignment-lock-tests
  hostd/src/state_apply/hostd_assignment_lock.cpp
  hostd/src/state_apply/hostd_assignment_lock_tests.cpp
)
target_include_directories(naim-hostd-assignment-lock-tests PRIVATE common/include hostd/include)
target_link_libraries(naim-hostd-assignment-lock-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-assignment-lock-tests)

add_executable(
  naim-hostd-cli-request-resolution-support-tests
  hostd/src/cli/hostd_command_line.cpp
  hostd/src/cli/hostd_cli_request_resolution_support.cpp
  hostd/src/cli/hostd_cli_request_resolution_support_tests.cpp
)
target_include_directories(
  naim-hostd-cli-request-resolution-support-tests PRIVATE common/include hostd/include)
target_link_libraries(
  naim-hostd-cli-request-resolution-support-tests PRIVATE naim-common)
naim_enable_strict_warnings(naim-hostd-cli-request-resolution-support-tests)

add_executable(
  naim-controller
  controller/src/auth/auth_http_service.cpp
  controller/src/auth/auth_http_support.cpp
  controller/src/auth/auth_payload_service.cpp
  controller/src/auth/auth_support_service.cpp
  controller/src/host/host_assignment_reconciliation_service.cpp
  controller/src/observation/plane_observation_matcher.cpp
  controller/src/scheduler/assignment_orchestration_service.cpp
  controller/src/bundle/bundle_cli_service.cpp
  controller/src/bundle/bundle_http_service.cpp
  controller/src/app/controller_cli.cpp
  controller/src/infra/controller_action.cpp
  controller/src/app/controller_command_line.cpp
  controller/src/app/controller_component_factory.cpp
  controller/src/app/controller_component_defaults.cpp
  controller/src/app/controller_http_component_factory.cpp
  controller/src/app/controller_plane_component_factory.cpp
  controller/src/app/controller_read_model_component_factory.cpp
  controller/src/app/controller_scheduler_component_factory.cpp
  controller/src/app/controller_component_factory_support.cpp
  controller/src/app/controller_composition_support.cpp
  controller/src/app/controller_http_service_support.cpp
  controller/src/app/controller_listener_policy.cpp
  controller/src/app/controller_plane_support.cpp
  controller/src/app/controller_read_model_support.cpp
  controller/src/app/controller_route_summary_builder.cpp
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
  controller/src/browsing/interaction_browsing_service.cpp
  controller/src/browsing/plane_browsing_service.cpp
  controller/src/plane/dashboard_service.cpp
  controller/src/plane/plane_placement_payload_builder.cpp
  controller/src/plane/plane_desired_state_request_parser.cpp
  controller/src/infra/controller_ui_service.cpp
  controller/src/host/host_registry_service.cpp
  controller/src/host/hostd_http_service.cpp
  controller/src/host/hostd_http_support.cpp
  controller/src/interaction/interaction_conversation_archive_service.cpp
  controller/src/interaction/interaction_continuation_payload_builder.cpp
  controller/src/interaction/interaction_conversation_payload_builder.cpp
  controller/src/interaction/interaction_conversation_record_builder.cpp
  controller/src/interaction/interaction_completion_policy_support.cpp
  controller/src/interaction/interaction_contract_responder.cpp
  controller/src/interaction/interaction_conversation_service.cpp
  controller/src/interaction/interaction_http_service.cpp
  controller/src/interaction/interaction_hostd_runtime_relay_service.cpp
  controller/src/interaction/interaction_http_executor_factory.cpp
  controller/src/interaction/interaction_http_support.cpp
  controller/src/interaction/interaction_model_identity_builder.cpp
  controller/src/interaction/interaction_payload_builder.cpp
  controller/src/interaction/interaction_request_contract_support.cpp
  controller/src/interaction/interaction_request_identity_support.cpp
  controller/src/interaction/interaction_request_heuristics.cpp
  controller/src/interaction/interaction_replica_group_summary_builder.cpp
  controller/src/interaction/interaction_runtime_text_support.cpp
  controller/src/interaction/interaction_sse_frame_builder.cpp
  controller/src/interaction/interaction_stream_http_error_response_builder.cpp
  controller/src/interaction/interaction_stream_http_request_preparation_service.cpp
  controller/src/interaction/interaction_target_relay_policy.cpp
  controller/src/interaction/interaction_text_post_processor.cpp
  controller/src/interaction/interaction_upstream_event_parser.cpp
  controller/src/interaction/interaction_utf8_payload_sanitizer.cpp
  controller/src/interaction/interaction_runtime_support_service.cpp
  controller/src/interaction/interaction_service.cpp
  controller/src/knowledge/knowledge_vault_http_service.cpp
  controller/src/knowledge/knowledge_vault_service.cpp
  controller/src/knowledge/knowledge_vault_service_repository.cpp
  controller/src/model/model_library_http_service.cpp
  controller/src/model/model_library_http_support.cpp
  controller/src/model/model_conversion_service.cpp
  controller/src/model/model_library_node_placement.cpp
  controller/src/model/model_library_service.cpp
  controller/src/model/model_library_support.cpp
  controller/src/plane/plane_http_service.cpp
  controller/src/plane/plane_http_support.cpp
  controller/src/plane/plane_dashboard_skills_summary_service.cpp
  controller/src/plane/plane_lifecycle_support.cpp
  controller/src/plane/plane_mutation_service.cpp
  controller/src/plane/plane_realization_service.cpp
  controller/src/plane/plane_registry_query_support.cpp
  controller/src/plane/plane_registry_service.cpp
  controller/src/plane/plane_state_presentation_support.cpp
  controller/src/read_model/read_model_cli_service.cpp
  controller/src/read_model/read_model_http_service.cpp
  controller/src/read_model/read_model_http_support.cpp
  controller/src/read_model/read_model_service.cpp
  controller/src/app/remote_controller_cli_service.cpp
  controller/src/scheduler/scheduler_cli_service.cpp
  controller/src/scheduler/scheduler_domain_service.cpp
  controller/src/scheduler/scheduler_domain_support.cpp
  controller/src/scheduler/scheduler_execution_dependencies.cpp
  controller/src/scheduler/scheduler_execution_support.cpp
  controller/src/scheduler/scheduler_http_service.cpp
  controller/src/scheduler/scheduler_view_service.cpp
  controller/src/plane/plane_service.cpp
  controller/src/skills/plane_skill_contextual_resolver_service.cpp
  controller/src/skills/plane_skill_catalog_service.cpp
  controller/src/skills/plane_skill_runtime_sync_service.cpp
  controller/src/skills/plane_skills_target_resolver.cpp
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
target_include_directories(naim-controller PRIVATE controller/include)
target_link_libraries(naim-controller PRIVATE naim-common)
if(TARGET zstd::libzstd_shared)
  target_link_libraries(naim-controller PRIVATE zstd::libzstd_shared)
elseif(TARGET zstd::libzstd_static)
  target_link_libraries(naim-controller PRIVATE zstd::libzstd_static)
endif()
target_compile_definitions(
  naim-controller
  PRIVATE
    NAIM_LLAMA_CPP_SOURCE_TREE="${llama_cpp_SOURCE_DIR}"
    NAIM_LLAMA_CPP_RUNTIME_OUTPUT_DIR="${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
)
if(NAIM_CONTROLLER_BUILD_LLAMA_QUANTIZE AND TARGET llama-quantize)
  add_dependencies(naim-controller llama-quantize)
endif()
if(WIN32)
  target_link_libraries(naim-controller PRIVATE ws2_32)
endif()
naim_enable_strict_warnings(naim-controller)

add_executable(
  naim-hostd
  hostd/src/app/hostd_composition_root.cpp
  hostd/src/app/hostd_service_component_factory.cpp
  hostd/src/app/hostd_support_component_factory.cpp
  hostd/src/app/hostd_app_assignment_support.cpp
  hostd/src/app/hostd_app_controller_support.cpp
  hostd/src/app/hostd_app_observation_support.cpp
  hostd/src/app/hostd_bootstrap_active_model_support.cpp
  hostd/src/app/hostd_bootstrap_model_artifact_support.cpp
  hostd/src/app/hostd_bootstrap_model_support.cpp
  hostd/src/app/hostd_bootstrap_model_support_factory.cpp
  hostd/src/app/hostd_bootstrap_transfer_support.cpp
  hostd/src/app/hostd_command_support.cpp
  hostd/src/app/hostd_compose_runtime_support.cpp
  hostd/src/app/hostd_container_name_support.cpp
  hostd/src/app/hostd_controller_transport_support.cpp
  hostd/src/app/hostd_desired_state_apply_plan_support.cpp
  hostd/src/app/hostd_desired_state_apply_support.cpp
  hostd/src/app/hostd_desired_state_display_support.cpp
  hostd/src/app/hostd_desired_state_path_support.cpp
  hostd/src/app/hostd_disk_runtime_support.cpp
  hostd/src/app/hostd_file_support.cpp
  hostd/src/app/hostd_local_runtime_state_support.cpp
  hostd/src/app/hostd_local_state_path_support.cpp
  hostd/src/app/hostd_local_state_repository.cpp
  hostd/src/app/hostd_model_artifact_request_support.cpp
  hostd/src/app/hostd_observed_state_snapshot_builder.cpp
  hostd/src/app/hostd_post_deploy_support.cpp
  hostd/src/app/hostd_reporting_support.cpp
  hostd/src/app/hostd_repo_root_support.cpp
  hostd/src/app/hostd_runtime_http_proxy.cpp
  hostd/src/app/hostd_runtime_telemetry_support.cpp
  hostd/src/app/hostd_system_telemetry_collector.cpp
  hostd/src/backend/hostd_backend_factory.cpp
  hostd/src/backend/http_hostd_backend.cpp
  hostd/src/backend/local_db_hostd_backend.cpp
  hostd/src/cli/hostd_cli_command_dispatcher.cpp
  hostd/src/cli/hostd_cli_request_resolution_support.cpp
  hostd/src/observation/hostd_observation_service.cpp
  hostd/src/state_apply/hostd_assignment_lock.cpp
  hostd/src/state_apply/hostd_assignment_service.cpp
  hostd/src/cli/hostd_cli.cpp
  hostd/src/cli/hostd_command_line.cpp
  hostd/src/config/node_config_loader.cpp
  hostd/src/main.cpp
  hostd/src/app/hostd_app.cpp
)
target_include_directories(naim-hostd PRIVATE hostd/include)
target_link_libraries(naim-hostd PRIVATE naim-common)
if(UNIX AND NOT APPLE)
  target_link_libraries(naim-hostd PRIVATE dl)
endif()
if(WIN32)
  target_link_libraries(naim-hostd PRIVATE ws2_32)
endif()
naim_enable_strict_warnings(naim-hostd)

add_executable(
  naim-node
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
  launcher/src/run/hostd_peer_service.cpp
  launcher/src/run/launcher_run_service.cpp
  hostd/src/app/hostd_controller_transport_support.cpp
  hostd/src/backend/http_hostd_backend.cpp
  launcher/src/main.cpp
  launcher/src/app/launcher_app.cpp
)
target_include_directories(naim-node PRIVATE launcher/include hostd/include)
target_link_libraries(naim-node PRIVATE naim-common)
if(UNIX AND NOT APPLE)
  target_link_libraries(naim-node PRIVATE Threads::Threads)
endif()
naim_enable_strict_warnings(naim-node)

if(NOT WIN32)
  add_executable(
    naim-devtool
    tools/devtool/src/http_client.cpp
    tools/devtool/src/main.cpp
  )
  target_include_directories(naim-devtool PRIVATE tools/devtool/include)
  target_link_libraries(
    naim-devtool
    PRIVATE
      nlohmann_json::nlohmann_json
      Threads::Threads
  )
  naim_enable_strict_warnings(naim-devtool)

  add_executable(
    naim-webgatewayd
    runtime/browsing/src/main.cpp
  )
  target_include_directories(
    naim-webgatewayd PRIVATE common/include controller/include runtime/browsing/include)
  target_link_libraries(
    naim-webgatewayd
    PRIVATE
      naim-webgateway-runtime
      naim-common
      Threads::Threads
  )
  if(NAIM_WITH_CEF)
    target_compile_definitions(naim-webgatewayd PRIVATE NAIM_WITH_CEF=1 USING_CEF_SHARED=1)
    set_target_properties(
      naim-webgatewayd
      PROPERTIES
        BUILD_RPATH "$ORIGIN"
        INSTALL_RPATH "$ORIGIN"
    )
    naim_stage_cef_runtime(naim-webgatewayd)
  endif()
  naim_enable_strict_warnings(naim-webgatewayd)

  add_executable(
    naim-skillsd
    controller/src/http/controller_http_server_support.cpp
    controller/src/http/controller_http_transport.cpp
    controller/src/infra/controller_network_manager.cpp
    runtime/skills/src/skills_server.cpp
    runtime/skills/src/skills_store.cpp
    runtime/skills/src/main.cpp
    common/src/runtime/runtime_status.cpp
    common/src/state/sqlite_statement.cpp
  )
  target_include_directories(
    naim-skillsd PRIVATE common/include controller/include runtime/skills/include)
  target_link_libraries(
    naim-skillsd
    PRIVATE
      nlohmann_json::nlohmann_json
      unofficial::sqlite3::sqlite3
  )
  naim_enable_strict_warnings(naim-skillsd)

  add_executable(
    naim-knowledged
    controller/src/http/controller_http_server_support.cpp
    controller/src/http/controller_http_transport.cpp
    controller/src/infra/controller_network_manager.cpp
    runtime/knowledge/src/knowledge_runtime_config_loader.cpp
    runtime/knowledge/src/knowledge_server.cpp
    runtime/knowledge/src/knowledge_server_signal_handler.cpp
    runtime/knowledge/src/knowledge_store_keys.cpp
    runtime/knowledge/src/knowledge_store.cpp
    runtime/knowledge/src/knowledge_text_processor.cpp
    runtime/knowledge/src/rocksdb_json_repository.cpp
    runtime/knowledge/src/main.cpp
  )
  target_include_directories(
    naim-knowledged PRIVATE common/include controller/include runtime/knowledge/include)
  target_link_libraries(
    naim-knowledged
    PRIVATE
      naim-common
      RocksDB::rocksdb
      Threads::Threads
  )
  if(WIN32)
    target_link_libraries(naim-knowledged PRIVATE ws2_32)
  endif()
  naim_enable_strict_warnings(naim-knowledged)

  add_executable(
    naim-inferctl
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
  target_include_directories(naim-inferctl PRIVATE common/include runtime/infer/include)
  target_include_directories(
    naim-inferctl
    BEFORE PRIVATE
      "${llama_cpp_SOURCE_DIR}/include"
      "${llama_cpp_SOURCE_DIR}/ggml/include"
  )
  target_link_libraries(
    naim-inferctl
    PRIVATE
      naim-common
      llama
  )
  naim_enable_strict_warnings(naim-inferctl)

  add_executable(
    naim-workerd
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
  target_include_directories(naim-workerd PRIVATE common/include runtime/worker/include)
  # target_include_directories(
  #   naim-workerd
  #   BEFORE PRIVATE
  #     "${llama_cpp_SOURCE_DIR}/include"
  #     "${llama_cpp_SOURCE_DIR}/ggml/include"
  # )
  target_link_libraries(
    naim-workerd
    PRIVATE
      nlohmann_json::nlohmann_json
      # llama
  )
  naim_enable_strict_warnings(naim-workerd)
endif()
