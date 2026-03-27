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

function(comet_enable_strict_warnings target_name)
  if(MSVC)
    target_compile_options(${target_name} PRIVATE /W4)
  else()
    target_compile_options(${target_name} PRIVATE -Wall -Wextra -Wpedantic)
  endif()
endfunction()

include(FetchContent)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")

set(COMET_LLAMA_CPP_GIT_TAG "ec2b787ebe975cab1fa592c2505a6d3a9e4ff2e7" CACHE STRING
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
set(LLAMA_BUILD_COMMON OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_SERVER OFF CACHE BOOL "" FORCE)
set(LLAMA_OPENSSL OFF CACHE BOOL "" FORCE)
set(LLAMA_USE_SYSTEM_GGML OFF CACHE BOOL "" FORCE)
set(GGML_NATIVE OFF CACHE BOOL "" FORCE)
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
if(_comet_cuda_available)
  set(GGML_CUDA ON CACHE BOOL "" FORCE)
else()
  set(GGML_CUDA OFF CACHE BOOL "" FORCE)
endif()

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
  common/src/compose_renderer.cpp
  common/src/crypto_utils.cpp
  common/src/demo_state.cpp
  common/src/execution_plan.cpp
  common/src/import_bundle.cpp
  common/src/infer_runtime_config.cpp
  common/src/planner.cpp
  common/src/reconcile.cpp
  common/src/runtime_status.cpp
  common/src/scheduling_policy.cpp
  common/src/state_json.cpp
  common/src/sqlite_store.cpp
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
  comet-controller
  controller/src/auth_http_service.cpp
  controller/src/auth_support_service.cpp
  controller/src/bundle_http_service.cpp
  controller/src/controller_cli.cpp
  controller/src/controller_action.cpp
  controller/src/controller_command_line.cpp
  controller/src/controller_http_server.cpp
  controller/src/controller_http_router.cpp
  controller/src/controller_network_manager.cpp
  controller/src/controller_http_server_support.cpp
  controller/src/controller_http_transport.cpp
  controller/src/controller_remote_client.cpp
  controller/src/host_registry_service.cpp
  controller/src/hostd_http_service.cpp
  controller/src/interaction_http_service.cpp
  controller/src/interaction_service.cpp
  controller/src/model_library_http_service.cpp
  controller/src/plane_http_service.cpp
  controller/src/read_model_http_service.cpp
  controller/src/scheduler_http_service.cpp
  controller/src/plane_service.cpp
  controller/src/scheduler_service.cpp
  controller/src/web_ui_service.cpp
  controller/src/main.cpp
)
target_include_directories(comet-controller PRIVATE controller/include)
target_link_libraries(comet-controller PRIVATE comet-common)
if(WIN32)
  target_link_libraries(comet-controller PRIVATE ws2_32)
endif()
comet_enable_strict_warnings(comet-controller)

add_executable(comet-hostd hostd/src/main.cpp)
target_link_libraries(comet-hostd PRIVATE comet-common)
if(UNIX AND NOT APPLE)
  target_link_libraries(comet-hostd PRIVATE dl)
endif()
if(WIN32)
  target_link_libraries(comet-hostd PRIVATE ws2_32)
endif()
comet_enable_strict_warnings(comet-hostd)

add_executable(comet-node launcher/src/main.cpp)
target_link_libraries(comet-node PRIVATE comet-common)
comet_enable_strict_warnings(comet-node)

if(NOT WIN32)
  add_executable(
    comet-inferctl
    runtime/infer/src/main.cpp
    common/src/runtime_status.cpp
  )
  target_include_directories(comet-inferctl PRIVATE common/include)
  target_include_directories(
    comet-inferctl
    BEFORE PRIVATE
      "${llama_cpp_SOURCE_DIR}/include"
      "${llama_cpp_SOURCE_DIR}/ggml/include"
  )
  target_link_libraries(
    comet-inferctl
    PRIVATE
      nlohmann_json::nlohmann_json
      llama
  )
  comet_enable_strict_warnings(comet-inferctl)

  add_executable(
    comet-workerd
    runtime/worker/src/main.cpp
    common/src/runtime_status.cpp
  )
  target_include_directories(comet-workerd PRIVATE common/include)
  target_include_directories(
    comet-workerd
    BEFORE PRIVATE
      "${llama_cpp_SOURCE_DIR}/include"
      "${llama_cpp_SOURCE_DIR}/ggml/include"
  )
  target_link_libraries(
    comet-workerd
    PRIVATE
      nlohmann_json::nlohmann_json
      llama
  )
  comet_enable_strict_warnings(comet-workerd)
endif()
