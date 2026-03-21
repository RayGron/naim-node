cmake_minimum_required(VERSION 3.22)

option(COMET_SKIP_VCPKG_TOOLCHAIN "Skip automatic vcpkg toolchain detection" OFF)

if(DEFINED VCPKG_INSTALLED_DIR AND DEFINED VCPKG_TARGET_TRIPLET)
  list(PREPEND CMAKE_PREFIX_PATH "${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}")
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

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")

find_package(unofficial-sqlite3 CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(llama CONFIG REQUIRED)

add_library(
  comet-common
  common/src/compose_renderer.cpp
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
    unofficial::sqlite3::sqlite3
)
target_compile_options(comet-common PRIVATE -Wall -Wextra -Wpedantic)

add_executable(comet-controller controller/src/main.cpp)
target_link_libraries(comet-controller PRIVATE comet-common)
target_compile_options(comet-controller PRIVATE -Wall -Wextra -Wpedantic)

add_executable(comet-hostd hostd/src/main.cpp)
target_link_libraries(comet-hostd PRIVATE comet-common)
if(UNIX AND NOT APPLE)
  target_link_libraries(comet-hostd PRIVATE dl)
endif()
target_compile_options(comet-hostd PRIVATE -Wall -Wextra -Wpedantic)

add_executable(
  comet-inferctl
  runtime/infer/src/main.cpp
  common/src/runtime_status.cpp
)
target_include_directories(comet-inferctl PRIVATE common/include)
target_link_libraries(
  comet-inferctl
  PRIVATE
    nlohmann_json::nlohmann_json
    llama
)
target_compile_options(comet-inferctl PRIVATE -Wall -Wextra -Wpedantic)

add_executable(
  comet-workerd
  runtime/worker/src/main.cpp
  common/src/runtime_status.cpp
)
target_include_directories(comet-workerd PRIVATE common/include)
target_link_libraries(
  comet-workerd
  PRIVATE
    nlohmann_json::nlohmann_json
    llama
)
target_compile_options(comet-workerd PRIVATE -Wall -Wextra -Wpedantic)
