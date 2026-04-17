#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/build-context.sh"

naim_resolve_build_context "${script_dir}" "$@"

cuda_root=""
cuda_nvcc=""
openmp_root=""
openmp_include_dir=""
openmp_library=""

detect_cuda_root() {
  local candidate=""
  for candidate in \
    "${CUDA_TOOLKIT_ROOT_DIR:-}" \
    "${CUDA_HOME:-}" \
    "${CUDA_PATH:-}" \
    "/usr/local/cuda-13.1" \
    "/usr/local/cuda" \
    "/usr/local/cuda-13.2" \
    "/usr/local/cuda-13.0" \
    "/usr/local/cuda-12.9" \
    "/usr/local/cuda-12.8" \
    "/usr/local/cuda-12.7" \
    "/usr/local/cuda-12.6" \
    "/usr/local/cuda-12.5" \
    "/usr/local/cuda-12.4" \
    "/usr/local/cuda-12.3" \
    "/usr/local/cuda-12.2" \
    "/usr/local/cuda-12.1" \
    "/usr/local/cuda-12.0" \
    "/usr/lib/nvidia-cuda-toolkit"; do
    if [[ -n "${candidate}" && -x "${candidate}/bin/nvcc" && -f "${candidate}/include/cuda_runtime.h" ]]; then
      cuda_root="${candidate}"
      cuda_nvcc="${candidate}/bin/nvcc"
      return 0
    fi
  done

  if command -v nvcc >/dev/null 2>&1; then
    candidate="$(dirname -- "$(dirname -- "$(readlink -f "$(command -v nvcc)")")")"
    if [[ -x "${candidate}/bin/nvcc" && -f "${candidate}/include/cuda_runtime.h" ]]; then
      cuda_root="${candidate}"
      cuda_nvcc="${candidate}/bin/nvcc"
      return 0
    fi
  fi

  return 1
}

detect_macos_openmp() {
  local candidate=""
  local root_candidates=()

  if [[ "${TARGET_OS}" != "macos" ]]; then
    return 1
  fi

  if command -v brew >/dev/null 2>&1; then
    candidate="$(brew --prefix libomp 2>/dev/null || true)"
    if [[ -n "${candidate}" ]]; then
      root_candidates+=("${candidate}")
    fi
  fi

  root_candidates+=(
    "/opt/homebrew/opt/libomp"
    "/usr/local/opt/libomp"
  )

  for candidate in "${root_candidates[@]}"; do
    if [[ -f "${candidate}/include/omp.h" ]]; then
      if [[ -f "${candidate}/lib/libomp.dylib" ]]; then
        openmp_root="${candidate}"
        openmp_include_dir="${candidate}/include"
        openmp_library="${candidate}/lib/libomp.dylib"
        return 0
      fi
      if [[ -f "${candidate}/lib/libomp.a" ]]; then
        openmp_root="${candidate}"
        openmp_include_dir="${candidate}/include"
        openmp_library="${candidate}/lib/libomp.a"
        return 0
      fi
    fi
  done

  return 1
}

if detect_cuda_root; then
  export CUDA_TOOLKIT_ROOT_DIR="${cuda_root}"
  export CUDA_HOME="${cuda_root}"
  export CUDA_PATH="${cuda_root}"
  export CUDA_BIN_PATH="${cuda_root}/bin"
  export CUDACXX="${cuda_nvcc}"
else
  echo "[cmake] CUDA toolkit is required for naim-node builds; none was found" >&2
  echo "[cmake] checked CUDA_TOOLKIT_ROOT_DIR, CUDA_HOME, CUDA_PATH, /usr/local/cuda*, and nvcc on PATH" >&2
  exit 1
fi

if detect_macos_openmp; then
  :
elif [[ "${TARGET_OS}" == "macos" ]]; then
  echo "[cmake] OpenMP runtime was not found for macOS; building without OpenMP (install with 'brew install libomp' to enable it)" >&2
fi

vcpkg_root="$("${script_dir}/find-vcpkg.sh" --root)"
vcpkg_toolchain="${vcpkg_root}/scripts/buildsystems/vcpkg.cmake"
ninja_exe="$("${script_dir}/find-ninja.sh")"
cmake_exe="$("${script_dir}/find-cmake.sh")"
extra_cmake_args=()

if [[ -n "${NAIM_CMAKE_ARGS:-}" ]]; then
  # NAIM_CMAKE_ARGS uses shell-style tokenization so callers can pass multiple -D flags.
  # shellcheck disable=SC2206
  extra_cmake_args=( ${NAIM_CMAKE_ARGS} )
fi

if [[ ! -f "${vcpkg_toolchain}" ]]; then
  echo "error: vcpkg toolchain file was not found under '${vcpkg_root}'" >&2
  exit 1
fi

mkdir -p "${BUILD_DIR}"
cache_path="${BUILD_DIR}/CMakeCache.txt"
needs_clean_reconfigure=0

if [[ -f "${cache_path}" ]]; then
  if ! grep -Fq "CMAKE_BUILD_TYPE:STRING=${BUILD_TYPE}" "${cache_path}"; then
    needs_clean_reconfigure=1
  fi
  if ! grep -Fq "VCPKG_TARGET_TRIPLET:UNINITIALIZED=${VCPKG_TRIPLET}" "${cache_path}" \
    && ! grep -Fq "VCPKG_TARGET_TRIPLET:STRING=${VCPKG_TRIPLET}" "${cache_path}"; then
    needs_clean_reconfigure=1
  fi
  if ! grep -Fq "CMAKE_TOOLCHAIN_FILE:FILEPATH=${vcpkg_toolchain}" "${cache_path}" \
    && ! grep -Fq "CMAKE_TOOLCHAIN_FILE:PATH=${vcpkg_toolchain}" "${cache_path}" \
    && ! grep -Fq "CMAKE_TOOLCHAIN_FILE:STRING=${vcpkg_toolchain}" "${cache_path}" \
    && ! grep -Fq "CMAKE_TOOLCHAIN_FILE:UNINITIALIZED=${vcpkg_toolchain}" "${cache_path}"; then
    needs_clean_reconfigure=1
  fi
  if [[ -n "${cuda_root}" ]]; then
    if ! grep -Fq "CUDAToolkit_ROOT:UNINITIALIZED=${cuda_root}" "${cache_path}" \
      && ! grep -Fq "CUDAToolkit_ROOT:PATH=${cuda_root}" "${cache_path}" \
      && ! grep -Fq "CUDAToolkit_ROOT:STRING=${cuda_root}" "${cache_path}"; then
      needs_clean_reconfigure=1
    fi
    if ! grep -Fq "CMAKE_CUDA_COMPILER:FILEPATH=${cuda_nvcc}" "${cache_path}" \
      && ! grep -Fq "CMAKE_CUDA_COMPILER:UNINITIALIZED=${cuda_nvcc}" "${cache_path}" \
      && ! grep -Fq "CMAKE_CUDA_COMPILER:STRING=${cuda_nvcc}" "${cache_path}"; then
      needs_clean_reconfigure=1
    fi
  else
    if grep -Eq '^CUDAToolkit_ROOT:(UNINITIALIZED|PATH|STRING)=' "${cache_path}" \
      || grep -Eq '^CMAKE_CUDA_COMPILER:(FILEPATH|UNINITIALIZED|STRING)=' "${cache_path}" \
      || grep -Fq 'GGML_CUDA:BOOL=ON' "${cache_path}"; then
      needs_clean_reconfigure=1
    fi
  fi
  if [[ -n "${openmp_root}" ]]; then
    if ! grep -Fq "OpenMP_ROOT:UNINITIALIZED=${openmp_root}" "${cache_path}" \
      && ! grep -Fq "OpenMP_ROOT:PATH=${openmp_root}" "${cache_path}" \
      && ! grep -Fq "OpenMP_ROOT:STRING=${openmp_root}" "${cache_path}"; then
      needs_clean_reconfigure=1
    fi
    if ! grep -Fq "OpenMP_C_INCLUDE_DIR:PATH=${openmp_include_dir}" "${cache_path}" \
      && ! grep -Fq "OpenMP_C_INCLUDE_DIR:STRING=${openmp_include_dir}" "${cache_path}" \
      && ! grep -Fq "OpenMP_C_INCLUDE_DIR:UNINITIALIZED=${openmp_include_dir}" "${cache_path}"; then
      needs_clean_reconfigure=1
    fi
    if ! grep -Fq "OpenMP_CXX_INCLUDE_DIR:PATH=${openmp_include_dir}" "${cache_path}" \
      && ! grep -Fq "OpenMP_CXX_INCLUDE_DIR:STRING=${openmp_include_dir}" "${cache_path}" \
      && ! grep -Fq "OpenMP_CXX_INCLUDE_DIR:UNINITIALIZED=${openmp_include_dir}" "${cache_path}"; then
      needs_clean_reconfigure=1
    fi
    if ! grep -Fq "OpenMP_libomp_LIBRARY:FILEPATH=${openmp_library}" "${cache_path}" \
      && ! grep -Fq "OpenMP_libomp_LIBRARY:PATH=${openmp_library}" "${cache_path}" \
      && ! grep -Fq "OpenMP_libomp_LIBRARY:STRING=${openmp_library}" "${cache_path}" \
      && ! grep -Fq "OpenMP_libomp_LIBRARY:UNINITIALIZED=${openmp_library}" "${cache_path}"; then
      needs_clean_reconfigure=1
    fi
  fi
  if ! grep -Fq "CMAKE_GENERATOR:INTERNAL=Ninja" "${cache_path}"; then
    needs_clean_reconfigure=1
  fi
  if ! grep -Fq "CMAKE_MAKE_PROGRAM:FILEPATH=${ninja_exe}" "${cache_path}" \
    && ! grep -Fq "CMAKE_MAKE_PROGRAM:UNINITIALIZED=${ninja_exe}" "${cache_path}" \
    && ! grep -Fq "CMAKE_MAKE_PROGRAM:STRING=${ninja_exe}" "${cache_path}"; then
    needs_clean_reconfigure=1
  fi
fi

if [[ "${needs_clean_reconfigure}" == "1" ]]; then
  echo "[cmake] cache settings changed; recreating ${BUILD_DIR}" >&2
  "${cmake_exe}" -E rm -f "${cache_path}"
  "${cmake_exe}" -E remove_directory "${BUILD_DIR}/CMakeFiles"
fi

cmake_args=(
  -S "${REPO_DIR}"
  -B "${BUILD_DIR}"
  -G Ninja
  -DCMAKE_BUILD_TYPE="${BUILD_TYPE}"
  -DCMAKE_MAKE_PROGRAM="${ninja_exe}"
  -DCMAKE_TOOLCHAIN_FILE="${vcpkg_toolchain}"
  -DVCPKG_TARGET_TRIPLET="${VCPKG_TRIPLET}"
)

cmake_args+=("-DCUDAToolkit_ROOT=${cuda_root}")
cmake_args+=("-DCMAKE_CUDA_COMPILER=${cuda_nvcc}")
if [[ -n "${openmp_root}" ]]; then
  cmake_args+=(
    "-DOpenMP_ROOT=${openmp_root}"
    "-DOpenMP_C_INCLUDE_DIR=${openmp_include_dir}"
    "-DOpenMP_CXX_INCLUDE_DIR=${openmp_include_dir}"
    "-DOpenMP_libomp_LIBRARY=${openmp_library}"
  )
fi
if [[ -n "${NAIM_CMAKE_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  extra_cmake_args=(${NAIM_CMAKE_ARGS})
  cmake_args+=("${extra_cmake_args[@]}")
fi

"${cmake_exe}" "${cmake_args[@]}"

echo "${BUILD_DIR}"
