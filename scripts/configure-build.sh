#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: configure-build.sh <os> <arch> [build-type]" >&2
  exit 1
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd -- "${script_dir}/.." && pwd)"
target_os="${1}"
target_arch="${2}"
build_type="${3:-Debug}"
cuda_root=""
cuda_nvcc=""
openmp_root=""
openmp_include_dir=""
openmp_library=""

case "${build_type}" in
  Debug|Release|RelWithDebInfo|MinSizeRel)
    ;;
  *)
    echo "error: unsupported build type '${build_type}'" >&2
    echo "supported values: Debug, Release, RelWithDebInfo, MinSizeRel" >&2
    exit 1
    ;;
esac

eval "$("${script_dir}/resolve-build-target.sh" "${target_os}" "${target_arch}")"

detect_cuda_root() {
  local candidate=""
  for candidate in \
    "${CUDA_TOOLKIT_ROOT_DIR:-}" \
    "${CUDA_HOME:-}" \
    "${CUDA_PATH:-}" \
    "/usr/local/cuda" \
    "/usr/local/cuda-13.2" \
    "/usr/local/cuda-13.1" \
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
    if [[ -n "${candidate}" && -x "${candidate}/bin/nvcc" ]]; then
      cuda_root="${candidate}"
      cuda_nvcc="${candidate}/bin/nvcc"
      return 0
    fi
  done

  if command -v nvcc >/dev/null 2>&1; then
    candidate="$(dirname -- "$(dirname -- "$(readlink -f "$(command -v nvcc)")")")"
    if [[ -x "${candidate}/bin/nvcc" ]]; then
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

  if [[ "${target_os}" != "macos" ]]; then
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
fi

if detect_macos_openmp; then
  :
elif [[ "${target_os}" == "macos" ]]; then
  echo "[cmake] OpenMP runtime was not found for macOS; building without OpenMP (install with 'brew install libomp' to enable it)" >&2
fi

build_dir="$("${script_dir}/print-build-dir.sh" "${target_os}" "${target_arch}")"
"${script_dir}/ensure-vcpkg-deps.sh" "${VCPKG_TRIPLET}"
vcpkg_installed_dir="${repo_dir}/vcpkg_installed"
ninja_exe="$("${script_dir}/find-ninja.sh")"
cmake_prefix_path="${vcpkg_installed_dir}/${VCPKG_TRIPLET}"

mkdir -p "${build_dir}"
cache_path="${build_dir}/CMakeCache.txt"
needs_clean_reconfigure=0

if [[ -f "${cache_path}" ]]; then
  if ! grep -Fq "CMAKE_BUILD_TYPE:STRING=${build_type}" "${cache_path}"; then
    needs_clean_reconfigure=1
  fi
  if ! grep -Fq "VCPKG_TARGET_TRIPLET:UNINITIALIZED=${VCPKG_TRIPLET}" "${cache_path}" \
    && ! grep -Fq "VCPKG_TARGET_TRIPLET:STRING=${VCPKG_TRIPLET}" "${cache_path}"; then
    needs_clean_reconfigure=1
  fi
  if ! grep -Fq "VCPKG_INSTALLED_DIR:UNINITIALIZED=${vcpkg_installed_dir}" "${cache_path}" \
    && ! grep -Fq "VCPKG_INSTALLED_DIR:PATH=${vcpkg_installed_dir}" "${cache_path}" \
    && ! grep -Fq "VCPKG_INSTALLED_DIR:STRING=${vcpkg_installed_dir}" "${cache_path}"; then
    needs_clean_reconfigure=1
  fi
  if ! grep -Fq "COMET_SKIP_VCPKG_TOOLCHAIN:BOOL=ON" "${cache_path}" \
    && ! grep -Fq "COMET_SKIP_VCPKG_TOOLCHAIN:UNINITIALIZED=ON" "${cache_path}" \
    && ! grep -Fq "COMET_SKIP_VCPKG_TOOLCHAIN:STRING=ON" "${cache_path}"; then
    needs_clean_reconfigure=1
  fi
  if ! grep -Fq "CMAKE_PREFIX_PATH:UNINITIALIZED=${cmake_prefix_path}" "${cache_path}" \
    && ! grep -Fq "CMAKE_PREFIX_PATH:PATH=${cmake_prefix_path}" "${cache_path}" \
    && ! grep -Fq "CMAKE_PREFIX_PATH:STRING=${cmake_prefix_path}" "${cache_path}"; then
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
  echo "[cmake] cache settings changed; recreating ${build_dir}" >&2
  cmake -E rm -f "${cache_path}"
  cmake -E remove_directory "${build_dir}/CMakeFiles"
fi

cmake_args=(
  -S "${repo_dir}"
  -B "${build_dir}"
  -G Ninja
  -DCMAKE_BUILD_TYPE="${build_type}"
  -DCMAKE_MAKE_PROGRAM="${ninja_exe}"
  -DCMAKE_PREFIX_PATH="${cmake_prefix_path}"
  -DCOMET_SKIP_VCPKG_TOOLCHAIN=ON
  -DVCPKG_TARGET_TRIPLET="${VCPKG_TRIPLET}"
  -DVCPKG_INSTALLED_DIR="${vcpkg_installed_dir}"
)

if [[ -n "${cuda_root}" ]]; then
  cmake_args+=("-DCUDAToolkit_ROOT=${cuda_root}")
fi
if [[ -n "${cuda_nvcc}" ]]; then
  cmake_args+=("-DCMAKE_CUDA_COMPILER=${cuda_nvcc}")
fi
if [[ -n "${openmp_root}" ]]; then
  cmake_args+=(
    "-DOpenMP_ROOT=${openmp_root}"
    "-DOpenMP_C_INCLUDE_DIR=${openmp_include_dir}"
    "-DOpenMP_CXX_INCLUDE_DIR=${openmp_include_dir}"
    "-DOpenMP_libomp_LIBRARY=${openmp_library}"
  )
fi

cmake "${cmake_args[@]}"

echo "${build_dir}"
