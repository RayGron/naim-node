#!/usr/bin/env bash
set -euo pipefail

triplet="${1:-}"
if [[ -z "${triplet}" ]]; then
  echo "usage: ensure-vcpkg-deps.sh <triplet>" >&2
  exit 1
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd -- "${script_dir}/.." && pwd)"
vcpkg_root="$("${script_dir}/find-vcpkg.sh" --root)"
vcpkg_exe="${vcpkg_root}/vcpkg"
install_root="${repo_dir}/vcpkg_installed"
stamp_dir="${install_root}/vcpkg"
stamp_path="${stamp_dir}/.comet-${triplet}.manifest.sha256"
cuda_root=""
cuda_nvcc=""

if [[ ! -x "${vcpkg_exe}" ]]; then
  echo "error: vcpkg executable is missing at '${vcpkg_exe}'" >&2
  exit 1
fi

if [[ ! -f "${vcpkg_root}/scripts/buildsystems/vcpkg.cmake" ]]; then
  echo "error: vcpkg toolchain file was not found under '${vcpkg_root}'" >&2
  exit 1
fi

hash_cmd=()
if command -v sha256sum >/dev/null 2>&1; then
  hash_cmd=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
  hash_cmd=(shasum -a 256)
else
  echo "error: unable to find sha256sum or shasum for vcpkg manifest fingerprinting" >&2
  exit 1
fi

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

if detect_cuda_root; then
  export CUDA_TOOLKIT_ROOT_DIR="${cuda_root}"
  export CUDA_HOME="${cuda_root}"
  export CUDA_PATH="${cuda_root}"
  export CUDA_BIN_PATH="${cuda_root}/bin"
  export CUDACXX="${cuda_nvcc}"
fi

manifest_input="$(
  {
    printf 'triplet=%s\n' "${triplet}"
    printf 'vcpkg_root=%s\n' "${vcpkg_root}"
    printf 'vcpkg_exe=%s\n' "${vcpkg_exe}"
    printf 'cuda_root=%s\n' "${cuda_root}"
    printf 'cuda_nvcc=%s\n' "${cuda_nvcc}"
    cat "${repo_dir}/vcpkg.json"
    if [[ -f "${repo_dir}/vcpkg-configuration.json" ]]; then
      printf '\n-- vcpkg-configuration.json --\n'
      cat "${repo_dir}/vcpkg-configuration.json"
    fi
  } | "${hash_cmd[@]}" | awk '{print $1}'
)"

if [[ -f "${stamp_path}" && -d "${install_root}/${triplet}" ]]; then
  recorded_manifest="$(<"${stamp_path}")"
  if [[ "${recorded_manifest}" == "${manifest_input}" ]]; then
    echo "[vcpkg] root=${vcpkg_root}"
    echo "[vcpkg] triplet=${triplet}"
    echo "[vcpkg] install_root=${install_root}"
    echo "[vcpkg] manifest unchanged; skipping install"
    exit 0
  fi
fi

echo "[vcpkg] root=${vcpkg_root}"
echo "[vcpkg] triplet=${triplet}"
echo "[vcpkg] install_root=${install_root}"
if [[ -n "${cuda_root}" ]]; then
  echo "[vcpkg] cuda_root=${cuda_root}"
  echo "[vcpkg] cuda_nvcc=${cuda_nvcc}"
fi
"${vcpkg_exe}" install \
  --x-manifest-root="${repo_dir}" \
  --x-install-root="${install_root}" \
  --x-wait-for-lock \
  --triplet="${triplet}"

mkdir -p "${stamp_dir}"
printf '%s\n' "${manifest_input}" > "${stamp_path}"
