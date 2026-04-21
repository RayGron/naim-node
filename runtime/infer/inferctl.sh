#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/../.." && pwd)"

bin_path="${NAIM_INFERCTL_BIN:-}"
if [[ -z "${bin_path}" ]]; then
  if [[ -x /runtime/bin/naim-inferctl ]]; then
    bin_path="/runtime/bin/naim-inferctl"
  else
    candidate="$("${repo_root}/scripts/print-build-dir.sh")/naim-inferctl"
    if [[ -x "${candidate}" ]]; then
      bin_path="${candidate}"
    fi
  fi
fi

if [[ -z "${bin_path}" || ! -x "${bin_path}" ]]; then
  echo "error: naim-inferctl binary not found; build the project or set NAIM_INFERCTL_BIN" >&2
  exit 1
fi

export NAIM_INFER_PROFILES_PATH="${NAIM_INFER_PROFILES_PATH:-${script_dir}/runtime-profiles.json}"
exec "${bin_path}" "$@"
