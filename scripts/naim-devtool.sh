#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

bin_path="${NAIM_DEVTOOL_BIN:-}"
if [[ -z "${bin_path}" ]]; then
  candidate="$("${repo_root}/scripts/print-build-dir.sh")/naim-devtool"
  if [[ -x "${candidate}" ]]; then
    bin_path="${candidate}"
  fi
fi

if [[ -z "${bin_path}" || ! -x "${bin_path}" ]]; then
  echo "error: naim-devtool binary not found; build the project or set NAIM_DEVTOOL_BIN" >&2
  exit 1
fi

exec "${bin_path}" "$@"
