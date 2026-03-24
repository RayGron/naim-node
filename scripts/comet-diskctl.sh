#!/usr/bin/env bash
set -euo pipefail

command="${1:-}"
path="${2:-}"

case "${command}" in
  ensure-dir)
    mkdir -p "${path}"
    ;;
  cleanup-dir)
    rmdir "${path}" 2>/dev/null || true
    ;;
  *)
    echo "usage: comet-diskctl.sh <ensure-dir|cleanup-dir> <path>" >&2
    exit 1
    ;;
esac
