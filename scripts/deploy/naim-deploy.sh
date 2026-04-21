#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

inventory_path=""
skip_pull="no"
deploy_control="yes"
deploy_workers="yes"
declare -a selected_workers=()

usage() {
  cat <<'EOF'
Usage:
  naim-deploy.sh --inventory <path> [--worker <name> ...] [--skip-pull]
  naim-deploy.sh [--control-only|--workers-only] [--skip-pull]

Deploys the NAIM control plane and worker hostd containers. With an inventory,
all workers are deployed by default; repeat --worker to deploy a subset.
Without an inventory, the current environment variables describe a single
control host and a single worker host.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --inventory)
      inventory_path="${2:-}"
      if [[ -z "${inventory_path}" ]]; then
        echo "error: --inventory requires a path" >&2
        exit 1
      fi
      shift 2
      ;;
    --worker)
      next_worker="${2:-}"
      if [[ -z "${next_worker}" ]]; then
        echo "error: --worker requires a name" >&2
        exit 1
      fi
      selected_workers+=("${next_worker}")
      shift 2
      ;;
    --skip-pull)
      skip_pull="yes"
      shift
      ;;
    --control-only)
      deploy_workers="no"
      shift
      ;;
    --workers-only)
      deploy_control="no"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument '$1'" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${deploy_control}" == "yes" ]]; then
  control_cmd=("${script_dir}/deploy-control-plane.sh")
  if [[ -n "${inventory_path}" ]]; then
    control_cmd+=(--inventory "${inventory_path}")
  fi
  if [[ "${skip_pull}" == "yes" ]]; then
    control_cmd+=(--skip-pull)
  fi
  "${control_cmd[@]}"
fi

if [[ "${deploy_workers}" != "yes" ]]; then
  exit 0
fi

if [[ -n "${inventory_path}" && ${#selected_workers[@]} -eq 0 ]]; then
  while IFS= read -r worker; do
    [[ -n "${worker}" ]] && selected_workers+=("${worker}")
  done < <("${script_dir}/naim-inventory.py" workers --inventory "${inventory_path}")
fi

if [[ ${#selected_workers[@]} -eq 0 ]]; then
  worker_cmd=("${script_dir}/deploy-worker-hostd.sh")
  if [[ -n "${inventory_path}" ]]; then
    worker_cmd+=(--inventory "${inventory_path}")
  fi
  if [[ "${skip_pull}" == "yes" ]]; then
    worker_cmd+=(--skip-pull)
  fi
  "${worker_cmd[@]}"
else
  for worker in "${selected_workers[@]}"; do
    worker_cmd=("${script_dir}/deploy-worker-hostd.sh")
    if [[ -n "${inventory_path}" ]]; then
      worker_cmd+=(--inventory "${inventory_path}")
    fi
    worker_cmd+=(--worker "${worker}")
    if [[ "${skip_pull}" == "yes" ]]; then
      worker_cmd+=(--skip-pull)
    fi
    "${worker_cmd[@]}"
  done
fi
