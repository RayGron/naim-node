#!/usr/bin/env bash
set -euo pipefail

mkdir -p /tmp
config_path="${NAIM_INFER_RUNTIME_CONFIG:-${NAIM_CONTROL_ROOT:-/naim/shared/control/${NAIM_PLANE_NAME:-unknown}}/infer-runtime.json}"
boot_mode="${NAIM_INFER_BOOT_MODE:-launch-runtime}"
echo "[naim-infer] booting plane=${NAIM_PLANE_NAME:-unknown} instance=${NAIM_INSTANCE_NAME:-unknown}"
echo "[naim-infer] control_root=${NAIM_CONTROL_ROOT:-unknown}"
echo "[naim-infer] runtime_config=${config_path}"
echo "[naim-infer] boot_mode=${boot_mode}"

run_startup_doctor() {
  local checks="${NAIM_INFER_STARTUP_DOCTOR_CHECKS:-config,filesystem,tools,gateway}"
  /runtime/infer/inferctl.sh doctor --config "${config_path}" --checks "${checks}"
}

probe_topology_doctor() {
  if /runtime/infer/inferctl.sh doctor --config "${config_path}" --checks topology; then
    return 0
  fi
  echo "[naim-infer] worker group topology is still bootstrapping; continuing startup"
  return 0
}

if [[ -f "${config_path}" ]]; then
  echo "[naim-infer] runtime config found"
  if [[ -x /runtime/bin/naim-inferctl || -x /runtime/infer/inferctl.sh ]]; then
    case "${boot_mode}" in
      prepare-only)
        /runtime/infer/inferctl.sh bootstrap-runtime --config "${config_path}" --apply
        /runtime/infer/inferctl.sh doctor --config "${config_path}"
        /runtime/infer/inferctl.sh plan-launch --config "${config_path}"
        /runtime/infer/inferctl.sh status --config "${config_path}" --apply
        ;;
      validate-only)
        /runtime/infer/inferctl.sh validate-config --config "${config_path}"
        /runtime/infer/inferctl.sh doctor --config "${config_path}"
        /runtime/infer/inferctl.sh plan-launch --config "${config_path}"
        ;;
      launch-embedded)
        /runtime/infer/inferctl.sh bootstrap-runtime --config "${config_path}" --apply
        run_startup_doctor
        probe_topology_doctor
        /runtime/infer/inferctl.sh gateway-plan --config "${config_path}" --apply
        /runtime/infer/inferctl.sh plan-launch --config "${config_path}"
        /runtime/infer/inferctl.sh status --config "${config_path}" --apply
        exec /runtime/infer/inferctl.sh launch-embedded-runtime --config "${config_path}"
        ;;
      launch-runtime)
        /runtime/infer/inferctl.sh bootstrap-runtime --config "${config_path}" --apply
        run_startup_doctor
        probe_topology_doctor
        /runtime/infer/inferctl.sh gateway-plan --config "${config_path}" --apply
        /runtime/infer/inferctl.sh plan-launch --config "${config_path}"
        /runtime/infer/inferctl.sh status --config "${config_path}" --apply
        exec /runtime/infer/inferctl.sh launch-runtime --config "${config_path}" --backend "${NAIM_INFER_RUNTIME_BACKEND:-auto}"
        ;;
      idle)
        echo "[naim-infer] skipping inferctl bootstrap"
        ;;
      *)
        echo "[naim-infer] unsupported boot mode: ${boot_mode}" >&2
        exit 1
        ;;
    esac
  else
    echo "[naim-infer] inferctl helper not found"
  fi
else
  echo "[naim-infer] runtime config not found yet"
fi
touch /tmp/naim-ready
exec sleep infinity
