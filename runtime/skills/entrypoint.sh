#!/usr/bin/env bash
set -euo pipefail

db_path="${NAIM_SKILLS_DB_PATH:-/naim/private/skills.sqlite}"
status_path="${NAIM_SKILLS_RUNTIME_STATUS_PATH:-/naim/private/skills-runtime-status.json}"

mkdir -p "$(dirname "${db_path}")" "$(dirname "${status_path}")" /tmp

echo "[naim-skills] booting plane=${NAIM_PLANE_NAME:-unknown} instance=${NAIM_INSTANCE_NAME:-unknown}"
echo "[naim-skills] db_path=${db_path}"
echo "[naim-skills] status_path=${status_path}"
echo "[naim-skills] port=${NAIM_SKILLS_PORT:-18120}"

exec /runtime/bin/naim-skillsd
