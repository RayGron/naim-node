#!/usr/bin/env bash
set -euo pipefail

db_path="${COMET_SKILLS_DB_PATH:-/comet/private/skills.sqlite}"
status_path="${COMET_SKILLS_RUNTIME_STATUS_PATH:-/comet/private/skills-runtime-status.json}"

mkdir -p "$(dirname "${db_path}")" "$(dirname "${status_path}")" /tmp

echo "[comet-skills] booting plane=${COMET_PLANE_NAME:-unknown} instance=${COMET_INSTANCE_NAME:-unknown}"
echo "[comet-skills] db_path=${db_path}"
echo "[comet-skills] status_path=${status_path}"
echo "[comet-skills] port=${COMET_SKILLS_PORT:-18120}"

exec python3 /runtime/skills/skillsd.py

