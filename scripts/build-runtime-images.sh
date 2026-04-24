#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

skip_web_ui="no"
if [[ "${1:-}" == "--skip-web-ui" ]]; then
  skip_web_ui="yes"
  shift
fi

declare -a docker_cmd
docker_probe_output=""

probe_docker() {
  local -a candidate=("$@")
  docker_probe_output="$("${candidate[@]}" version 2>&1)" && return 0
  return 1
}

print_docker_probe_error() {
  local label="$1"
  local output="$2"

  if [[ -z "${output}" ]]; then
    return
  fi

  echo "  ${label}:" >&2
  printf '%s\n' "${output}" | sed 's/^/    /' >&2
}

print_docker_socket_diagnostics() {
  if [[ ! -S /var/run/docker.sock ]]; then
    return
  fi

  echo "  docker socket:" >&2
  ls -ln /var/run/docker.sock 2>/dev/null | sed 's/^/    /' >&2 || true
  id 2>/dev/null | sed 's/^/    current user: /' >&2 || true
}

resolve_docker() {
  local docker_error="docker was not found on PATH"
  local sudo_error="sudo is unavailable or passwordless sudo is not configured"
  local windows_error=""
  local windows_docker="/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"

  if command -v docker >/dev/null 2>&1; then
    if probe_docker docker; then
      docker_cmd=(docker)
      return 0
    fi
    docker_error="${docker_probe_output}"
  fi

  if command -v sudo >/dev/null 2>&1; then
    if probe_docker sudo -n docker; then
      docker_cmd=(sudo -n docker)
      return 0
    fi
    sudo_error="${docker_probe_output}"
  fi

  if [[ -x "${windows_docker}" ]]; then
    if probe_docker "${windows_docker}"; then
      docker_cmd=("${windows_docker}")
      return 0
    fi
    windows_error="${docker_probe_output}"
  else
    windows_error="${windows_docker} was not found or is not executable"
  fi

  echo "error: no usable Docker API connection is available" >&2
  echo "checked: docker, sudo -n docker, and ${windows_docker}" >&2
  print_docker_probe_error "docker" "${docker_error}"
  print_docker_probe_error "sudo -n docker" "${sudo_error}"
  print_docker_probe_error "${windows_docker}" "${windows_error}"
  print_docker_socket_diagnostics
  cat >&2 <<'EOF'

For WSL + Docker Desktop, make sure Docker Desktop is running and WSL integration
is enabled for this distribution. Then verify that this exact command works:

  docker version
EOF
  return 1
}

resolve_docker

base_tag="${1:-naim/base-runtime:dev}"
infer_tag="${2:-naim/infer-runtime:dev}"
worker_tag="${3:-naim/worker-runtime:dev}"
web_ui_tag="${4:-naim/web-ui:dev}"
skills_tag="${5:-naim/skills-runtime:dev}"
webgateway_tag="${6:-naim/webgateway-runtime:dev}"
interaction_tag="${7:-naim/interaction-runtime:dev}"
controller_tag="${8:-naim/controller:dev}"
hostd_tag="${9:-naim/hostd:dev}"
knowledge_tag="${10:-naim/knowledge-runtime:dev}"

build_dir="$("${script_dir}/print-build-dir.sh")"
turboquant_build_dir="${NAIM_TURBOQUANT_BUILD_DIR:-${repo_root}/build-turboquant/linux/x64}"
image_work_root="${NAIM_RUNTIME_IMAGE_CONTEXT_ROOT:-${build_dir}/image-contexts}"
mkdir -p "${image_work_root}"
image_context="$(mktemp -d "${image_work_root}/runtime-image-context.XXXXXX")"
cleanup_image_context() {
  rm -rf "${image_context}" 2>/dev/null || sudo -n rm -rf "${image_context}" 2>/dev/null || true
}
trap cleanup_image_context EXIT

mkdir -p "${image_context}/build/linux/x64/bin"
mkdir -p "${image_context}/build-turboquant/linux/x64/bin"
cp -R "${repo_root}/runtime" "${image_context}/runtime"
for binary in naim-controller naim-hostd naim-node naim-inferctl naim-workerd naim-skillsd naim-knowledged naim-webgatewayd naim-interactiond; do
  cp "${build_dir}/${binary}" "${image_context}/build/linux/x64/${binary}"
done
cp "${build_dir}/bin/llama-server" "${image_context}/build/linux/x64/bin/llama-server"
cp "${build_dir}/bin/rpc-server" "${image_context}/build/linux/x64/bin/rpc-server"
copy_turboquant_binary() {
  local binary="$1"
  local source_path="${turboquant_build_dir}/bin/${binary}"
  local target_path="${image_context}/build-turboquant/linux/x64/bin/${binary}"

  if [[ -x "${source_path}" ]]; then
    cp "${source_path}" "${target_path}"
    return
  fi

  if [[ "${NAIM_REQUIRE_TURBOQUANT:-0}" == "1" ]]; then
    echo "error: required TurboQuant binary was not found: ${source_path}" >&2
    echo "run scripts/build-turboquant-runtime.sh first, or unset NAIM_REQUIRE_TURBOQUANT for local images" >&2
    exit 1
  fi

  echo "warning: TurboQuant binary not found; adding unsupported-runtime placeholder for ${binary}" >&2
  cat > "${target_path}" <<EOF
#!/usr/bin/env sh
echo "TurboQuant runtime is not included in this local image. Run scripts/build-turboquant-runtime.sh and rebuild images, or use the default runtime flavor." >&2
exit 127
EOF
  chmod +x "${target_path}"
}

copy_turboquant_binary llama-server
copy_turboquant_binary rpc-server

build_web_ui_image() {
  local temp_root
  mkdir -p "${image_work_root}"
  temp_root="$(mktemp -d "${image_work_root}/web-ui-image.XXXXXX")"
  cleanup_web_ui_image() {
    rm -rf "${temp_root}" 2>/dev/null || sudo -n rm -rf "${temp_root}" 2>/dev/null || true
  }
  trap cleanup_web_ui_image RETURN
  mkdir -p "${temp_root}/dist"
  cp "${repo_root}/runtime/web-ui/Caddyfile" "${temp_root}/Caddyfile"

  local -a helper_args=(
    run
    --rm
    -v "${repo_root}/ui/operator-react:/src:ro"
    -v "${temp_root}/dist:/out"
    -w /tmp/work
    node:20-bookworm-slim
    bash
    -lc
    "cp /src/package.json /src/package-lock.json . && npm ci && cp -R /src/. . && npm run build && cp -R dist/. /out/"
  )
  if [[ "$(uname -s)" == "Linux" ]]; then
    helper_args=(run --rm --security-opt apparmor=unconfined "${helper_args[@]:2}")
  fi

  "${docker_cmd[@]}" "${helper_args[@]}"

  cat > "${temp_root}/Dockerfile" <<'EOF'
FROM caddy:2.10.2-alpine

COPY Caddyfile /etc/caddy/Caddyfile
COPY dist/ /srv/

EXPOSE 8080
EOF

  "${docker_cmd[@]}" build -f "${temp_root}/Dockerfile" -t "${web_ui_tag}" "${temp_root}"
}

echo "building ${base_tag}"
"${docker_cmd[@]}" build \
  -f "${image_context}/runtime/base/Dockerfile" \
  -t "${base_tag}" \
  "${image_context}"

echo "building ${controller_tag}"
"${docker_cmd[@]}" build \
  -f "${image_context}/runtime/controller/Dockerfile" \
  -t "${controller_tag}" \
  "${image_context}"

echo "building ${hostd_tag}"
"${docker_cmd[@]}" build \
  -f "${image_context}/runtime/hostd/Dockerfile" \
  -t "${hostd_tag}" \
  "${image_context}"

echo "building ${infer_tag}"
"${docker_cmd[@]}" build \
  -f "${image_context}/runtime/infer/Dockerfile" \
  --build-arg "BASE_IMAGE=${base_tag}" \
  -t "${infer_tag}" \
  "${image_context}"

echo "building ${worker_tag}"
"${docker_cmd[@]}" build \
  -f "${image_context}/runtime/worker/Dockerfile" \
  --build-arg "BASE_IMAGE=${base_tag}" \
  -t "${worker_tag}" \
  "${image_context}"

echo "building ${skills_tag}"
"${docker_cmd[@]}" build \
  -f "${image_context}/runtime/skills/Dockerfile" \
  --build-arg "BASE_IMAGE=${base_tag}" \
  -t "${skills_tag}" \
  "${image_context}"

echo "building ${knowledge_tag}"
"${docker_cmd[@]}" build \
  -f "${image_context}/runtime/knowledge/Dockerfile" \
  --build-arg "BASE_IMAGE=${base_tag}" \
  -t "${knowledge_tag}" \
  "${image_context}"

echo "building ${webgateway_tag}"
"${docker_cmd[@]}" build \
  -f "${image_context}/runtime/browsing/Dockerfile" \
  --build-arg "BASE_IMAGE=${base_tag}" \
  -t "${webgateway_tag}" \
  "${image_context}"

echo "building ${interaction_tag}"
"${docker_cmd[@]}" build \
  -f "${image_context}/runtime/interaction/Dockerfile" \
  --build-arg "BASE_IMAGE=${base_tag}" \
  -t "${interaction_tag}" \
  "${image_context}"

if [[ "${skip_web_ui}" != "yes" ]]; then
  echo "building ${web_ui_tag}"
  build_web_ui_image
fi

echo "runtime images ready"
echo "  base=${base_tag}"
echo "  turboquant_build=${turboquant_build_dir}"
echo "  controller=${controller_tag}"
echo "  hostd=${hostd_tag}"
echo "  infer=${infer_tag}"
echo "  worker=${worker_tag}"
echo "  skills=${skills_tag}"
echo "  knowledge=${knowledge_tag}"
echo "  webgateway=${webgateway_tag}"
echo "  interaction=${interaction_tag}"
if [[ "${skip_web_ui}" != "yes" ]]; then
  echo "  web_ui=${web_ui_tag}"
fi
