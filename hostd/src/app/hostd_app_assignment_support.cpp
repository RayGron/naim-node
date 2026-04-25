#include "app/hostd_app_assignment_support.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <stdexcept>
#include <thread>

#include "naim/security/crypto_utils.h"

namespace naim::hostd {

HostdAppAssignmentSupport::HostdAppAssignmentSupport()
    : path_support_(),
      runtime_telemetry_support_(),
      local_state_path_support_(),
      local_state_repository_(local_state_path_support_),
      local_runtime_state_support_(
          path_support_,
          local_state_repository_,
          runtime_telemetry_support_),
      command_support_(),
      file_support_(),
      compose_runtime_support_(command_support_),
      disk_runtime_support_(command_support_, path_support_, file_support_),
      apply_plan_support_(
          command_support_,
          compose_runtime_support_,
          disk_runtime_support_,
          file_support_),
      post_deploy_support_(command_support_),
      reporting_support_(),
      model_library_transfer_support_(
          command_support_,
          file_support_,
          reporting_support_),
      bootstrap_model_support_factory_(
          path_support_,
          command_support_,
          file_support_,
          reporting_support_),
      bootstrap_model_support_(bootstrap_model_support_factory_.Create()),
      display_support_(path_support_),
      apply_support_(
          path_support_,
          display_support_,
          apply_plan_support_,
          disk_runtime_support_,
          post_deploy_support_,
          local_state_repository_,
          local_runtime_state_support_,
          bootstrap_model_support_),
      observation_support_() {}

naim::DesiredState HostdAppAssignmentSupport::RebaseStateForRuntimeRoot(
    naim::DesiredState state,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) const {
  return path_support_.RebaseStateForRuntimeRoot(std::move(state), storage_root, runtime_root);
}

nlohmann::json HostdAppAssignmentSupport::BuildAssignmentProgressPayload(
    const std::string& phase,
    const std::string& phase_label,
    const std::string& message,
    int progress_percent,
    const std::string& plane_name,
    const std::string& node_name) const {
  return reporting_support_.BuildAssignmentProgressPayload(
      phase,
      phase_label,
      message,
      progress_percent,
      plane_name,
      node_name);
}

void HostdAppAssignmentSupport::PublishAssignmentProgress(
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const nlohmann::json& progress) const {
  reporting_support_.PublishAssignmentProgress(backend, assignment_id, progress);
}

std::vector<std::string> HostdAppAssignmentSupport::ParseTaggedCsv(
    const std::string& tagged_message,
    const std::string& tag) const {
  return reporting_support_.ParseTaggedCsv(tagged_message, tag);
}

naim::HostObservation HostdAppAssignmentSupport::BuildObservedStateSnapshot(
    const std::string& node_name,
    const std::string& storage_root,
    const std::string& state_root,
    naim::HostObservationStatus status,
    const std::string& status_message,
    const std::optional<int>& assignment_id) const {
  return observation_support_.BuildObservedStateSnapshot(
      node_name,
      storage_root,
      state_root,
      status,
      status_message,
      assignment_id);
}

std::map<std::string, int> HostdAppAssignmentSupport::CaptureServiceHostPids(
    const std::vector<std::string>& service_names) const {
  return reporting_support_.CaptureServiceHostPids(service_names);
}

bool HostdAppAssignmentSupport::VerifyEvictionAssignment(
    const naim::DesiredState& desired_state,
    const std::string& node_name,
    const std::string& state_root,
    const std::string& tagged_message,
    const std::map<std::string, int>& expected_victim_host_pids) const {
  return reporting_support_.VerifyEvictionAssignment(
      desired_state,
      node_name,
      state_root,
      tagged_message,
      expected_victim_host_pids);
}

void HostdAppAssignmentSupport::ApplyDesiredNodeState(
    const naim::DesiredState& desired_node_state,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    ComposeMode compose_mode,
    const std::string& source_label,
    const std::optional<int>& desired_generation,
    const std::optional<int>& assignment_id,
    HostdBackend* backend) const {
  apply_support_.ApplyDesiredNodeState(
      desired_node_state,
      artifacts_root,
      storage_root,
      runtime_root,
      state_root,
      compose_mode,
      source_label,
      desired_generation,
      assignment_id,
      backend,
      [&](const std::string& phase,
          const std::string& title,
          const std::string& detail,
          int percent,
          const std::string& plane_name,
          const std::string& node_name) {
        PublishAssignmentProgress(
            backend,
            assignment_id,
            BuildAssignmentProgressPayload(
                phase,
                title,
                detail,
                percent,
                plane_name,
                node_name));
      });
}

void HostdAppAssignmentSupport::DownloadModelLibraryArtifacts(
    const nlohmann::json& payload,
    const std::string& node_name,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  const std::vector<std::string> source_urls =
      payload.value("source_urls", std::vector<std::string>{});
  const std::vector<std::string> target_paths =
      payload.value("target_paths", std::vector<std::string>{});
  if (source_urls.empty()) {
    throw std::runtime_error("model-library-download payload is missing source_urls");
  }
  if (source_urls.size() != target_paths.size()) {
    throw std::runtime_error("model-library-download payload source_urls/target_paths mismatch");
  }

  std::uintmax_t aggregate_prefix = 0;
  std::optional<std::uintmax_t> aggregate_total = std::uintmax_t{0};
  for (const auto& source_url : source_urls) {
    const auto size = model_library_transfer_support_.ProbeContentLength(source_url);
    if (!size.has_value()) {
      aggregate_total = std::nullopt;
      break;
    }
    *aggregate_total += *size;
  }

  for (std::size_t index = 0; index < source_urls.size(); ++index) {
    model_library_transfer_support_.DownloadFileWithProgress(
        source_urls[index],
        target_paths[index],
        backend,
        assignment_id,
        payload.value("job_id", std::string("model-library")),
        node_name,
        index,
        source_urls.size(),
        aggregate_prefix,
        aggregate_total);
    if (const auto size =
            model_library_transfer_support_.FileSizeIfExists(target_paths[index]);
        size.has_value()) {
      aggregate_prefix += *size;
    }
  }
}

void HostdAppAssignmentSupport::ReadModelArtifactChunk(
    const nlohmann::json& payload,
    const std::string& node_name,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (backend == nullptr || !assignment_id.has_value()) {
    throw std::runtime_error("model-artifact-read-chunk requires a controller assignment");
  }
  const std::string source_node_name = payload.value("source_node_name", node_name);
  if (source_node_name != node_name) {
    throw std::runtime_error("model-artifact-read-chunk source node mismatch");
  }
  const std::string source_path =
      model_artifact_request_support_.NormalizePathString(
          std::filesystem::path(payload.value("source_path", std::string{})));
  if (source_path.empty() || source_path.front() != '/') {
    throw std::runtime_error("model-artifact-read-chunk payload is missing source_path");
  }
  const std::uintmax_t offset =
      payload.contains("offset") && payload.at("offset").is_number_unsigned()
          ? payload.at("offset").get<std::uintmax_t>()
          : std::uintmax_t{0};
  std::uintmax_t max_bytes =
      payload.contains("max_bytes") && payload.at("max_bytes").is_number_unsigned()
          ? payload.at("max_bytes").get<std::uintmax_t>()
          : HostdModelArtifactRequestSupport::kMaxChunkBytes;
  if (max_bytes == 0 || max_bytes > HostdModelArtifactRequestSupport::kMaxChunkBytes) {
    max_bytes = HostdModelArtifactRequestSupport::kMaxChunkBytes;
  }

  std::ifstream input(source_path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open model artifact chunk source: " + source_path);
  }
  input.seekg(0, std::ios::end);
  const auto end_position = input.tellg();
  if (end_position < 0) {
    throw std::runtime_error("failed to determine model artifact size: " + source_path);
  }
  const std::uintmax_t total_size = static_cast<std::uintmax_t>(end_position);
  const bool offset_past_eof = offset >= total_size;
  std::uintmax_t bytes_to_read = 0;
  if (!offset_past_eof) {
    bytes_to_read = std::min(max_bytes, total_size - offset);
  }
  std::vector<unsigned char> bytes(static_cast<std::size_t>(bytes_to_read));
  if (bytes_to_read > 0) {
    input.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    input.read(
        reinterpret_cast<char*>(bytes.data()),
        static_cast<std::streamsize>(bytes.size()));
    const auto bytes_read = input.gcount();
    if (bytes_read < 0) {
      throw std::runtime_error("failed to read model artifact chunk: " + source_path);
    }
    bytes.resize(static_cast<std::size_t>(bytes_read));
  }
  const std::uintmax_t next_offset = offset + bytes.size();
  backend->UpdateHostAssignmentProgress(
      *assignment_id,
      nlohmann::json{
          {"phase", "chunk-ready"},
          {"title", "Model artifact chunk ready"},
          {"detail", "Storage node read a model artifact chunk."},
          {"percent", next_offset >= total_size ? 100 : 50},
          {"transfer_id", payload.value("transfer_id", std::string{})},
          {"requester_node_name", payload.value("requester_node_name", std::string{})},
          {"source_node_name", source_node_name},
          {"source_path", source_path},
          {"offset", offset},
          {"next_offset", next_offset},
          {"bytes_read", bytes.size()},
          {"bytes_total", total_size},
          {"eof", next_offset >= total_size},
          {"bytes_base64", naim::EncodeBytesBase64(bytes)}});
}

void HostdAppAssignmentSupport::BuildModelArtifactManifest(
    const nlohmann::json& payload,
    const std::string& node_name,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (backend == nullptr || !assignment_id.has_value()) {
    throw std::runtime_error("model-artifact-build-manifest requires a controller assignment");
  }
  const std::string source_node_name = payload.value("source_node_name", node_name);
  if (source_node_name != node_name) {
    throw std::runtime_error("model-artifact-build-manifest source node mismatch");
  }
  if (!payload.contains("source_paths") || !payload.at("source_paths").is_array()) {
    throw std::runtime_error("model-artifact-build-manifest payload is missing source_paths");
  }

  nlohmann::json roots = nlohmann::json::array();
  nlohmann::json files = nlohmann::json::array();
  std::uintmax_t total_size = 0;
  int root_index = 0;
  for (const auto& source_path_item : payload.at("source_paths")) {
    if (!source_path_item.is_string()) {
      continue;
    }
    const std::filesystem::path source_path(
        model_artifact_request_support_.NormalizePathString(
            std::filesystem::path(source_path_item.get<std::string>())));
    const std::string source_path_text = source_path.string();
    if (source_path_text.empty() || source_path_text.front() != '/') {
      throw std::runtime_error("model-artifact-build-manifest source path must be absolute");
    }

    std::error_code error;
    if (!std::filesystem::exists(source_path, error) || error) {
      throw std::runtime_error("model artifact manifest source does not exist: " + source_path_text);
    }

    const bool source_is_directory = std::filesystem::is_directory(source_path, error) && !error;
    roots.push_back(
        nlohmann::json{
            {"source_path", source_path_text},
            {"kind", source_is_directory ? "directory" : "file"},
            {"root_index", root_index},
        });

    auto append_file =
        [&](const std::filesystem::path& current_path, const std::filesystem::path& relative_path) {
          if (!model_artifact_request_support_.IsSafeRelativePath(relative_path)) {
            throw std::runtime_error(
                "model artifact manifest contains unsafe relative path: " +
                relative_path.generic_string());
          }
          std::error_code file_error;
          const auto size = std::filesystem::file_size(current_path, file_error);
          if (file_error) {
            throw std::runtime_error(
                "failed to read model artifact file size: " + current_path.string());
          }
          total_size += size;
          const std::string sha256 = naim::ComputeFileSha256Hex(current_path.string());
          files.push_back(
              nlohmann::json{
                  {"root_index", root_index},
                  {"source_path", model_artifact_request_support_.NormalizePathString(current_path)},
                  {"relative_path", relative_path.generic_string()},
                  {"size_bytes", size},
                  {"sha256", sha256},
              });
        };

    if (source_is_directory) {
      for (std::filesystem::recursive_directory_iterator iterator(
               source_path,
               std::filesystem::directory_options::skip_permission_denied,
               error);
           !error && iterator != std::filesystem::recursive_directory_iterator();
           iterator.increment(error)) {
        if (error) {
          break;
        }
        if (!iterator->is_regular_file(error)) {
          error.clear();
          continue;
        }
        error.clear();
        append_file(iterator->path(), iterator->path().lexically_relative(source_path));
      }
      if (error) {
        throw std::runtime_error(
            "failed to walk model artifact directory: " + source_path_text);
      }
    } else if (std::filesystem::is_regular_file(source_path, error) && !error) {
      append_file(source_path, source_path.filename());
    } else {
      throw std::runtime_error(
          "model artifact manifest source is neither file nor directory: " + source_path_text);
    }
    ++root_index;
  }

  if (files.empty()) {
    throw std::runtime_error("model artifact manifest has no files");
  }
  const std::string manifest_sha256 = naim::ComputeSha256Hex(
      model_artifact_request_support_.BuildManifestCanonicalText(files));

  backend->UpdateHostAssignmentProgress(
      *assignment_id,
      nlohmann::json{
          {"phase", "manifest-ready"},
          {"title", "Model artifact manifest ready"},
          {"detail", "Storage node built a model artifact file manifest."},
          {"percent", 100},
          {"transfer_id", payload.value("transfer_id", std::string{})},
          {"requester_node_name", payload.value("requester_node_name", std::string{})},
          {"source_node_name", source_node_name},
          {"roots", roots},
          {"files", files},
          {"file_count", files.size()},
          {"bytes_total", total_size},
          {"manifest_algorithm", "naim-model-manifest-v1"},
          {"manifest_sha256", manifest_sha256},
      });
}

void HostdAppAssignmentSupport::ApplyKnowledgeVaultService(
    const nlohmann::json& payload,
    const std::string& node_name,
    const std::string& storage_root,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (backend == nullptr || !assignment_id.has_value()) {
    throw std::runtime_error("knowledge-vault-apply requires a controller assignment");
  }
  const std::string service_id = payload.value("service_id", std::string("kv_default"));
  const std::string image = payload.value("image", std::string{});
  const int port = payload.value("endpoint_port", 18200);
  if (image.empty()) {
    throw std::runtime_error("knowledge-vault-apply payload is missing image");
  }
  if (port <= 0) {
    throw std::runtime_error("knowledge-vault-apply endpoint port is invalid");
  }

  const std::string container_name =
      container_name_support_.KnowledgeVaultContainerName(service_id);
  const std::string effective_storage_root =
      storage_root.empty() ? payload.value("storage_root", std::string{}) : storage_root;
  if (effective_storage_root.empty()) {
    throw std::runtime_error("knowledge-vault-apply storage root is empty");
  }
  const std::filesystem::path service_root =
      std::filesystem::path(effective_storage_root) / "knowledge-vault" /
      container_name_support_.KnowledgeVaultStorageSegment(service_id);
  std::filesystem::create_directories(service_root);

  backend->UpdateHostAssignmentProgress(
      *assignment_id,
      nlohmann::json{
          {"phase", "starting"},
          {"title", "Starting knowledge vault"},
          {"detail", "Hostd is preparing the knowledge vault container."},
          {"percent", 10},
          {"service_id", service_id},
          {"node_name", node_name},
          {"storage_root", service_root.string()}});

  const std::string docker = command_support_.ResolvedDockerCommand();
  const auto quote = [&](const std::string& value) {
    return command_support_.ShellQuote(value);
  };
  command_support_.RunCommandOk(
      docker + " rm -f " + quote(container_name) + " >/dev/null 2>&1 || true");
  if (!command_support_.RunCommandOk(docker + " pull " + quote(image))) {
    throw std::runtime_error("failed to pull knowledge vault image: " + image);
  }

  std::ostringstream run_command;
  run_command
      << docker << " run -d"
      << " --name " << quote(container_name)
      << " --restart unless-stopped"
      << " -p 0.0.0.0:" << port << ":" << port
      << " -v " << quote(service_root.string()) << ":/naim/knowledge"
      << " -e " << quote("NAIM_KNOWLEDGE_SERVICE_ID=" + service_id)
      << " -e " << quote("NAIM_NODE_NAME=" + node_name)
      << " -e " << quote("NAIM_KNOWLEDGE_LISTEN_HOST=0.0.0.0")
      << " -e " << quote("NAIM_KNOWLEDGE_PORT=" + std::to_string(port))
      << " -e " << quote("NAIM_KNOWLEDGE_STORE_PATH=/naim/knowledge/store")
      << " -e " << quote("NAIM_KNOWLEDGE_STATUS_PATH=/naim/knowledge/runtime-status.json")
      << " " << quote(image);
  if (!command_support_.RunCommandOk(run_command.str())) {
    throw std::runtime_error("failed to start knowledge vault container: " + container_name);
  }

  HostdRuntimeHttpResponse health;
  bool ready = false;
  for (int attempt = 0; attempt < 40; ++attempt) {
    try {
      health = runtime_http_proxy_.Send(
          "127.0.0.1",
          port,
          "GET",
          "/health",
          "",
          {},
          HostdRuntimeProxyPolicy::Runtime);
      if (health.status_code >= 200 && health.status_code < 300) {
        ready = true;
        break;
      }
    } catch (const std::exception&) {
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  if (!ready) {
    throw std::runtime_error("knowledge vault container did not become healthy");
  }

  backend->UpdateHostAssignmentProgress(
      *assignment_id,
      nlohmann::json{
          {"phase", "completed"},
          {"title", "Knowledge vault ready"},
          {"detail", "Knowledge vault container is running on this storage node."},
          {"percent", 100},
          {"service_id", service_id},
          {"node_name", node_name},
          {"container_name", container_name},
          {"endpoint_port", port},
          {"health_status_code", health.status_code}});
}

void HostdAppAssignmentSupport::StopKnowledgeVaultService(
    const nlohmann::json& payload,
    const std::string& node_name,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (backend == nullptr || !assignment_id.has_value()) {
    throw std::runtime_error("knowledge-vault-stop requires a controller assignment");
  }
  const std::string service_id = payload.value("service_id", std::string("kv_default"));
  const std::string container_name =
      container_name_support_.KnowledgeVaultContainerName(service_id);
  const std::string docker = command_support_.ResolvedDockerCommand();
  const std::string command = docker + " rm -f " +
                              command_support_.ShellQuote(container_name) +
                              " >/dev/null 2>&1 || true";
  if (!command_support_.RunCommandOk(command)) {
    throw std::runtime_error("failed to stop knowledge vault container: " + container_name);
  }
  backend->UpdateHostAssignmentProgress(
      *assignment_id,
      nlohmann::json{
          {"phase", "completed"},
          {"title", "Knowledge vault stopped"},
          {"detail", "Hostd stopped the knowledge vault container."},
          {"percent", 100},
          {"service_id", service_id},
          {"node_name", node_name},
          {"container_name", container_name}});
}

void HostdAppAssignmentSupport::ExecuteHostSelfUpdate(
    const nlohmann::json& payload,
    const std::string& node_name,
    const std::optional<std::string>& host_private_key_path,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (backend == nullptr || !assignment_id.has_value()) {
    throw std::runtime_error("hostd-self-update requires a controller assignment");
  }
  if (!host_private_key_path.has_value() || host_private_key_path->empty()) {
    throw std::runtime_error("hostd-self-update requires --host-private-key");
  }

  const std::string release_tag = payload.value("release_tag", std::string{});
  const std::string hostd_image = payload.value("hostd_image", std::string{});
  if (release_tag.empty() || hostd_image.empty()) {
    throw std::runtime_error("hostd-self-update payload must contain release_tag and hostd_image");
  }

  const std::filesystem::path hostd_root =
      install_layout_support_.ResolveHostdRootFromPrivateKeyPath(*host_private_key_path);
  if (hostd_root.empty()) {
    throw std::runtime_error("failed to derive hostd root from host private key path");
  }
  const std::filesystem::path compose_file =
      payload.contains("compose_file_path") && payload.at("compose_file_path").is_string()
          ? std::filesystem::path(payload.at("compose_file_path").get<std::string>())
          : hostd_root / "docker-compose.yml";
  if (!std::filesystem::exists(compose_file)) {
    throw std::runtime_error("hostd-self-update compose file does not exist: " +
                             compose_file.string());
  }

  const std::filesystem::path registry_config_dir =
      payload.contains("registry_config_dir") && payload.at("registry_config_dir").is_string()
          ? std::filesystem::path(payload.at("registry_config_dir").get<std::string>())
          : hostd_root / "install-state" / "registry-docker";
  const std::filesystem::path update_script =
      hostd_root / "install-state" / "hostd-self-update.sh";
  const std::filesystem::path update_log =
      hostd_root / "logs" / ("hostd-self-update-" + release_tag + ".log");

  std::string script = "#!/usr/bin/env bash\n"
                       "set -euo pipefail\n";
  if (std::filesystem::exists(registry_config_dir / "config.json")) {
    script += "export DOCKER_CONFIG=" +
              command_support_.ShellQuote(registry_config_dir.string()) + "\n";
  }
  script += "python3 - " + command_support_.ShellQuote(compose_file.string()) + " " +
            command_support_.ShellQuote(hostd_image) + " <<'PY'\n"
            "import pathlib\n"
            "import sys\n"
            "\n"
            "compose_path = pathlib.Path(sys.argv[1])\n"
            "target_image = sys.argv[2]\n"
            "text = compose_path.read_text(encoding='utf-8')\n"
            "lines = text.splitlines()\n"
            "in_service = False\n"
            "service_indent = None\n"
            "updated = False\n"
            "for index, line in enumerate(lines):\n"
            "    stripped = line.lstrip()\n"
            "    indent = len(line) - len(stripped)\n"
            "    if stripped == 'naim-hostd:':\n"
            "        in_service = True\n"
            "        service_indent = indent\n"
            "        continue\n"
            "    if in_service and stripped and not stripped.startswith('#') and indent <= service_indent:\n"
            "        in_service = False\n"
            "    if in_service and stripped.startswith('image:'):\n"
            "        lines[index] = ' ' * indent + 'image: ' + target_image\n"
            "        updated = True\n"
            "        break\n"
            "if not updated:\n"
            "    raise SystemExit('failed to locate naim-hostd image line in compose file')\n"
            "compose_path.write_text('\\n'.join(lines) + '\\n', encoding='utf-8')\n"
            "PY\n"
            "sleep 2\n"
            "docker compose -f " + command_support_.ShellQuote(compose_file.string()) +
            " pull naim-hostd\n"
            "docker compose -f " + command_support_.ShellQuote(compose_file.string()) +
            " up -d --remove-orphans naim-hostd\n";

  file_support_.WriteTextFile(update_script.string(), script);
  if (!command_support_.RunCommandOk(
          "chmod 0700 " + command_support_.ShellQuote(update_script.string()))) {
    throw std::runtime_error("failed to chmod hostd self-update script");
  }

  backend->UpdateHostAssignmentProgress(
      *assignment_id,
      nlohmann::json{
          {"phase", "scheduled"},
          {"title", "Hostd self-update scheduled"},
          {"detail", "Hostd queued its own container refresh via docker compose."},
          {"percent", 90},
          {"node_name", node_name},
          {"release_tag", release_tag},
          {"hostd_image", hostd_image},
          {"compose_file_path", compose_file.string()},
          {"script_path", update_script.string()},
          {"log_path", update_log.string()}});

  const std::string launch_command =
      "nohup bash " + command_support_.ShellQuote(update_script.string()) + " >" +
      command_support_.ShellQuote(update_log.string()) + " 2>&1 < /dev/null &";
  if (!command_support_.RunCommandOk(launch_command)) {
    throw std::runtime_error("failed to launch hostd self-update background job");
  }
}

void HostdAppAssignmentSupport::ShowDemoOps(
    const std::string& node_name,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) const {
  display_support_.ShowDemoOps(node_name, storage_root, runtime_root);
}

void HostdAppAssignmentSupport::ShowStateOps(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root) const {
  display_support_.ShowStateOps(
      db_path,
      node_name,
      artifacts_root,
      storage_root,
      runtime_root,
      state_root);
}

void HostdAppAssignmentSupport::AppendHostdEvent(
    HostdBackend& backend,
    const std::string& category,
    const std::string& event_type,
    const std::string& message,
    const nlohmann::json& payload,
    const std::string& plane_name,
    const std::string& node_name,
    const std::string& worker_name,
    const std::optional<int>& assignment_id,
    const std::optional<int>& rollout_action_id,
    const std::string& severity) const {
  observation_support_.AppendHostdEvent(
      backend,
      category,
      event_type,
      message,
      payload,
      plane_name,
      node_name,
      worker_name,
      assignment_id,
      rollout_action_id,
      severity);
}

}  // namespace naim::hostd
