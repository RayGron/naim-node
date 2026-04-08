#include "app/hostd_bootstrap_model_support.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

#include <sodium.h>

#include "comet/security/crypto_utils.h"

namespace comet::hostd {

namespace {

using nlohmann::json;
namespace fs = std::filesystem;

struct BootstrapModelArtifact {
  std::optional<std::string> local_path;
  std::optional<std::string> source_url;
  std::string target_host_path;
};

constexpr std::string_view kTurboQuantDefaultCacheTypeK = "planar3";
constexpr std::string_view kTurboQuantDefaultCacheTypeV = "f16";

const comet::DiskSpec& RequirePlaneSharedDiskForNode(
    const HostdBootstrapModelSupport::Deps& deps,
    const comet::DesiredState& state,
    const std::string& node_name) {
  const comet::DiskSpec* shared_disk = deps.find_plane_shared_disk_for_node(state, node_name);
  if (shared_disk != nullptr) {
    return *shared_disk;
  }
  throw std::runtime_error(
      "plane '" + state.plane_name + "' is missing a plane-shared disk for node '" + node_name +
      "'");
}

std::string FilenameFromUrl(const std::string& source_url) {
  const auto query = source_url.find_first_of("?#");
  const std::string without_query =
      query == std::string::npos ? source_url : source_url.substr(0, query);
  const std::string filename = fs::path(without_query).filename().string();
  if (filename.empty()) {
    throw std::runtime_error("failed to infer filename from bootstrap model URL: " + source_url);
  }
  return filename;
}

std::optional<std::uintmax_t> FileSizeIfExists(const std::string& path) {
  std::error_code error;
  if (!fs::exists(path, error) || error) {
    return std::nullopt;
  }
  if (fs::is_directory(path, error)) {
    if (error) {
      return std::nullopt;
    }
    std::uintmax_t total = 0;
    for (const auto& entry : fs::recursive_directory_iterator(path, error)) {
      if (error) {
        return std::nullopt;
      }
      if (!entry.is_regular_file(error)) {
        if (error) {
          return std::nullopt;
        }
        continue;
      }
      total += entry.file_size(error);
      if (error) {
        return std::nullopt;
      }
    }
    return total;
  }
  const auto size = fs::file_size(path, error);
  if (error) {
    return std::nullopt;
  }
  return size;
}

bool LooksLikeRecognizedModelDirectory(const std::string& path) {
  std::error_code error;
  const fs::path root(path);
  if (!fs::exists(root, error) || error || !fs::is_directory(root, error) || error) {
    return false;
  }
  return fs::exists(root / "config.json", error) || fs::exists(root / "params.json", error);
}

std::string ActiveModelPathForNode(
    const HostdBootstrapModelSupport::Deps& deps,
    const comet::DesiredState& state,
    const std::string& node_name) {
  const auto active_model_path =
      deps.control_file_path_for_node(state, node_name, "active-model.json");
  if (!active_model_path.has_value()) {
    throw std::runtime_error(
        "plane '" + state.plane_name + "' is missing infer control path for node '" + node_name +
        "'");
  }
  return *active_model_path;
}

std::vector<BootstrapModelArtifact> BuildBootstrapModelArtifacts(
    const HostdBootstrapModelSupport::Deps& deps,
    const comet::DesiredState& state,
    const std::string& node_name) {
  const auto& shared_disk = RequirePlaneSharedDiskForNode(deps, state, node_name);
  const fs::path target_root = deps.shared_disk_host_path_for_container_path(
      shared_disk,
      state.inference.gguf_cache_dir,
      "models/gguf");

  std::vector<BootstrapModelArtifact> artifacts;
  std::string filename = "model.gguf";
  if (!state.bootstrap_model.has_value()) {
    artifacts.push_back(BootstrapModelArtifact{
        std::nullopt,
        std::nullopt,
        (target_root / filename).string(),
    });
    return artifacts;
  }

  const auto& bootstrap_model = *state.bootstrap_model;
  if (!bootstrap_model.source_urls.empty()) {
    artifacts.reserve(bootstrap_model.source_urls.size());
    for (const auto& source_url : bootstrap_model.source_urls) {
      artifacts.push_back(BootstrapModelArtifact{
          std::nullopt,
          source_url,
          (target_root / FilenameFromUrl(source_url)).string(),
      });
    }
    return artifacts;
  }

  if (bootstrap_model.target_filename.has_value() && !bootstrap_model.target_filename->empty()) {
    filename = *bootstrap_model.target_filename;
  } else if (bootstrap_model.local_path.has_value() && !bootstrap_model.local_path->empty()) {
    filename = fs::path(*bootstrap_model.local_path).filename().string();
  } else if (bootstrap_model.source_url.has_value() && !bootstrap_model.source_url->empty()) {
    filename = FilenameFromUrl(*bootstrap_model.source_url);
  }
  if (filename.empty()) {
    filename = "model.gguf";
  }
  artifacts.push_back(BootstrapModelArtifact{
      bootstrap_model.local_path,
      bootstrap_model.source_url,
      (target_root / filename).string(),
  });
  return artifacts;
}

std::string BootstrapModelTargetPath(
    const HostdBootstrapModelSupport::Deps& deps,
    const comet::DesiredState& state,
    const std::string& node_name) {
  const auto artifacts = BuildBootstrapModelArtifacts(deps, state, node_name);
  if (artifacts.empty()) {
    throw std::runtime_error(
        "failed to resolve bootstrap model target path for plane '" + state.plane_name + "'");
  }
  return artifacts.front().target_host_path;
}

std::string BootstrapRuntimeModelPath(
    const HostdBootstrapModelSupport::Deps& deps,
    const comet::DesiredState& state,
    const std::string& target_host_path) {
  const std::string node_name = deps.require_single_node_name(state);
  const comet::DiskSpec shared_disk = RequirePlaneSharedDiskForNode(deps, state, node_name);
  const fs::path target_path(target_host_path);
  const fs::path shared_root(shared_disk.host_path);
  std::string relative = target_path.lexically_relative(shared_root).generic_string();
  if (relative.empty() || relative == ".") {
    relative = target_path.filename().string();
  }
  return (fs::path(shared_disk.container_path) / relative).generic_string();
}

std::string SharedModelBootstrapOwnerNode(const comet::DesiredState& state) {
  if (!state.inference.primary_infer_node.empty()) {
    return state.inference.primary_infer_node;
  }
  return state.nodes.empty() ? std::string{} : state.nodes.front().name;
}

std::string ComputeFileSha256Hex(const std::string& path) {
  comet::InitializeCrypto();
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open file for sha256: " + path);
  }
  crypto_hash_sha256_state context;
  crypto_hash_sha256_init(&context);
  std::array<char, 1024 * 1024> buffer{};
  while (input.good()) {
    input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    const auto count = input.gcount();
    if (count > 0) {
      crypto_hash_sha256_update(
          &context,
          reinterpret_cast<const unsigned char*>(buffer.data()),
          static_cast<unsigned long long>(count));
    }
  }
  std::array<unsigned char, crypto_hash_sha256_BYTES> digest{};
  crypto_hash_sha256_final(&context, digest.data());
  std::ostringstream out;
  out << std::hex << std::setfill('0');
  for (unsigned char byte : digest) {
    out << std::setw(2) << static_cast<int>(byte);
  }
  return out.str();
}

std::optional<std::uintmax_t> ProbeContentLength(
    const HostdBootstrapModelSupport::Deps& deps,
    const std::string& source_url) {
  const std::string output = deps.run_command_capture(
      "/usr/bin/curl -fsSLI " + deps.shell_quote(source_url) + " 2>/dev/null || true");
  std::optional<std::uintmax_t> content_length;
  std::istringstream input(output);
  std::string line;
  while (std::getline(input, line)) {
    const std::string trimmed = deps.trim(line);
    if (trimmed.empty()) {
      continue;
    }
    const auto colon = trimmed.find(':');
    if (colon == std::string::npos) {
      continue;
    }
    const std::string key = deps.normalize_lowercase(deps.trim(trimmed.substr(0, colon)));
    if (key != "content-length") {
      continue;
    }
    try {
      content_length = static_cast<std::uintmax_t>(
          std::stoull(deps.trim(trimmed.substr(colon + 1))));
    } catch (...) {
      content_length = std::nullopt;
    }
  }
  return content_length;
}

void PublishAssignmentProgress(
    const HostdBootstrapModelSupport::Deps& deps,
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const std::string& phase,
    const std::string& title,
    const std::string& detail,
    int percent,
    const std::string& plane_name,
    const std::string& node_name,
    const std::optional<std::uintmax_t>& bytes_done = std::nullopt,
    const std::optional<std::uintmax_t>& bytes_total = std::nullopt) {
  deps.publish_assignment_progress(
      backend,
      assignment_id,
      deps.build_assignment_progress_payload(
          phase,
          title,
          detail,
          percent,
          plane_name,
          node_name,
          bytes_done,
          bytes_total));
}

void CopyFileWithProgress(
    const HostdBootstrapModelSupport::Deps& deps,
    const std::string& source_path,
    const std::string& target_path,
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const std::string& plane_name,
    const std::string& node_name,
    std::size_t part_index = 0,
    std::size_t part_count = 1,
    std::uintmax_t aggregate_prefix = 0,
    const std::optional<std::uintmax_t>& aggregate_total = std::nullopt) {
  if (fs::is_directory(source_path)) {
    const auto total_size = FileSizeIfExists(source_path);
    const fs::path source_root(source_path);
    const fs::path target_root(target_path);
    const fs::path temp_root = target_root.string() + ".partdir";
    fs::remove_all(temp_root);
    fs::create_directories(temp_root);
    std::uintmax_t copied = 0;
    for (const auto& entry : fs::recursive_directory_iterator(source_root)) {
      const auto relative = entry.path().lexically_relative(source_root);
      const auto temp_target = temp_root / relative;
      if (entry.is_directory()) {
        fs::create_directories(temp_target);
        continue;
      }
      if (!entry.is_regular_file()) {
        continue;
      }
      deps.ensure_parent_directory(temp_target.string());
      std::ifstream input(entry.path(), std::ios::binary);
      if (!input.is_open()) {
        throw std::runtime_error(
            "failed to open bootstrap model source file: " + entry.path().string());
      }
      std::ofstream output(temp_target, std::ios::binary | std::ios::trunc);
      if (!output.is_open()) {
        throw std::runtime_error(
            "failed to open bootstrap model target file: " + temp_target.string());
      }
      std::array<char, 1024 * 1024> buffer{};
      while (input.good()) {
        input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const auto count = input.gcount();
        if (count <= 0) {
          break;
        }
        output.write(buffer.data(), count);
        copied += static_cast<std::uintmax_t>(count);
        const std::uintmax_t overall_done = aggregate_prefix + copied;
        int percent = 40;
        if (aggregate_total.has_value() && *aggregate_total > 0) {
          percent =
              20 + static_cast<int>((static_cast<double>(overall_done) / *aggregate_total) * 40.0);
        } else if (total_size.has_value() && *total_size > 0) {
          percent = 20 + static_cast<int>((static_cast<double>(copied) / *total_size) * 40.0);
        }
        PublishAssignmentProgress(
            deps,
            backend,
            assignment_id,
            "acquiring-model",
            "Acquiring model",
            part_count > 1
                ? ("Copying bootstrap model part " + std::to_string(part_index + 1) + "/" +
                   std::to_string(part_count) + " into the plane shared disk.")
                : "Copying bootstrap model into the plane shared disk.",
            percent,
            plane_name,
            node_name,
            aggregate_total.has_value() ? std::optional<std::uintmax_t>(overall_done)
                                        : std::optional<std::uintmax_t>(copied),
            aggregate_total.has_value() ? aggregate_total : total_size);
      }
      output.close();
      if (!output.good()) {
        throw std::runtime_error(
            "failed to write bootstrap model target file: " + temp_target.string());
      }
    }
    fs::remove_all(target_root);
    deps.ensure_parent_directory(target_root.string());
    fs::rename(temp_root, target_root);
    return;
  }

  const auto total_size = FileSizeIfExists(source_path);
  deps.ensure_parent_directory(target_path);
  const std::string temp_path = target_path + ".part";
  std::ifstream input(source_path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open bootstrap model source: " + source_path);
  }
  std::ofstream output(temp_path, std::ios::binary | std::ios::trunc);
  if (!output.is_open()) {
    throw std::runtime_error("failed to open bootstrap model target: " + temp_path);
  }
  std::array<char, 1024 * 1024> buffer{};
  std::uintmax_t copied = 0;
  while (input.good()) {
    input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    const auto count = input.gcount();
    if (count <= 0) {
      break;
    }
    output.write(buffer.data(), count);
    copied += static_cast<std::uintmax_t>(count);
    const std::uintmax_t overall_done = aggregate_prefix + copied;
    int percent = 40;
    if (aggregate_total.has_value() && *aggregate_total > 0) {
      percent =
          20 + static_cast<int>((static_cast<double>(overall_done) / *aggregate_total) * 40.0);
    } else if (total_size.has_value() && *total_size > 0) {
      percent = 20 + static_cast<int>((static_cast<double>(copied) / *total_size) * 40.0);
    }
    PublishAssignmentProgress(
        deps,
        backend,
        assignment_id,
        "acquiring-model",
        "Acquiring model",
        part_count > 1
            ? ("Copying bootstrap model part " + std::to_string(part_index + 1) + "/" +
               std::to_string(part_count) + " into the plane shared disk.")
            : "Copying bootstrap model into the plane shared disk.",
        percent,
        plane_name,
        node_name,
        aggregate_total.has_value() ? std::optional<std::uintmax_t>(overall_done)
                                    : std::optional<std::uintmax_t>(copied),
        aggregate_total.has_value() ? aggregate_total : total_size);
  }
  output.close();
  if (!output.good()) {
    throw std::runtime_error("failed to write bootstrap model target: " + temp_path);
  }
  fs::rename(temp_path, target_path);
}

void DownloadFileWithProgress(
    const HostdBootstrapModelSupport::Deps& deps,
    const std::string& source_url,
    const std::string& target_path,
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const std::string& plane_name,
    const std::string& node_name,
    std::size_t part_index = 0,
    std::size_t part_count = 1,
    std::uintmax_t aggregate_prefix = 0,
    const std::optional<std::uintmax_t>& aggregate_total = std::nullopt) {
  deps.ensure_parent_directory(target_path);
  const std::string temp_path = target_path + ".part";
  fs::remove(temp_path);
  const auto content_length = ProbeContentLength(deps, source_url);
  auto future = std::async(
      std::launch::async,
      [command = "/usr/bin/curl -fL --silent --show-error --output " + deps.shell_quote(temp_path) +
                     " " + deps.shell_quote(source_url)]() {
        return std::system(command.c_str());
      });
  while (future.wait_for(std::chrono::milliseconds(500)) != std::future_status::ready) {
    const auto bytes_done = FileSizeIfExists(temp_path).value_or(0);
    const std::uintmax_t overall_done = aggregate_prefix + bytes_done;
    int percent = 40;
    if (aggregate_total.has_value() && *aggregate_total > 0) {
      percent =
          20 + static_cast<int>((static_cast<double>(overall_done) / *aggregate_total) * 40.0);
    } else if (content_length.has_value() && *content_length > 0) {
      percent = 20 + static_cast<int>((static_cast<double>(bytes_done) / *content_length) * 40.0);
    }
    PublishAssignmentProgress(
        deps,
        backend,
        assignment_id,
        "acquiring-model",
        "Acquiring model",
        part_count > 1
            ? ("Downloading bootstrap model part " + std::to_string(part_index + 1) + "/" +
               std::to_string(part_count) + " into the plane shared disk.")
            : "Downloading bootstrap model into the plane shared disk.",
        percent,
        plane_name,
        node_name,
        aggregate_total.has_value() ? std::optional<std::uintmax_t>(overall_done)
                                    : std::optional<std::uintmax_t>(bytes_done),
        aggregate_total.has_value() ? aggregate_total : content_length);
  }
  const int rc = future.get();
  if (rc != 0) {
    throw std::runtime_error("failed to download bootstrap model from " + source_url);
  }
  fs::rename(temp_path, target_path);
}

void WriteBootstrapActiveModel(
    const HostdBootstrapModelSupport::Deps& deps,
    const comet::DesiredState& state,
    const std::string& node_name,
    const std::string& target_host_path,
    const std::optional<std::string>& runtime_model_path_override = std::nullopt) {
  const auto& bootstrap_model = *state.bootstrap_model;
  const std::string runtime_model_path =
      runtime_model_path_override.has_value()
          ? *runtime_model_path_override
          : BootstrapRuntimeModelPath(deps, state, target_host_path);
  std::vector<std::string> llama_args;
  std::optional<std::string> active_cache_type_k;
  std::optional<std::string> active_cache_type_v;
  if (state.turboquant.has_value() && state.turboquant->enabled) {
    active_cache_type_k = state.turboquant->cache_type_k.value_or(
        std::string(kTurboQuantDefaultCacheTypeK));
    active_cache_type_v = state.turboquant->cache_type_v.value_or(
        std::string(kTurboQuantDefaultCacheTypeV));
    llama_args = {
        "--cache-type-k",
        *active_cache_type_k,
        "--cache-type-v",
        *active_cache_type_v,
    };
  }
  json payload{
      {"version", 1},
      {"plane_name", state.plane_name},
      {"model_id", bootstrap_model.model_id},
      {"source_model_id", bootstrap_model.model_id},
      {"served_model_name",
       bootstrap_model.served_model_name.has_value()
           ? *bootstrap_model.served_model_name
           : bootstrap_model.model_id},
      {"local_model_path", target_host_path},
      {"cached_local_model_path", target_host_path},
      {"cached_runtime_model_path", runtime_model_path},
      {"runtime_model_path", runtime_model_path},
  };
  if (!llama_args.empty()) {
    payload["llama_args"] = llama_args;
    payload["turboquant_enabled"] = true;
    payload["active_cache_type_k"] = *active_cache_type_k;
    payload["active_cache_type_v"] = *active_cache_type_v;
  }
  deps.write_text_file(
      ActiveModelPathForNode(deps, state, node_name),
      payload.dump(2));
}

}  // namespace

HostdBootstrapModelSupport::HostdBootstrapModelSupport(Deps deps) : deps_(std::move(deps)) {}

void HostdBootstrapModelSupport::BootstrapPlaneModelIfNeeded(
    const comet::DesiredState& state,
    const std::string& node_name,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) const {
  if (state.instances.empty()) {
    return;
  }

  const auto& shared_disk = RequirePlaneSharedDiskForNode(deps_, state, node_name);
  if (!fs::exists(shared_disk.host_path)) {
    throw std::runtime_error(
        "plane shared disk path does not exist after ensure-disk: " + shared_disk.host_path);
  }

  PublishAssignmentProgress(
      deps_,
      backend,
      assignment_id,
      "ensuring-shared-disk",
      "Ensuring shared disk",
      "Plane shared disk is mounted and ready for model/bootstrap data.",
      12,
      state.plane_name,
      node_name);

  const std::string active_model_path = ActiveModelPathForNode(deps_, state, node_name);
  if (!state.bootstrap_model.has_value()) {
    deps_.remove_file_if_exists(active_model_path);
    return;
  }

  const auto& bootstrap_model = *state.bootstrap_model;
  if (bootstrap_model.materialization_mode == "reference" &&
      bootstrap_model.local_path.has_value() &&
      !bootstrap_model.local_path->empty()) {
    const fs::path reference_path(*bootstrap_model.local_path);
    std::error_code error;
    if (!fs::exists(reference_path, error) || error) {
      throw std::runtime_error(
          "bootstrap model reference path does not exist: " + *bootstrap_model.local_path);
    }
    PublishAssignmentProgress(
        deps_,
        backend,
        assignment_id,
        "using-model-reference",
        "Using model reference",
        "Using the configured model path directly without copying it into the plane shared disk.",
        72,
        state.plane_name,
        node_name);
    WriteBootstrapActiveModel(
        deps_,
        state,
        node_name,
        *bootstrap_model.local_path,
        *bootstrap_model.local_path);
    return;
  }
  const std::string target_path = BootstrapModelTargetPath(deps_, state, node_name);
  const auto artifacts = BuildBootstrapModelArtifacts(deps_, state, node_name);
  const std::string bootstrap_owner_node = SharedModelBootstrapOwnerNode(state);
  const bool shared_bootstrap_owned_elsewhere =
      bootstrap_owner_node != node_name &&
      std::any_of(
          artifacts.begin(),
          artifacts.end(),
          [](const BootstrapModelArtifact& artifact) {
            return artifact.local_path.has_value() && !artifact.local_path->empty();
          });
  if (shared_bootstrap_owned_elsewhere) {
    for (int attempt = 0; attempt < 300; ++attempt) {
      if (LooksLikeRecognizedModelDirectory(target_path) || fs::exists(target_path)) {
        WriteBootstrapActiveModel(deps_, state, node_name, target_path);
        return;
      }
      PublishAssignmentProgress(
          deps_,
          backend,
          assignment_id,
          "waiting-for-model",
          "Waiting for shared model",
          "Waiting for " + bootstrap_owner_node +
              " to finish copying the shared model into the plane disk.",
          18,
          state.plane_name,
          node_name);
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    throw std::runtime_error(
        "timed out waiting for shared model bootstrap on node '" + bootstrap_owner_node + "'");
  }

  bool already_present = !artifacts.empty();
  std::optional<std::uintmax_t> aggregate_total = std::uintmax_t{0};
  for (const auto& artifact : artifacts) {
    if (!fs::exists(artifact.target_host_path)) {
      already_present = false;
    }
    std::optional<std::uintmax_t> expected_size;
    if (artifact.local_path.has_value() && !artifact.local_path->empty()) {
      expected_size = FileSizeIfExists(*artifact.local_path);
      const auto target_size = FileSizeIfExists(artifact.target_host_path);
      const bool source_is_directory = fs::is_directory(*artifact.local_path);
      const bool target_has_model_root =
          !source_is_directory || LooksLikeRecognizedModelDirectory(artifact.target_host_path);
      if (!expected_size.has_value() || !target_size.has_value() || *expected_size != *target_size) {
        already_present = false;
      } else if (!target_has_model_root) {
        already_present = false;
      }
    } else if (artifact.source_url.has_value() && !artifact.source_url->empty()) {
      expected_size = ProbeContentLength(deps_, *artifact.source_url);
      const auto target_size = FileSizeIfExists(artifact.target_host_path);
      if (!expected_size.has_value() || !target_size.has_value() || *expected_size != *target_size) {
        already_present = false;
      }
    } else if (!fs::exists(artifact.target_host_path)) {
      already_present = false;
    }
    if (!expected_size.has_value()) {
      aggregate_total = std::nullopt;
    } else if (aggregate_total.has_value()) {
      *aggregate_total += *expected_size;
    }
  }

  if (already_present && bootstrap_model.sha256.has_value() && artifacts.size() == 1) {
    if (fs::is_directory(target_path)) {
      throw std::runtime_error(
          "bootstrap_model.sha256 is not supported for directory-based bootstrap models");
    }
    PublishAssignmentProgress(
        deps_,
        backend,
        assignment_id,
        "verifying-model",
        "Verifying model",
        "Checking the existing shared-disk model checksum.",
        68,
        state.plane_name,
        node_name);
    already_present =
        deps_.normalize_lowercase(ComputeFileSha256Hex(target_path)) ==
        deps_.normalize_lowercase(*bootstrap_model.sha256);
  } else if (bootstrap_model.sha256.has_value() && artifacts.size() > 1) {
    throw std::runtime_error(
        "bootstrap_model.sha256 is not supported with multipart bootstrap_model.source_urls");
  }

  if (!already_present) {
    std::uintmax_t aggregate_prefix = 0;
    for (std::size_t index = 0; index < artifacts.size(); ++index) {
      const auto& artifact = artifacts[index];
      bool artifact_present = fs::exists(artifact.target_host_path);
      std::optional<std::uintmax_t> artifact_size = FileSizeIfExists(artifact.target_host_path);
      if (artifact.local_path.has_value() && !artifact.local_path->empty()) {
        const auto source_size = FileSizeIfExists(*artifact.local_path);
        artifact_present = artifact_present && source_size.has_value() && artifact_size.has_value() &&
                           *source_size == *artifact_size;
        if (!artifact_present) {
          CopyFileWithProgress(
              deps_,
              *artifact.local_path,
              artifact.target_host_path,
              backend,
              assignment_id,
              state.plane_name,
              node_name,
              index,
              artifacts.size(),
              aggregate_prefix,
              aggregate_total);
          artifact_size = FileSizeIfExists(artifact.target_host_path);
        }
      } else if (artifact.source_url.has_value() && !artifact.source_url->empty()) {
        const auto remote_size = ProbeContentLength(deps_, *artifact.source_url);
        artifact_present = artifact_present && remote_size.has_value() && artifact_size.has_value() &&
                           *remote_size == *artifact_size;
        if (!artifact_present) {
          DownloadFileWithProgress(
              deps_,
              *artifact.source_url,
              artifact.target_host_path,
              backend,
              assignment_id,
              state.plane_name,
              node_name,
              index,
              artifacts.size(),
              aggregate_prefix,
              aggregate_total);
          artifact_size = FileSizeIfExists(artifact.target_host_path);
        }
      }
      if (artifact_size.has_value()) {
        aggregate_prefix += *artifact_size;
      }
    }
  }

  if (bootstrap_model.sha256.has_value() && artifacts.size() == 1) {
    if (fs::is_directory(target_path)) {
      throw std::runtime_error(
          "bootstrap_model.sha256 is not supported for directory-based bootstrap models");
    }
    PublishAssignmentProgress(
        deps_,
        backend,
        assignment_id,
        "verifying-model",
        "Verifying model",
        "Verifying the model checksum in the shared disk.",
        72,
        state.plane_name,
        node_name);
    if (deps_.normalize_lowercase(ComputeFileSha256Hex(target_path)) !=
        deps_.normalize_lowercase(*bootstrap_model.sha256)) {
      throw std::runtime_error("bootstrap model checksum mismatch for " + target_path);
    }
  }

  const bool has_source =
      (bootstrap_model.local_path.has_value() && !bootstrap_model.local_path->empty()) ||
      (bootstrap_model.source_url.has_value() && !bootstrap_model.source_url->empty()) ||
      !bootstrap_model.source_urls.empty();
  if (!has_source && !fs::exists(target_path)) {
    deps_.remove_file_if_exists(active_model_path);
    return;
  }

  PublishAssignmentProgress(
      deps_,
      backend,
      assignment_id,
      "activating-model",
      "Activating model",
      "Writing active-model.json for infer and worker runtime.",
      80,
      state.plane_name,
      node_name);
  WriteBootstrapActiveModel(deps_, state, node_name, target_path);
}

}  // namespace comet::hostd
