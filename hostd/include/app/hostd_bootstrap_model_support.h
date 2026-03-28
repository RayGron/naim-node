#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "backend/hostd_backend.h"
#include "comet/state/models.h"

namespace comet::hostd {

class HostdBootstrapModelSupport final {
 public:
  using ProgressPayloadBuilder = std::function<nlohmann::json(
      const std::string& phase,
      const std::string& title,
      const std::string& detail,
      int percent,
      const std::string& plane_name,
      const std::string& node_name,
      const std::optional<std::uintmax_t>& bytes_done,
      const std::optional<std::uintmax_t>& bytes_total)>;

  struct Deps {
    std::function<const comet::DiskSpec*(const comet::DesiredState&, const std::string&)>
        find_plane_shared_disk_for_node;
    std::function<std::string(const comet::DiskSpec&, const std::string&, const std::string&)>
        shared_disk_host_path_for_container_path;
    std::function<std::optional<std::string>(
        const comet::DesiredState&,
        const std::string&,
        const std::string&)> control_file_path_for_node;
    std::function<std::string(const comet::DesiredState&)> require_single_node_name;
    std::function<std::string(const std::string&)> run_command_capture;
    std::function<std::string(const std::string&)> shell_quote;
    std::function<std::string(const std::string&)> normalize_lowercase;
    std::function<std::string(const std::string&)> trim;
    std::function<void(const std::string&, const std::string&)> write_text_file;
    std::function<void(const std::string&)> remove_file_if_exists;
    std::function<void(const std::string&)> ensure_parent_directory;
    ProgressPayloadBuilder build_assignment_progress_payload;
    std::function<void(HostdBackend*, const std::optional<int>&, const nlohmann::json&)>
        publish_assignment_progress;
  };

  explicit HostdBootstrapModelSupport(Deps deps);

  void BootstrapPlaneModelIfNeeded(
      const comet::DesiredState& state,
      const std::string& node_name,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const;

 private:
  Deps deps_;
};

}  // namespace comet::hostd
