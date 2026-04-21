#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "app/hostd_command_support.h"
#include "app/hostd_file_support.h"
#include "app/hostd_reporting_support.h"
#include "backend/hostd_backend.h"

namespace naim::hostd {

class HostdBootstrapTransferSupport final {
 public:
  HostdBootstrapTransferSupport(
      const HostdCommandSupport& command_support,
      const HostdFileSupport& file_support,
      const HostdReportingSupport& reporting_support);

  std::optional<std::uintmax_t> FileSizeIfExists(const std::string& path) const;
  std::optional<std::uintmax_t> ProbeContentLength(const std::string& source_url) const;
  bool CheckFileSha256Hex(const std::string& path, const std::string& expected_hex) const;
  void CopyFileWithProgress(
      const std::string& source_path,
      const std::string& target_path,
      HostdBackend* backend,
      const std::optional<int>& assignment_id,
      const std::string& plane_name,
      const std::string& node_name,
      std::size_t part_index = 0,
      std::size_t part_count = 1,
      std::uintmax_t aggregate_prefix = 0,
      const std::optional<std::uintmax_t>& aggregate_total = std::nullopt) const;
  void DownloadFileWithProgress(
      const std::string& source_url,
      const std::string& target_path,
      HostdBackend* backend,
      const std::optional<int>& assignment_id,
      const std::string& plane_name,
      const std::string& node_name,
      std::size_t part_index = 0,
      std::size_t part_count = 1,
      std::uintmax_t aggregate_prefix = 0,
      const std::optional<std::uintmax_t>& aggregate_total = std::nullopt) const;

 private:
  static std::string NormalizeLowercase(std::string value);
  std::string ComputeFileSha256Hex(const std::string& path) const;
  void PublishAssignmentProgress(
      HostdBackend* backend,
      const std::optional<int>& assignment_id,
      const std::string& phase,
      const std::string& title,
      const std::string& detail,
      int percent,
      const std::string& plane_name,
      const std::string& node_name,
      const std::optional<std::uintmax_t>& bytes_done = std::nullopt,
      const std::optional<std::uintmax_t>& bytes_total = std::nullopt) const;

  const HostdCommandSupport& command_support_;
  const HostdFileSupport& file_support_;
  const HostdReportingSupport& reporting_support_;
};

}  // namespace naim::hostd
