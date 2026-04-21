#include "app/hostd_bootstrap_transfer_support.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include <sodium.h>

#include "naim/security/crypto_utils.h"

namespace naim::hostd {

namespace fs = std::filesystem;

HostdBootstrapTransferSupport::HostdBootstrapTransferSupport(
    const HostdCommandSupport& command_support,
    const HostdFileSupport& file_support,
    const HostdReportingSupport& reporting_support)
    : command_support_(command_support),
      file_support_(file_support),
      reporting_support_(reporting_support) {}

std::optional<std::uintmax_t> HostdBootstrapTransferSupport::FileSizeIfExists(
    const std::string& path) const {
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

std::optional<std::uintmax_t> HostdBootstrapTransferSupport::ProbeContentLength(
    const std::string& source_url) const {
  const std::string output = command_support_.RunCommandCapture(
      "/usr/bin/curl -fsSLI " + command_support_.ShellQuote(source_url) + " 2>/dev/null || true");
  std::optional<std::uintmax_t> content_length;
  std::istringstream input(output);
  std::string line;
  while (std::getline(input, line)) {
    const std::string trimmed = command_support_.Trim(line);
    if (trimmed.empty()) {
      continue;
    }
    const auto colon = trimmed.find(':');
    if (colon == std::string::npos) {
      continue;
    }
    const std::string key =
        NormalizeLowercase(command_support_.Trim(trimmed.substr(0, colon)));
    if (key != "content-length") {
      continue;
    }
    try {
      content_length = static_cast<std::uintmax_t>(
          std::stoull(command_support_.Trim(trimmed.substr(colon + 1))));
    } catch (...) {
      content_length = std::nullopt;
    }
  }
  return content_length;
}

bool HostdBootstrapTransferSupport::CheckFileSha256Hex(
    const std::string& path,
    const std::string& expected_hex) const {
  return NormalizeLowercase(ComputeFileSha256Hex(path)) == NormalizeLowercase(expected_hex);
}

void HostdBootstrapTransferSupport::CopyFileWithProgress(
    const std::string& source_path,
    const std::string& target_path,
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const std::string& plane_name,
    const std::string& node_name,
    std::size_t part_index,
    std::size_t part_count,
    std::uintmax_t aggregate_prefix,
    const std::optional<std::uintmax_t>& aggregate_total) const {
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
      file_support_.EnsureParentDirectory(temp_target.string());
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
    file_support_.EnsureParentDirectory(target_root.string());
    fs::rename(temp_root, target_root);
    return;
  }

  const auto total_size = FileSizeIfExists(source_path);
  file_support_.EnsureParentDirectory(target_path);
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

void HostdBootstrapTransferSupport::DownloadFileWithProgress(
    const std::string& source_url,
    const std::string& target_path,
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const std::string& plane_name,
    const std::string& node_name,
    std::size_t part_index,
    std::size_t part_count,
    std::uintmax_t aggregate_prefix,
    const std::optional<std::uintmax_t>& aggregate_total) const {
  file_support_.EnsureParentDirectory(target_path);
  const std::string temp_path = target_path + ".part";
  const auto content_length = ProbeContentLength(source_url);

  if (const auto final_size = FileSizeIfExists(target_path);
      final_size.has_value() &&
      (!content_length.has_value() || *content_length == 0 || *final_size == *content_length)) {
    const std::uintmax_t overall_done = aggregate_prefix + *final_size;
    PublishAssignmentProgress(
        backend,
        assignment_id,
        "acquiring-model",
        "Acquiring model",
        part_count > 1
            ? ("Using existing bootstrap model part " + std::to_string(part_index + 1) + "/" +
               std::to_string(part_count) + " in the plane shared disk.")
            : "Using existing bootstrap model in the plane shared disk.",
        60,
        plane_name,
        node_name,
        aggregate_total.has_value() ? std::optional<std::uintmax_t>(overall_done)
                                    : std::optional<std::uintmax_t>(*final_size),
        aggregate_total.has_value() ? aggregate_total : content_length);
    return;
  }

  if (content_length.has_value() && *content_length > 0) {
    const auto partial_size = FileSizeIfExists(temp_path).value_or(0);
    if (partial_size == *content_length) {
      fs::rename(temp_path, target_path);
      return;
    }
    if (partial_size > *content_length) {
      fs::remove(temp_path);
    }
  }

  auto future = std::async(
      std::launch::async,
      [command = "/usr/bin/curl -fL -C - --silent --show-error --output " +
                     command_support_.ShellQuote(temp_path) +
                     " " + command_support_.ShellQuote(source_url)]() {
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

std::string HostdBootstrapTransferSupport::NormalizeLowercase(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

std::string HostdBootstrapTransferSupport::ComputeFileSha256Hex(const std::string& path) const {
  naim::InitializeCrypto();
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

void HostdBootstrapTransferSupport::PublishAssignmentProgress(
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const std::string& phase,
    const std::string& title,
    const std::string& detail,
    int percent,
    const std::string& plane_name,
    const std::string& node_name,
    const std::optional<std::uintmax_t>& bytes_done,
    const std::optional<std::uintmax_t>& bytes_total) const {
  reporting_support_.PublishAssignmentProgress(
      backend,
      assignment_id,
      reporting_support_.BuildAssignmentProgressPayload(
          phase,
          title,
          detail,
          percent,
          plane_name,
          node_name,
          bytes_done,
          bytes_total));
}

}  // namespace naim::hostd
