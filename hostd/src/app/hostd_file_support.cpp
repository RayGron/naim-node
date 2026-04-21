#include "app/hostd_file_support.h"

#include <filesystem>
#include <fstream>
#include <stdexcept>

namespace naim::hostd {

void HostdFileSupport::WriteTextFile(
    const std::string& path,
    const std::string& contents) const {
  const std::filesystem::path file_path(path);
  if (file_path.has_parent_path()) {
    std::filesystem::create_directories(file_path.parent_path());
  }

  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    throw std::runtime_error("failed to open file for write: " + path);
  }
  out << contents;
  if (!out.good()) {
    throw std::runtime_error("failed to write file: " + path);
  }
}

void HostdFileSupport::RemoveFileIfExists(const std::string& path) const {
  std::error_code error;
  std::filesystem::remove(path, error);
  if (error) {
    throw std::runtime_error("failed to remove file '" + path + "': " + error.message());
  }
}

void HostdFileSupport::EnsureParentDirectory(const std::string& path) const {
  const std::filesystem::path file_path(path);
  if (file_path.has_parent_path()) {
    std::filesystem::create_directories(file_path.parent_path());
  }
}

}  // namespace naim::hostd
