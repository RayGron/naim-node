#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

#include "app/hostd_file_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  return std::string(
      std::istreambuf_iterator<char>(input),
      std::istreambuf_iterator<char>());
}

}  // namespace

int main() {
  try {
    namespace fs = std::filesystem;
    const naim::hostd::HostdFileSupport support;
    const fs::path temp_root = fs::temp_directory_path() / "naim-hostd-file-support-tests";
    std::error_code cleanup_error;
    fs::remove_all(temp_root, cleanup_error);

    {
      const fs::path file_path = temp_root / "nested" / "demo.txt";
      support.WriteTextFile(file_path.string(), "hello");
      Expect(fs::exists(file_path), "WriteTextFile should create the file");
      Expect(ReadFile(file_path) == "hello", "WriteTextFile should persist contents");
    }

    {
      const fs::path file_path = temp_root / "parent-only" / "demo.txt";
      support.EnsureParentDirectory(file_path.string());
      Expect(
          fs::exists(file_path.parent_path()),
          "EnsureParentDirectory should create the parent directory");
    }

    {
      const fs::path file_path = temp_root / "delete-me.txt";
      support.WriteTextFile(file_path.string(), "bye");
      support.RemoveFileIfExists(file_path.string());
      Expect(!fs::exists(file_path), "RemoveFileIfExists should remove the file");
    }

    fs::remove_all(temp_root, cleanup_error);
    std::cout << "ok: hostd-file-support-write\n";
    std::cout << "ok: hostd-file-support-parent-directory\n";
    std::cout << "ok: hostd-file-support-remove\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
