#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

#include "app/hostd_repo_root_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

class CurrentPathGuard {
 public:
  explicit CurrentPathGuard(const std::filesystem::path& new_path)
      : old_path_(std::filesystem::current_path()) {
    std::filesystem::current_path(new_path);
  }

  ~CurrentPathGuard() {
    std::error_code error;
    std::filesystem::current_path(old_path_, error);
  }

 private:
  std::filesystem::path old_path_;
};

void Touch(const std::filesystem::path& path, const std::string& contents = {}) {
  std::filesystem::create_directories(path.parent_path());
  std::ofstream output(path);
  output << contents;
}

naim::DesiredState BuildDesiredState(const std::string& plane_name) {
  naim::DesiredState state;
  state.plane_name = plane_name;
  return state;
}

}  // namespace

int main() {
  try {
    namespace fs = std::filesystem;
    const fs::path temp_root =
        fs::temp_directory_path() / "naim-hostd-repo-root-support-tests";
    std::error_code cleanup_error;
    fs::remove_all(temp_root, cleanup_error);

    const fs::path repo_root = temp_root / "backups" / "baal" / "repos" / "naim-node";
    const fs::path plane_root = temp_root / "backups" / "baal" / "repos" / "lt-cypher-ai";
    const fs::path build_root =
        temp_root / "backups" / "baal" / "builds" / "naim-node-build" / "linux" / "x64";
    const fs::path artifacts_root = temp_root / "artifacts";

    Touch(repo_root / "scripts" / "build-runtime-images.sh", "#!/bin/sh\n");
    Touch(repo_root / "runtime" / "base" / "Dockerfile", "FROM scratch\n");
    Touch(plane_root / "deploy" / "scripts" / "post-deploy.sh", "#!/bin/sh\n");
    Touch(artifacts_root / "lt-cypher-ai" / "deploy" / "scripts" / "artifact-hook.sh",
          "#!/bin/sh\n");
    fs::create_directories(build_root);

    {
      CurrentPathGuard guard(build_root);
      const naim::hostd::HostdRepoRootSupport support;
      const auto repo = support.DetectNaimRepoRoot();
      Expect(repo.has_value(), "repo root should be detected from split builds/repos layout");
      Expect(repo->lexically_normal() == repo_root.lexically_normal(),
             "detected repo root should match repos/naim-node");

      const auto plane_script = support.ResolvePlaneOwnedPath(
          BuildDesiredState("lt-cypher-ai"),
          "deploy/scripts/post-deploy.sh",
          artifacts_root.string());
      Expect(plane_script.has_value(), "sibling plane hook should resolve");
      Expect(
          plane_script->lexically_normal() ==
              (plane_root / "deploy" / "scripts" / "post-deploy.sh").lexically_normal(),
          "sibling plane hook path should match");

      const auto artifact_script = support.ResolvePlaneOwnedPath(
          BuildDesiredState("lt-cypher-ai"),
          "bundle://deploy/scripts/artifact-hook.sh",
          artifacts_root.string());
      Expect(artifact_script.has_value(), "artifact hook should resolve");
      Expect(
          artifact_script->lexically_normal() ==
              (artifacts_root / "lt-cypher-ai" / "deploy" / "scripts" / "artifact-hook.sh")
                  .lexically_normal(),
          "artifact hook path should match");
    }

    fs::remove_all(temp_root, cleanup_error);
    std::cout << "ok: detect-repo-root-from-split-build-layout\n";
    std::cout << "ok: resolve-plane-hook-from-sibling-repo\n";
    std::cout << "ok: resolve-plane-hook-from-artifacts\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
