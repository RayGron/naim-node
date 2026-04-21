#pragma once

#include <optional>
#include <ostream>
#include <string>

namespace naim::hostd {

enum class ComposeMode {
  Skip,
  Exec,
};

class HostdCommandLine {
 public:
  HostdCommandLine(int argc, char** argv);

  void PrintUsage(std::ostream& out) const;
  bool HasCommand() const;

  const std::string& command() const;
  std::optional<std::string> node() const;
  std::optional<std::string> db() const;
  std::optional<std::string> controller() const;
  std::optional<std::string> artifacts_root() const;
  std::optional<std::string> runtime_root() const;
  std::optional<std::string> compose_mode_raw() const;
  std::optional<std::string> state_root() const;
  std::optional<std::string> host_private_key() const;
  std::optional<std::string> controller_fingerprint() const;
  std::optional<std::string> onboarding_key() const;
  std::optional<std::string> config_path() const;

  std::string ResolveDbPath(const std::optional<std::string>& db_arg) const;
  std::string ResolveArtifactsRoot(
      const std::optional<std::string>& artifacts_root_arg) const;
  std::string ResolveStateRoot(
      const std::optional<std::string>& state_root_arg) const;
  ComposeMode ResolveComposeMode(
      const std::optional<std::string>& compose_mode_arg) const;

 private:
  std::optional<std::string> FindOptionValue(const std::string& option_name) const;
  static std::string DefaultDbPath();
  static std::string DefaultArtifactsRoot();
  static std::string DefaultStateRoot();

  int argc_;
  char** argv_;
  std::string command_;
};

}  // namespace naim::hostd
