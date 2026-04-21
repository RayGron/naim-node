#pragma once

#include <optional>
#include <ostream>
#include <string>
#include <vector>

namespace naim::controller {

class ControllerCommandLine {
 public:
  ControllerCommandLine(int argc, char** argv);

  bool HasCommand() const;
  const std::string& command() const;

  void PrintUsage(std::ostream& output) const;

  std::optional<std::string> node() const;
  std::optional<std::string> db() const;
  std::optional<std::string> bundle() const;
  std::optional<std::string> plane() const;
  std::optional<std::string> artifacts_root() const;
  std::optional<std::string> listen_host() const;
  std::optional<int> listen_port() const;
  std::optional<std::string> ui_root() const;
  std::optional<std::string> web_ui_root() const;
  std::optional<std::string> controller_upstream() const;
  std::optional<std::string> skills_factory_upstream() const;
  std::optional<std::string> compose_mode() const;
  std::optional<std::string> controller() const;
  std::optional<int> id() const;
  std::optional<int> stale_after() const;
  std::optional<int> limit() const;
  std::optional<std::string> availability() const;
  std::optional<std::string> message() const;
  std::optional<std::string> status() const;
  std::optional<std::string> worker() const;
  std::optional<std::string> category() const;
  std::optional<std::string> public_key() const;
  std::optional<std::string> public_key_base64() const;
  std::optional<std::string> state_file() const;

 private:
  std::optional<std::string> FindOptionValue(const char* name) const;
  std::optional<int> FindIntOption(const char* name) const;

  static std::string ReadTextFile(const std::string& path);
  static std::string Trim(std::string value);

  std::vector<std::string> args_;
  std::string command_;
};

}  // namespace naim::controller
