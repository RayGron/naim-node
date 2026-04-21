#include "app/hostd_registration_service.h"

#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "naim/state/sqlite_store.h"

namespace naim::launcher {

namespace fs = std::filesystem;

void HostdRegistrationService::Connect(const LauncherCommandLine& command_line) const {
  const auto db_path = command_line.FindFlagValue("--db");
  const auto node_name = command_line.FindFlagValue("--node");
  const auto public_key = command_line.FindFlagValue("--public-key");
  if (!db_path.has_value() || !node_name.has_value() || !public_key.has_value()) {
    throw std::runtime_error("--db, --node and --public-key are required for connect-hostd");
  }

  naim::ControllerStore store(*db_path);
  store.Initialize();
  naim::RegisteredHostRecord record;
  record.node_name = *node_name;
  record.advertised_address = command_line.FindFlagValue("--address").value_or("");
  record.public_key_base64 = ReadPublicKeyBase64Argument(*public_key);
  record.controller_public_key_fingerprint =
      command_line.FindFlagValue("--controller-fingerprint").value_or("");
  record.transport_mode = command_line.FindFlagValue("--transport").value_or("out");
  record.execution_mode = command_line.FindFlagValue("--execution-mode").value_or("mixed");
  record.registration_state = "registered";
  record.session_state = "disconnected";
  record.capabilities_json = "{}";
  record.status_message = "registered by naim-node connect-hostd";
  store.UpsertRegisteredHost(record);
  store.AppendEvent(naim::EventRecord{
      0,
      "",
      *node_name,
      "",
      std::nullopt,
      std::nullopt,
      "host-registry",
      "registered",
      "info",
      "registered hostd node",
      "{\"source\":\"naim-node connect-hostd\"}",
      "",
  });
  std::cout << "registered hostd node=" << *node_name << "\n";
}

std::string HostdRegistrationService::ReadPublicKeyBase64Argument(const std::string& value) const {
  const fs::path candidate(value);
  if (fs::exists(candidate)) {
    return Trim(ReadTextFile(candidate));
  }
  return value;
}

std::string HostdRegistrationService::ReadTextFile(const fs::path& path) const {
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("failed to read file '" + path.string() + "'");
  }
  std::ostringstream buffer;
  buffer << input.rdbuf();
  return buffer.str();
}

std::string HostdRegistrationService::Trim(const std::string& value) const {
  std::size_t begin = 0;
  while (begin < value.size() && std::isspace(static_cast<unsigned char>(value[begin])) != 0) {
    ++begin;
  }
  std::size_t end = value.size();
  while (end > begin && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(begin, end - begin);
}

}  // namespace naim::launcher
