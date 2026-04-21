#include "backend/hostd_backend_factory.h"

#include <fstream>
#include <sstream>
#include <stdexcept>

#include "backend/http_hostd_backend.h"
#include "backend/local_db_hostd_backend.h"

namespace {

std::string DefaultDbPath() {
  return (std::filesystem::path("var") / "controller.sqlite").string();
}

}  // namespace

namespace naim::hostd {

HostdBackendFactory::HostdBackendFactory(const IHttpHostdBackendSupport& support)
    : support_(support) {}

std::unique_ptr<HostdBackend> HostdBackendFactory::CreateBackend(
    const std::optional<std::string>& db_path,
    const std::optional<std::string>& controller_url,
    const std::optional<std::string>& host_private_key_path,
    const std::optional<std::string>& controller_fingerprint,
    const std::optional<std::string>& onboarding_key,
    const std::string& node_name,
    const std::string& storage_root) const {
  if (controller_url.has_value() && !controller_url->empty()) {
    if (!host_private_key_path.has_value() || host_private_key_path->empty()) {
      throw std::runtime_error("--host-private-key is required for remote host-agent mode");
    }
    std::ifstream input(*host_private_key_path);
    if (!input.is_open()) {
      throw std::runtime_error("failed to read host private key '" + *host_private_key_path + "'");
    }
    std::stringstream buffer;
    buffer << input.rdbuf();
    return std::make_unique<HttpHostdBackend>(
        *controller_url,
        support_.Trim(buffer.str()),
        controller_fingerprint.value_or(""),
        onboarding_key.value_or(""),
        node_name,
        storage_root,
        support_);
  }
  return std::make_unique<LocalDbHostdBackend>(db_path.value_or(DefaultDbPath()));
}

}  // namespace naim::hostd
