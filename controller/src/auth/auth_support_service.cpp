#include "auth/auth_support_service.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "auth/webauthn_service.h"
#include "naim/security/crypto_utils.h"

using nlohmann::json;

namespace {

constexpr int kBrowserSessionLifetimeSeconds = 12 * 60 * 60;
constexpr int kSshSessionLifetimeSeconds = 60 * 60;

std::string TrimCopy(const std::string& value) {
  const auto begin = value.find_first_not_of(" \t\r\n");
  if (begin == std::string::npos) {
    return "";
  }
  const auto end = value.find_last_not_of(" \t\r\n");
  return value.substr(begin, end - begin + 1);
}

std::string LowercaseCopy(std::string value) {
  std::transform(
      value.begin(),
      value.end(),
      value.begin(),
      [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return value;
}

std::optional<std::string> GetEnvString(const char* key) {
  if (const char* value = std::getenv(key); value != nullptr && value[0] != '\0') {
    return std::string(value);
  }
  return std::nullopt;
}

std::string UtcNowSqlTimestamp() {
  const auto now = std::chrono::system_clock::now();
  const std::time_t raw_time = std::chrono::system_clock::to_time_t(now);
  std::tm utc_time{};
#if defined(_WIN32)
  gmtime_s(&utc_time, &raw_time);
#else
  gmtime_r(&raw_time, &utc_time);
#endif
  std::ostringstream out;
  out << std::put_time(&utc_time, "%Y-%m-%d %H:%M:%S");
  return out.str();
}

std::string SqlTimestampAfterSeconds(int seconds) {
  const auto then = std::chrono::system_clock::now() + std::chrono::seconds(seconds);
  const std::time_t raw_time = std::chrono::system_clock::to_time_t(then);
  std::tm utc_time{};
#if defined(_WIN32)
  gmtime_s(&utc_time, &raw_time);
#else
  gmtime_r(&raw_time, &utc_time);
#endif
  std::ostringstream out;
  out << std::put_time(&utc_time, "%Y-%m-%d %H:%M:%S");
  return out.str();
}

std::optional<std::string> FindHeaderString(
    const HttpRequest& request,
    const std::string& key) {
  const auto it = request.headers.find(LowercaseCopy(key));
  if (it == request.headers.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<std::string> FindCookieValue(
    const HttpRequest& request,
    const std::string& name) {
  const auto cookie_header = FindHeaderString(request, "cookie");
  if (!cookie_header.has_value()) {
    return std::nullopt;
  }
  std::istringstream stream(*cookie_header);
  std::string item;
  while (std::getline(stream, item, ';')) {
    const auto eq = item.find('=');
    if (eq == std::string::npos) {
      continue;
    }
    const std::string cookie_name = TrimCopy(item.substr(0, eq));
    if (cookie_name != name) {
      continue;
    }
    return TrimCopy(item.substr(eq + 1));
  }
  return std::nullopt;
}

std::optional<std::string> FindBearerToken(const HttpRequest& request) {
  const auto authorization = FindHeaderString(request, "authorization");
  if (!authorization.has_value()) {
    return std::nullopt;
  }
  const std::string lowered = LowercaseCopy(*authorization);
  if (lowered.rfind("bearer ", 0) != 0) {
    return std::nullopt;
  }
  return TrimCopy(authorization->substr(7));
}

std::optional<std::string> FindControllerSessionToken(const HttpRequest& request) {
  if (const auto header_token = FindHeaderString(request, "x-naim-session-token");
      header_token.has_value()) {
    return header_token;
  }
  if (const auto cookie_token = FindCookieValue(request, "naim.sid"); cookie_token.has_value()) {
    return cookie_token;
  }
  return FindBearerToken(request);
}

bool RunCommand(const std::string& command) {
  const int rc = std::system(command.c_str());
  return rc == 0;
}

std::string EscapeShellArg(const std::string& value) {
  std::string escaped = "'";
  for (char ch : value) {
    if (ch == '\'') {
      escaped += "'\\''";
    } else {
      escaped.push_back(ch);
    }
  }
  escaped.push_back('\'');
  return escaped;
}

std::string RequestHostHeader(const HttpRequest& request) {
  if (const auto forwarded = FindHeaderString(request, "x-forwarded-host"); forwarded.has_value()) {
    return *forwarded;
  }
  if (const auto host = FindHeaderString(request, "host"); host.has_value()) {
    return *host;
  }
  return "127.0.0.1:18081";
}

std::string HostWithoutPort(const std::string& host_header) {
  if (host_header.empty()) {
    return host_header;
  }
  if (host_header.front() == '[') {
    const auto close = host_header.find(']');
    return close == std::string::npos ? host_header : host_header.substr(1, close - 1);
  }
  const auto colon = host_header.find(':');
  return colon == std::string::npos ? host_header : host_header.substr(0, colon);
}

std::string RequestScheme(const HttpRequest& request) {
  if (const auto forwarded = FindHeaderString(request, "x-forwarded-proto"); forwarded.has_value()) {
    const auto first = TrimCopy(
        forwarded->substr(0, forwarded->find(',')));
    if (!first.empty()) {
      return LowercaseCopy(first);
    }
  }
  if (const auto forwarded = FindHeaderString(request, "forwarded"); forwarded.has_value()) {
    const std::string lowered = LowercaseCopy(*forwarded);
    const auto proto_pos = lowered.find("proto=");
    if (proto_pos != std::string::npos) {
      const auto value_begin = proto_pos + 6;
      const auto value_end = lowered.find_first_of(";,\t ", value_begin);
      const auto value = TrimCopy(lowered.substr(value_begin, value_end - value_begin));
      if (!value.empty()) {
        return value;
      }
    }
  }
  if (const auto forwarded = FindHeaderString(request, "x-forwarded-ssl"); forwarded.has_value()) {
    if (LowercaseCopy(TrimCopy(*forwarded)) == "on") {
      return "https";
    }
  }
  if (const auto forwarded = FindHeaderString(request, "x-forwarded-port"); forwarded.has_value()) {
    if (TrimCopy(*forwarded) == "443") {
      return "https";
    }
  }
  return "http";
}

}  // namespace

std::optional<std::pair<naim::UserRecord, naim::AuthSessionRecord>>
AuthSupportService::AuthenticateControllerUserSession(
    naim::ControllerStore& store,
    const HttpRequest& request,
    const std::optional<std::string>& session_kind) const {
  const auto token = FindControllerSessionToken(request);
  if (!token.has_value()) {
    return std::nullopt;
  }
  const auto session = store.LoadActiveAuthSession(*token, session_kind);
  if (!session.has_value()) {
    return std::nullopt;
  }
  const auto user = store.LoadUserById(session->user_id);
  if (!user.has_value()) {
    return std::nullopt;
  }
  store.TouchAuthSession(session->token, UtcNowSqlTimestamp());
  return std::make_pair(*user, *session);
}

std::optional<naim::UserRecord> AuthSupportService::RequireControllerAdminUser(
    naim::ControllerStore& store,
    const HttpRequest& request) const {
  const auto session = AuthenticateControllerUserSession(store, request);
  if (!session.has_value() || session->first.role != "admin") {
    return std::nullopt;
  }
  return session->first;
}

std::string AuthSupportService::ResolveWebAuthnRpId(const HttpRequest& request) const {
  if (const auto env = GetEnvString("NAIM_WEBAUTHN_RP_ID"); env.has_value()) {
    return *env;
  }
  return HostWithoutPort(RequestHostHeader(request));
}

std::string AuthSupportService::ResolveWebAuthnOrigin(const HttpRequest& request) const {
  if (const auto env = GetEnvString("NAIM_WEBAUTHN_ORIGIN"); env.has_value()) {
    return *env;
  }
  return RequestScheme(request) + "://" + RequestHostHeader(request);
}

std::string AuthSupportService::ResolveWebAuthnRpName() const {
  return GetEnvString("NAIM_WEBAUTHN_RP_NAME").value_or("naim-node");
}

json AuthSupportService::RunWebAuthnHelper(
    const std::string& action,
    const json& payload) const {
  return naim::controller::auth::WebAuthnService().Run(action, payload);
}

std::string AuthSupportService::SessionCookieHeader(
    const std::string& token,
    const HttpRequest& request) const {
  std::string value = "naim.sid=" + token + "; Path=/; HttpOnly; SameSite=Strict";
  if (RequestScheme(request) == "https") {
    value += "; Secure";
  }
  return value;
}

std::string AuthSupportService::ClearSessionCookieHeader(const HttpRequest& request) const {
  std::string value =
      "naim.sid=; Path=/; HttpOnly; SameSite=Strict; Expires=Thu, 01 Jan 1970 00:00:00 GMT";
  if (RequestScheme(request) == "https") {
    value += "; Secure";
  }
  return value;
}

std::string AuthSupportService::CreateControllerSession(
    naim::ControllerStore& store,
    int user_id,
    const std::string& session_kind,
    const std::string& plane_name) const {
  const std::string now = UtcNowSqlTimestamp();
  const std::string token = naim::RandomTokenBase64(32);
  store.InsertAuthSession(naim::AuthSessionRecord{
      token,
      user_id,
      session_kind,
      plane_name,
      SqlTimestampAfterSeconds(
          session_kind == "ssh" ? kSshSessionLifetimeSeconds
                                : kBrowserSessionLifetimeSeconds),
      now,
      "",
      now,
  });
  store.UpdateUserLastLoginAt(user_id, now);
  return token;
}

std::string AuthSupportService::BuildSshChallengeMessage(
    const std::string& username,
    const std::string& plane_name,
    const std::string& challenge_token,
    const std::string& expires_at) const {
  return "naim-plane-auth\n" + username + "\n" + plane_name + "\n" +
         challenge_token + "\n" + expires_at + "\n";
}

std::string AuthSupportService::SanitizeTokenForPath(const std::string& value) const {
  std::string out;
  out.reserve(value.size());
  for (unsigned char ch : value) {
    if (std::isalnum(ch) || ch == '-' || ch == '_') {
      out.push_back(static_cast<char>(ch));
    } else {
      out.push_back('_');
    }
  }
  return out;
}

std::string AuthSupportService::ComputeSshPublicKeyFingerprint(
    const std::string& public_key) const {
  const std::filesystem::path temp_dir =
      std::filesystem::temp_directory_path() /
      ("naim-ssh-key-" + SanitizeTokenForPath(naim::RandomTokenBase64(12)));
  std::filesystem::create_directories(temp_dir);
  const std::filesystem::path key_path = temp_dir / "key.pub";
  try {
    {
      std::ofstream out(key_path);
      out << public_key << "\n";
    }
    const std::string command =
        "ssh-keygen -lf " + EscapeShellArg(key_path.string()) + " 2>/dev/null";
    std::array<char, 512> buffer{};
    std::string output;
    FILE* pipe = popen(command.c_str(), "r");
    if (pipe == nullptr) {
      throw std::runtime_error("failed to spawn ssh-keygen for fingerprint");
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
      output += buffer.data();
    }
    const int rc = pclose(pipe);
    if (rc != 0 || output.empty()) {
      throw std::runtime_error("ssh-keygen rejected SSH public key");
    }
    std::istringstream input(output);
    std::string bits;
    std::string fingerprint;
    input >> bits >> fingerprint;
    if (fingerprint.empty()) {
      throw std::runtime_error("failed to parse SSH key fingerprint");
    }
    std::filesystem::remove_all(temp_dir);
    return fingerprint;
  } catch (...) {
    std::filesystem::remove_all(temp_dir);
    throw;
  }
}

bool AuthSupportService::VerifySshDetachedSignature(
    const std::string& username,
    const std::string& public_key,
    const std::string& message,
    const std::string& signature) const {
  const std::filesystem::path temp_dir =
      std::filesystem::temp_directory_path() /
      ("naim-ssh-verify-" + SanitizeTokenForPath(naim::RandomTokenBase64(12)));
  std::filesystem::create_directories(temp_dir);
  const std::filesystem::path allowed_signers_path = temp_dir / "allowed_signers";
  const std::filesystem::path message_path = temp_dir / "message.txt";
  const std::filesystem::path signature_path = temp_dir / "signature.txt";
  try {
    {
      std::ofstream out(allowed_signers_path);
      out << username << " " << public_key << "\n";
    }
    {
      std::ofstream out(message_path);
      out << message;
    }
    {
      std::ofstream out(signature_path);
      out << signature;
    }
    const std::string command =
        "ssh-keygen -Y verify"
        " -f " + EscapeShellArg(allowed_signers_path.string()) +
        " -I " + EscapeShellArg(username) +
        " -n " + EscapeShellArg("naim-plane-auth") +
        " -s " + EscapeShellArg(signature_path.string()) +
        " < " + EscapeShellArg(message_path.string()) +
        " >/dev/null 2>/dev/null";
    const bool verified = RunCommand(command);
    std::filesystem::remove_all(temp_dir);
    return verified;
  } catch (...) {
    std::filesystem::remove_all(temp_dir);
    throw;
  }
}

std::optional<PendingWebAuthnFlow> AuthSupportService::LoadPendingWebAuthnFlow(
    const std::string& flow_id) const {
  std::lock_guard<std::mutex> lock(pending_webauthn_mutex_);
  const auto it = pending_webauthn_flows_.find(flow_id);
  if (it == pending_webauthn_flows_.end()) {
    return std::nullopt;
  }
  return it->second;
}

void AuthSupportService::SavePendingWebAuthnFlow(const PendingWebAuthnFlow& flow) const {
  std::lock_guard<std::mutex> lock(pending_webauthn_mutex_);
  pending_webauthn_flows_[flow.flow_id] = flow;
}

void AuthSupportService::ErasePendingWebAuthnFlow(const std::string& flow_id) const {
  std::lock_guard<std::mutex> lock(pending_webauthn_mutex_);
  pending_webauthn_flows_.erase(flow_id);
}

std::optional<PendingSshChallenge> AuthSupportService::LoadPendingSshChallenge(
    const std::string& challenge_id) const {
  std::lock_guard<std::mutex> lock(pending_ssh_mutex_);
  const auto it = pending_ssh_challenges_.find(challenge_id);
  if (it == pending_ssh_challenges_.end()) {
    return std::nullopt;
  }
  return it->second;
}

void AuthSupportService::SavePendingSshChallenge(
    const PendingSshChallenge& challenge) const {
  std::lock_guard<std::mutex> lock(pending_ssh_mutex_);
  pending_ssh_challenges_[challenge.challenge_id] = challenge;
}

void AuthSupportService::ErasePendingSshChallenge(
    const std::string& challenge_id) const {
  std::lock_guard<std::mutex> lock(pending_ssh_mutex_);
  pending_ssh_challenges_.erase(challenge_id);
}

void AuthSupportService::CleanupExpiredPendingAuthFlows() const {
  const std::string now = UtcNowSqlTimestamp();
  {
    std::lock_guard<std::mutex> lock(pending_webauthn_mutex_);
    for (auto it = pending_webauthn_flows_.begin(); it != pending_webauthn_flows_.end();) {
      if (it->second.expires_at < now) {
        it = pending_webauthn_flows_.erase(it);
      } else {
        ++it;
      }
    }
  }
  {
    std::lock_guard<std::mutex> lock(pending_ssh_mutex_);
    for (auto it = pending_ssh_challenges_.begin(); it != pending_ssh_challenges_.end();) {
      if (it->second.expires_at < now) {
        it = pending_ssh_challenges_.erase(it);
      } else {
        ++it;
      }
    }
  }
}

std::optional<std::pair<naim::UserRecord, naim::AuthSessionRecord>>
AuthSupportService::AuthenticateProtectedPlaneRequest(
    naim::ControllerStore& store,
    const HttpRequest& request,
    const std::string& plane_name) const {
  if (const auto web_session =
          AuthenticateControllerUserSession(
              store, request, std::optional<std::string>("web"));
      web_session.has_value()) {
    return web_session;
  }
  const auto token = FindControllerSessionToken(request);
  if (!token.has_value()) {
    return std::nullopt;
  }
  const auto ssh_session =
      store.LoadActiveAuthSession(*token, std::optional<std::string>("ssh"), plane_name);
  if (!ssh_session.has_value()) {
    return std::nullopt;
  }
  const auto user = store.LoadUserById(ssh_session->user_id);
  if (!user.has_value()) {
    return std::nullopt;
  }
  store.TouchAuthSession(ssh_session->token, UtcNowSqlTimestamp());
  return std::make_pair(*user, *ssh_session);
}
