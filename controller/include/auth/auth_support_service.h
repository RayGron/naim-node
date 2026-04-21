#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

#include <nlohmann/json.hpp>

#include "auth/auth_pending_flows.h"
#include "http/controller_http_types.h"

#include "naim/state/models.h"
#include "naim/state/sqlite_store.h"

class AuthSupportService {
 public:
  std::optional<std::pair<naim::UserRecord, naim::AuthSessionRecord>>
  AuthenticateControllerUserSession(
      naim::ControllerStore& store,
      const HttpRequest& request,
      const std::optional<std::string>& session_kind =
          std::optional<std::string>("web")) const;

  std::optional<naim::UserRecord> RequireControllerAdminUser(
      naim::ControllerStore& store,
      const HttpRequest& request) const;

  std::string ResolveWebAuthnRpId(const HttpRequest& request) const;
  std::string ResolveWebAuthnOrigin(const HttpRequest& request) const;
  std::string ResolveWebAuthnRpName() const;
  nlohmann::json RunWebAuthnHelper(
      const std::string& action,
      const nlohmann::json& payload) const;
  std::string SessionCookieHeader(
      const std::string& token,
      const HttpRequest& request) const;
  std::string ClearSessionCookieHeader(const HttpRequest& request) const;
  std::string CreateControllerSession(
      naim::ControllerStore& store,
      int user_id,
      const std::string& session_kind,
      const std::string& plane_name = "") const;
  std::string BuildSshChallengeMessage(
      const std::string& username,
      const std::string& plane_name,
      const std::string& challenge_token,
      const std::string& expires_at) const;
  std::string SanitizeTokenForPath(const std::string& value) const;
  std::string ComputeSshPublicKeyFingerprint(
      const std::string& public_key) const;
  bool VerifySshDetachedSignature(
      const std::string& username,
      const std::string& public_key,
      const std::string& message,
      const std::string& signature) const;

  std::optional<PendingWebAuthnFlow> LoadPendingWebAuthnFlow(
      const std::string& flow_id) const;
  void SavePendingWebAuthnFlow(const PendingWebAuthnFlow& flow) const;
  void ErasePendingWebAuthnFlow(const std::string& flow_id) const;

  std::optional<PendingSshChallenge> LoadPendingSshChallenge(
      const std::string& challenge_id) const;
  void SavePendingSshChallenge(const PendingSshChallenge& challenge) const;
  void ErasePendingSshChallenge(const std::string& challenge_id) const;

  void CleanupExpiredPendingAuthFlows() const;

  std::optional<std::pair<naim::UserRecord, naim::AuthSessionRecord>>
  AuthenticateProtectedPlaneRequest(
      naim::ControllerStore& store,
      const HttpRequest& request,
      const std::string& plane_name) const;

 private:
  mutable std::mutex pending_webauthn_mutex_;
  mutable std::map<std::string, PendingWebAuthnFlow> pending_webauthn_flows_;
  mutable std::mutex pending_ssh_mutex_;
  mutable std::map<std::string, PendingSshChallenge> pending_ssh_challenges_;
};
