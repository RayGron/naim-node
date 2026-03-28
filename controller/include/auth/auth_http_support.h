#pragma once

#include <map>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "auth/auth_payload_service.h"
#include "auth/auth_pending_flows.h"
#include "auth/auth_support_service.h"
#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"

#include "comet/state/models.h"
#include "comet/state/sqlite_store.h"

class AuthHttpSupport final {
 public:
  explicit AuthHttpSupport(AuthSupportService& auth_support);

  HttpResponse BuildJsonResponse(
      int status_code,
      const nlohmann::json& payload,
      const std::map<std::string, std::string>& headers) const;
  nlohmann::json BuildUserPayload(const comet::UserRecord& user) const;
  nlohmann::json BuildInvitePayload(const comet::RegistrationInviteRecord& invite) const;
  nlohmann::json BuildSshKeyPayload(const comet::UserSshKeyRecord& ssh_key) const;
  std::optional<std::pair<comet::UserRecord, comet::AuthSessionRecord>>
  AuthenticateControllerUserSession(
      comet::ControllerStore& store,
      const HttpRequest& request) const;
  std::optional<comet::UserRecord> RequireControllerAdminUser(
      comet::ControllerStore& store,
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
      comet::ControllerStore& store,
      int user_id,
      const std::string& session_kind,
      const std::string& plane_name) const;
  std::string UtcNowSqlTimestamp() const;
  std::string SqlTimestampAfterSeconds(int seconds) const;
  std::string Trim(const std::string& value) const;
  std::string BuildSshChallengeMessage(
      const std::string& username,
      const std::string& plane_name,
      const std::string& challenge_token,
      const std::string& expires_at) const;
  std::string SanitizeTokenForPath(const std::string& value) const;
  std::string ComputeSshPublicKeyFingerprint(const std::string& public_key) const;
  bool VerifySshDetachedSignature(
      const std::string& username,
      const std::string& public_key,
      const std::string& message,
      const std::string& signature) const;
  std::optional<PendingWebAuthnFlow> LoadPendingWebAuthnFlow(const std::string& flow_id) const;
  void SavePendingWebAuthnFlow(const PendingWebAuthnFlow& flow) const;
  void ErasePendingWebAuthnFlow(const std::string& flow_id) const;
  std::optional<PendingSshChallenge> LoadPendingSshChallenge(
      const std::string& challenge_id) const;
  void SavePendingSshChallenge(const PendingSshChallenge& challenge) const;
  void ErasePendingSshChallenge(const std::string& challenge_id) const;

  HttpResponse build_json_response(
      int status_code,
      const nlohmann::json& payload,
      const std::map<std::string, std::string>& headers) const {
    return BuildJsonResponse(status_code, payload, headers);
  }
  nlohmann::json build_user_payload(const comet::UserRecord& user) const {
    return BuildUserPayload(user);
  }
  nlohmann::json build_invite_payload(
      const comet::RegistrationInviteRecord& invite) const {
    return BuildInvitePayload(invite);
  }
  nlohmann::json build_ssh_key_payload(
      const comet::UserSshKeyRecord& ssh_key) const {
    return BuildSshKeyPayload(ssh_key);
  }
  std::optional<std::pair<comet::UserRecord, comet::AuthSessionRecord>>
  authenticate_controller_user_session(
      comet::ControllerStore& store,
      const HttpRequest& request) const {
    return AuthenticateControllerUserSession(store, request);
  }
  std::optional<comet::UserRecord> require_controller_admin_user(
      comet::ControllerStore& store,
      const HttpRequest& request) const {
    return RequireControllerAdminUser(store, request);
  }
  std::string resolve_webauthn_rp_id(const HttpRequest& request) const {
    return ResolveWebAuthnRpId(request);
  }
  std::string resolve_webauthn_origin(const HttpRequest& request) const {
    return ResolveWebAuthnOrigin(request);
  }
  std::string resolve_webauthn_rp_name() const {
    return ResolveWebAuthnRpName();
  }
  nlohmann::json run_webauthn_helper(
      const std::string& action,
      const nlohmann::json& payload) const {
    return RunWebAuthnHelper(action, payload);
  }
  std::string session_cookie_header(
      const std::string& token,
      const HttpRequest& request) const {
    return SessionCookieHeader(token, request);
  }
  std::string clear_session_cookie_header(const HttpRequest& request) const {
    return ClearSessionCookieHeader(request);
  }
  std::string create_controller_session(
      comet::ControllerStore& store,
      int user_id,
      const std::string& session_kind,
      const std::string& plane_name) const {
    return CreateControllerSession(store, user_id, session_kind, plane_name);
  }
  std::string utc_now_sql_timestamp() const {
    return UtcNowSqlTimestamp();
  }
  std::string sql_timestamp_after_seconds(int seconds) const {
    return SqlTimestampAfterSeconds(seconds);
  }
  std::string trim(const std::string& value) const {
    return Trim(value);
  }
  std::string build_ssh_challenge_message(
      const std::string& username,
      const std::string& plane_name,
      const std::string& challenge_token,
      const std::string& expires_at) const {
    return BuildSshChallengeMessage(
        username,
        plane_name,
        challenge_token,
        expires_at);
  }
  std::string sanitize_token_for_path(const std::string& value) const {
    return SanitizeTokenForPath(value);
  }
  std::string compute_ssh_public_key_fingerprint(
      const std::string& public_key) const {
    return ComputeSshPublicKeyFingerprint(public_key);
  }
  bool verify_ssh_detached_signature(
      const std::string& username,
      const std::string& public_key,
      const std::string& message,
      const std::string& signature) const {
    return VerifySshDetachedSignature(username, public_key, message, signature);
  }
  std::optional<PendingWebAuthnFlow> load_pending_webauthn_flow(
      const std::string& flow_id) const {
    return LoadPendingWebAuthnFlow(flow_id);
  }
  void save_pending_webauthn_flow(const PendingWebAuthnFlow& flow) const {
    SavePendingWebAuthnFlow(flow);
  }
  void erase_pending_webauthn_flow(const std::string& flow_id) const {
    ErasePendingWebAuthnFlow(flow_id);
  }
  std::optional<PendingSshChallenge> load_pending_ssh_challenge(
      const std::string& challenge_id) const {
    return LoadPendingSshChallenge(challenge_id);
  }
  void save_pending_ssh_challenge(const PendingSshChallenge& challenge) const {
    SavePendingSshChallenge(challenge);
  }
  void erase_pending_ssh_challenge(const std::string& challenge_id) const {
    ErasePendingSshChallenge(challenge_id);
  }

 private:
  AuthSupportService& auth_support_;
  comet::controller::AuthPayloadService auth_payload_service_;
};
