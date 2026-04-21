#include "auth/auth_http_support.h"

#include "app/controller_composition_support.h"

AuthHttpSupport::AuthHttpSupport(AuthSupportService& auth_support) : auth_support_(auth_support) {}

HttpResponse AuthHttpSupport::BuildJsonResponse(
    int status_code,
    const nlohmann::json& payload,
    const std::map<std::string, std::string>& headers) const {
  return naim::controller::composition_support::BuildJsonResponse(
      status_code,
      payload,
      headers);
}

nlohmann::json AuthHttpSupport::BuildUserPayload(const naim::UserRecord& user) const {
  return auth_payload_service_.BuildUserPayload(user);
}

nlohmann::json AuthHttpSupport::BuildInvitePayload(
    const naim::RegistrationInviteRecord& invite) const {
  return auth_payload_service_.BuildInvitePayload(invite);
}

nlohmann::json AuthHttpSupport::BuildSshKeyPayload(
    const naim::UserSshKeyRecord& ssh_key) const {
  return auth_payload_service_.BuildSshKeyPayload(ssh_key);
}

std::optional<std::pair<naim::UserRecord, naim::AuthSessionRecord>>
AuthHttpSupport::AuthenticateControllerUserSession(
    naim::ControllerStore& store,
    const HttpRequest& request) const {
  return auth_support_.AuthenticateControllerUserSession(
      store,
      request,
      std::optional<std::string>("web"));
}

std::optional<naim::UserRecord> AuthHttpSupport::RequireControllerAdminUser(
    naim::ControllerStore& store,
    const HttpRequest& request) const {
  return auth_support_.RequireControllerAdminUser(store, request);
}

std::string AuthHttpSupport::ResolveWebAuthnRpId(const HttpRequest& request) const {
  return auth_support_.ResolveWebAuthnRpId(request);
}

std::string AuthHttpSupport::ResolveWebAuthnOrigin(const HttpRequest& request) const {
  return auth_support_.ResolveWebAuthnOrigin(request);
}

std::string AuthHttpSupport::ResolveWebAuthnRpName() const {
  return auth_support_.ResolveWebAuthnRpName();
}

nlohmann::json AuthHttpSupport::RunWebAuthnHelper(
    const std::string& action,
    const nlohmann::json& payload) const {
  return auth_support_.RunWebAuthnHelper(action, payload);
}

std::string AuthHttpSupport::SessionCookieHeader(
    const std::string& token,
    const HttpRequest& request) const {
  return auth_support_.SessionCookieHeader(token, request);
}

std::string AuthHttpSupport::ClearSessionCookieHeader(const HttpRequest& request) const {
  return auth_support_.ClearSessionCookieHeader(request);
}

std::string AuthHttpSupport::CreateControllerSession(
    naim::ControllerStore& store,
    int user_id,
    const std::string& session_kind,
    const std::string& plane_name) const {
  return auth_support_.CreateControllerSession(store, user_id, session_kind, plane_name);
}

std::string AuthHttpSupport::UtcNowSqlTimestamp() const {
  return naim::controller::ControllerTimeSupport::UtcNowSqlTimestamp();
}

std::string AuthHttpSupport::SqlTimestampAfterSeconds(int seconds) const {
  return naim::controller::ControllerTimeSupport::SqlTimestampAfterSeconds(seconds);
}

std::string AuthHttpSupport::Trim(const std::string& value) const {
  return naim::controller::composition_support::Trim(value);
}

std::string AuthHttpSupport::BuildSshChallengeMessage(
    const std::string& username,
    const std::string& plane_name,
    const std::string& challenge_token,
    const std::string& expires_at) const {
  return auth_support_.BuildSshChallengeMessage(
      username,
      plane_name,
      challenge_token,
      expires_at);
}

std::string AuthHttpSupport::SanitizeTokenForPath(const std::string& value) const {
  return auth_support_.SanitizeTokenForPath(value);
}

std::string AuthHttpSupport::ComputeSshPublicKeyFingerprint(
    const std::string& public_key) const {
  return auth_support_.ComputeSshPublicKeyFingerprint(public_key);
}

bool AuthHttpSupport::VerifySshDetachedSignature(
    const std::string& username,
    const std::string& public_key,
    const std::string& message,
    const std::string& signature) const {
  return auth_support_.VerifySshDetachedSignature(username, public_key, message, signature);
}

std::optional<PendingWebAuthnFlow> AuthHttpSupport::LoadPendingWebAuthnFlow(
    const std::string& flow_id) const {
  return auth_support_.LoadPendingWebAuthnFlow(flow_id);
}

void AuthHttpSupport::SavePendingWebAuthnFlow(const PendingWebAuthnFlow& flow) const {
  auth_support_.SavePendingWebAuthnFlow(flow);
}

void AuthHttpSupport::ErasePendingWebAuthnFlow(const std::string& flow_id) const {
  auth_support_.ErasePendingWebAuthnFlow(flow_id);
}

std::optional<PendingSshChallenge> AuthHttpSupport::LoadPendingSshChallenge(
    const std::string& challenge_id) const {
  return auth_support_.LoadPendingSshChallenge(challenge_id);
}

void AuthHttpSupport::SavePendingSshChallenge(const PendingSshChallenge& challenge) const {
  auth_support_.SavePendingSshChallenge(challenge);
}

void AuthHttpSupport::ErasePendingSshChallenge(const std::string& challenge_id) const {
  auth_support_.ErasePendingSshChallenge(challenge_id);
}
