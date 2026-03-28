#include "auth/auth_http_service.h"

#include <utility>

#include "comet/security/crypto_utils.h"

using nlohmann::json;

namespace {

json ParseJsonBody(const HttpRequest& request) {
  if (request.body.empty()) {
    return json::object();
  }
  return json::parse(request.body);
}

bool StartsWithPathPrefix(const std::string& path, const std::string& prefix) {
  return path.rfind(prefix, 0) == 0;
}

constexpr int InviteLifetimeSeconds() {
  return 60 * 60;
}

constexpr int SshChallengeLifetimeSeconds() {
  return 5 * 60;
}

constexpr int SshSessionLifetimeSeconds() {
  return 60 * 60;
}

}  // namespace

AuthHttpService::AuthHttpService(AuthHttpSupport support) : support_(std::move(support)) {}

std::optional<HttpResponse> AuthHttpService::HandleRequest(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (!StartsWithPathPrefix(request.path, "/api/v1/auth/")) {
    return std::nullopt;
  }
  if (request.path == "/api/v1/auth/state") {
    return HandleState(db_path, request);
  }
  if (request.path == "/api/v1/auth/me") {
    return HandleMe(db_path, request);
  }
  if (request.path == "/api/v1/auth/logout") {
    return HandleLogout(request);
  }
  if (request.path == "/api/v1/auth/bootstrap/begin") {
    return HandleBootstrapBegin(db_path, request);
  }
  if (request.path == "/api/v1/auth/bootstrap/finish") {
    return HandleBootstrapFinish(db_path, request);
  }
  if (request.path == "/api/v1/auth/login/begin") {
    return HandleLoginBegin(db_path, request);
  }
  if (request.path == "/api/v1/auth/login/finish") {
    return HandleLoginFinish(db_path, request);
  }
  if (StartsWithPathPrefix(request.path, "/api/v1/auth/invite/")) {
    return HandleInviteLookup(db_path, request);
  }
  if (request.path == "/api/v1/auth/register/begin") {
    return HandleRegisterBegin(db_path, request);
  }
  if (request.path == "/api/v1/auth/register/finish") {
    return HandleRegisterFinish(db_path, request);
  }
  if (request.path == "/api/v1/auth/invites") {
    return HandleInvites(db_path, request);
  }
  if (StartsWithPathPrefix(request.path, "/api/v1/auth/invites/")) {
    return HandleInviteDelete(db_path, request);
  }
  if (request.path == "/api/v1/auth/ssh-keys") {
    return HandleSshKeys(db_path, request);
  }
  if (StartsWithPathPrefix(request.path, "/api/v1/auth/ssh-keys/")) {
    return HandleSshKeyDelete(db_path, request);
  }
  if (request.path == "/api/v1/auth/ssh/challenge") {
    return HandleSshChallenge(db_path, request);
  }
  if (request.path == "/api/v1/auth/ssh/verify") {
    return HandleSshVerify(db_path, request);
  }
  return support_.build_json_response(404, json{{"status", "not_found"}}, {});
}

HttpResponse AuthHttpService::HandleState(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "GET") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto session =
        support_.authenticate_controller_user_session(store, request);
    return support_.build_json_response(
        200,
        json{
            {"service", "comet-controller"},
            {"setup_required", store.LoadUserCount() == 0},
            {"authenticated", session.has_value()},
            {"user",
             session.has_value() ? support_.build_user_payload(session->first)
                                 : json(nullptr)},
            {"rp_id", support_.resolve_webauthn_rp_id(request)},
            {"origin", support_.resolve_webauthn_origin(request)},
        },
        {});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500, json{{"status", "internal_error"}, {"message", error.what()}}, {});
  }
}

HttpResponse AuthHttpService::HandleMe(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "GET") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto session =
        support_.authenticate_controller_user_session(store, request);
    if (!session.has_value()) {
      return support_.build_json_response(
          401,
          json{{"status", "unauthorized"},
               {"message", "authentication required"}},
          {{"Set-Cookie", support_.clear_session_cookie_header(request)}});
    }
    return support_.build_json_response(
        200, json{{"user", support_.build_user_payload(session->first)}}, {});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500, json{{"status", "internal_error"}, {"message", error.what()}}, {});
  }
}

HttpResponse AuthHttpService::HandleLogout(const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  return support_.build_json_response(
      200,
      json{{"status", "logged_out"}},
      {{"Set-Cookie", support_.clear_session_cookie_header(request)}});
}

HttpResponse AuthHttpService::HandleBootstrapBegin(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    const json body = ParseJsonBody(request);
    const std::string username = support_.trim(body.value("username", std::string{}));
    const std::string password = body.value("password", std::string{});
    if (username.empty() || password.empty()) {
      return support_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "username and password are required"}},
          {});
    }
    comet::ControllerStore store(db_path);
    store.Initialize();
    if (store.LoadUserCount() != 0) {
      return support_.build_json_response(
          409,
          json{{"status", "conflict"},
               {"message", "bootstrap is only available before the first user is created"}},
          {});
    }
    const std::string flow_id = comet::RandomTokenBase64(24);
    const std::string challenge = comet::RandomTokenBase64(32);
    const PendingWebAuthnFlow flow{
        flow_id,
        "bootstrap",
        username,
        comet::HashPassword(password),
        "",
        0,
        challenge,
        support_.resolve_webauthn_rp_id(request),
        support_.resolve_webauthn_origin(request),
        support_.sql_timestamp_after_seconds(5 * 60),
    };
    const json options = support_.run_webauthn_helper(
        "generate-registration-options",
        json{{"rpName", support_.resolve_webauthn_rp_name()},
             {"rpID", flow.rp_id},
             {"userName", username},
             {"challenge", challenge}});
    support_.save_pending_webauthn_flow(flow);
    return support_.build_json_response(
        200,
        json{{"flow_id", flow_id},
             {"expires_at", flow.expires_at},
             {"options", options}},
        {});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse AuthHttpService::HandleBootstrapFinish(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    const json body = ParseJsonBody(request);
    const std::string flow_id = body.value("flow_id", std::string{});
    if (flow_id.empty() || !body.contains("response")) {
      return support_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "flow_id and response are required"}},
          {});
    }
    const auto flow = support_.load_pending_webauthn_flow(flow_id);
    if (!flow.has_value() || flow->flow_kind != "bootstrap" ||
        flow->expires_at < support_.utc_now_sql_timestamp()) {
      return support_.build_json_response(
          410,
          json{{"status", "expired"},
               {"message", "bootstrap flow is missing or expired"}},
          {});
    }
    const json verification = support_.run_webauthn_helper(
        "verify-registration",
        json{{"response", body.at("response")},
             {"expectedChallenge", flow->challenge},
             {"expectedOrigin", flow->origin},
             {"expectedRPID", flow->rp_id}});
    if (!verification.value("verified", false)) {
      return support_.build_json_response(
          403,
          json{{"status", "forbidden"},
               {"message", "WebAuthn registration verification failed"}},
          {});
    }
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto user =
        store.CreateBootstrapAdmin(flow->username, flow->password_hash);
    const json credential = verification.at("registrationInfo");
    store.InsertWebAuthnCredential(comet::WebAuthnCredentialRecord{
        0,
        user.id,
        credential.value("credentialID", std::string{}),
        credential.value("credentialPublicKey", std::string{}),
        static_cast<std::uint32_t>(credential.value("counter", 0)),
        json(credential.value("transports", json::array())).dump(),
        "",
        "",
        "",
    });
    const std::string session_token =
        support_.create_controller_session(store, user.id, "web", "");
    support_.erase_pending_webauthn_flow(flow_id);
    return support_.build_json_response(
        200,
        json{{"user", support_.build_user_payload(user)}},
        {{"Set-Cookie", support_.session_cookie_header(session_token, request)}});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse AuthHttpService::HandleLoginBegin(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    const json body = ParseJsonBody(request);
    const std::string username = support_.trim(body.value("username", std::string{}));
    if (username.empty()) {
      return support_.build_json_response(
          400,
          json{{"status", "bad_request"}, {"message", "username is required"}},
          {});
    }
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto user = store.LoadUserByUsername(username);
    if (!user.has_value()) {
      return support_.build_json_response(
          404, json{{"status", "not_found"}, {"message", "user not found"}}, {});
    }
    const auto credentials = store.LoadWebAuthnCredentialsForUser(user->id);
    if (credentials.empty()) {
      return support_.build_json_response(
          409,
          json{{"status", "conflict"},
               {"message", "user has no registered WebAuthn credentials"}},
          {});
    }
    json allow_credentials = json::array();
    for (const auto& credential : credentials) {
      json transports = json::array();
      try {
        transports = credential.transports_json.empty()
                         ? json::array()
                         : json::parse(credential.transports_json);
      } catch (...) {
        transports = json::array();
      }
      allow_credentials.push_back(
          json{{"id", credential.credential_id}, {"transports", transports}});
    }
    const std::string flow_id = comet::RandomTokenBase64(24);
    const std::string challenge = comet::RandomTokenBase64(32);
    const PendingWebAuthnFlow flow{
        flow_id,
        "login",
        username,
        "",
        "",
        user->id,
        challenge,
        support_.resolve_webauthn_rp_id(request),
        support_.resolve_webauthn_origin(request),
        support_.sql_timestamp_after_seconds(5 * 60),
    };
    const json options = support_.run_webauthn_helper(
        "generate-authentication-options",
        json{{"rpID", flow.rp_id},
             {"challenge", challenge},
             {"allowCredentials", allow_credentials}});
    support_.save_pending_webauthn_flow(flow);
    return support_.build_json_response(
        200,
        json{{"flow_id", flow_id},
             {"expires_at", flow.expires_at},
             {"options", options}},
        {});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500, json{{"status", "internal_error"}, {"message", error.what()}}, {});
  }
}

HttpResponse AuthHttpService::HandleLoginFinish(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    const json body = ParseJsonBody(request);
    const std::string flow_id = body.value("flow_id", std::string{});
    if (flow_id.empty() || !body.contains("response")) {
      return support_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "flow_id and response are required"}},
          {});
    }
    const auto flow = support_.load_pending_webauthn_flow(flow_id);
    if (!flow.has_value() || flow->flow_kind != "login" ||
        flow->expires_at < support_.utc_now_sql_timestamp()) {
      return support_.build_json_response(
          410,
          json{{"status", "expired"},
               {"message", "login flow is missing or expired"}},
          {});
    }
    comet::ControllerStore store(db_path);
    store.Initialize();
    const std::string credential_id =
        body.at("response").value("id", std::string{});
    const auto credential = store.LoadWebAuthnCredentialById(credential_id);
    if (!credential.has_value() || credential->user_id != flow->user_id) {
      return support_.build_json_response(
          403,
          json{{"status", "forbidden"},
               {"message", "credential is not registered for this user"}},
          {});
    }
    const json verification = support_.run_webauthn_helper(
        "verify-authentication",
        json{
            {"response", body.at("response")},
            {"expectedChallenge", flow->challenge},
            {"expectedOrigin", flow->origin},
            {"expectedRPID", flow->rp_id},
            {"credential",
             {
                 {"id", credential->credential_id},
                 {"publicKey", credential->public_key},
                 {"counter", credential->counter},
                 {"transports",
                  credential->transports_json.empty()
                      ? json::array()
                      : json::parse(credential->transports_json)},
             }},
        });
    if (!verification.value("verified", false)) {
      return support_.build_json_response(
          403,
          json{{"status", "forbidden"},
               {"message", "WebAuthn authentication verification failed"}},
          {});
    }
    const json auth_info = verification.at("authenticationInfo");
    store.UpdateWebAuthnCredentialCounter(
        credential->credential_id,
        static_cast<std::uint32_t>(
            auth_info.value("newCounter", credential->counter)),
        support_.utc_now_sql_timestamp());
    const auto user = store.LoadUserById(flow->user_id);
    if (!user.has_value()) {
      return support_.build_json_response(
          404, json{{"status", "not_found"}, {"message", "user not found"}}, {});
    }
    const std::string session_token =
        support_.create_controller_session(store, user->id, "web", "");
    support_.erase_pending_webauthn_flow(flow_id);
    return support_.build_json_response(
        200,
        json{{"user", support_.build_user_payload(*user)}},
        {{"Set-Cookie", support_.session_cookie_header(session_token, request)}});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse AuthHttpService::HandleInviteLookup(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "GET") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    const std::string token =
        request.path.substr(std::string("/api/v1/auth/invite/").size());
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto invite = store.LoadRegistrationInviteByToken(token);
    const bool valid = invite.has_value() && invite->revoked_at.empty() &&
                       invite->used_at.empty() &&
                       invite->expires_at >= support_.utc_now_sql_timestamp();
    return support_.build_json_response(
        valid ? 200 : 404,
        json{{"valid", valid},
             {"invite",
              valid ? support_.build_invite_payload(*invite) : json(nullptr)}},
        {});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500, json{{"status", "internal_error"}, {"message", error.what()}}, {});
  }
}

HttpResponse AuthHttpService::HandleRegisterBegin(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    const json body = ParseJsonBody(request);
    const std::string invite_token = body.value("invite_token", std::string{});
    const std::string username = support_.trim(body.value("username", std::string{}));
    const std::string password = body.value("password", std::string{});
    if (invite_token.empty() || username.empty() || password.empty()) {
      return support_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message",
                "invite_token, username, and password are required"}},
          {});
    }
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto invite = store.LoadRegistrationInviteByToken(invite_token);
    if (!invite.has_value() || !invite->revoked_at.empty() ||
        !invite->used_at.empty() ||
        invite->expires_at < support_.utc_now_sql_timestamp()) {
      return support_.build_json_response(
          404,
          json{{"status", "not_found"},
               {"message",
                "invite is missing, expired, revoked, or already used"}},
          {});
    }
    if (store.LoadUserByUsername(username).has_value()) {
      return support_.build_json_response(
          409,
          json{{"status", "conflict"},
               {"message", "username is already taken"}},
          {});
    }
    const std::string flow_id = comet::RandomTokenBase64(24);
    const std::string challenge = comet::RandomTokenBase64(32);
    const PendingWebAuthnFlow flow{
        flow_id,
        "register",
        username,
        comet::HashPassword(password),
        invite_token,
        0,
        challenge,
        support_.resolve_webauthn_rp_id(request),
        support_.resolve_webauthn_origin(request),
        support_.sql_timestamp_after_seconds(5 * 60),
    };
    const json options = support_.run_webauthn_helper(
        "generate-registration-options",
        json{{"rpName", support_.resolve_webauthn_rp_name()},
             {"rpID", flow.rp_id},
             {"userName", username},
             {"challenge", challenge}});
    support_.save_pending_webauthn_flow(flow);
    return support_.build_json_response(
        200,
        json{{"flow_id", flow_id},
             {"expires_at", flow.expires_at},
             {"options", options}},
        {});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500, json{{"status", "internal_error"}, {"message", error.what()}}, {});
  }
}

HttpResponse AuthHttpService::HandleRegisterFinish(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    const json body = ParseJsonBody(request);
    const std::string flow_id = body.value("flow_id", std::string{});
    if (flow_id.empty() || !body.contains("response")) {
      return support_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "flow_id and response are required"}},
          {});
    }
    const auto flow = support_.load_pending_webauthn_flow(flow_id);
    if (!flow.has_value() || flow->flow_kind != "register" ||
        flow->expires_at < support_.utc_now_sql_timestamp()) {
      return support_.build_json_response(
          410,
          json{{"status", "expired"},
               {"message", "registration flow is missing or expired"}},
          {});
    }
    const json verification = support_.run_webauthn_helper(
        "verify-registration",
        json{{"response", body.at("response")},
             {"expectedChallenge", flow->challenge},
             {"expectedOrigin", flow->origin},
             {"expectedRPID", flow->rp_id}});
    if (!verification.value("verified", false)) {
      return support_.build_json_response(
          403,
          json{{"status", "forbidden"},
               {"message", "WebAuthn registration verification failed"}},
          {});
    }
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto user = store.CreateInvitedUser(
        flow->invite_token, flow->username, flow->password_hash);
    const json credential = verification.at("registrationInfo");
    store.InsertWebAuthnCredential(comet::WebAuthnCredentialRecord{
        0,
        user.id,
        credential.value("credentialID", std::string{}),
        credential.value("credentialPublicKey", std::string{}),
        static_cast<std::uint32_t>(credential.value("counter", 0)),
        json(credential.value("transports", json::array())).dump(),
        "",
        "",
        "",
    });
    const std::string session_token =
        support_.create_controller_session(store, user.id, "web", "");
    support_.erase_pending_webauthn_flow(flow_id);
    return support_.build_json_response(
        200,
        json{{"user", support_.build_user_payload(user)}},
        {{"Set-Cookie", support_.session_cookie_header(session_token, request)}});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse AuthHttpService::HandleInvites(
    const std::string& db_path,
    const HttpRequest& request) const {
  try {
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto admin = support_.require_controller_admin_user(store, request);
    if (!admin.has_value()) {
      return support_.build_json_response(
          401,
          json{{"status", "unauthorized"},
               {"message", "admin session is required"}},
          {{"Set-Cookie", support_.clear_session_cookie_header(request)}});
    }
    if (request.method == "GET") {
      json items = json::array();
      for (const auto& invite : store.LoadActiveRegistrationInvites()) {
        json item = support_.build_invite_payload(invite);
        item["registration_url"] =
            support_.resolve_webauthn_origin(request) + "/register/" + invite.token;
        items.push_back(std::move(item));
      }
      return support_.build_json_response(200, json{{"items", items}}, {});
    }
    if (request.method == "POST") {
      const std::string token =
          support_.sanitize_token_for_path(comet::RandomTokenBase64(18));
      const auto invite = store.CreateRegistrationInvite(
          admin->id,
          token,
          support_.sql_timestamp_after_seconds(InviteLifetimeSeconds()));
      json item = support_.build_invite_payload(invite);
      item["registration_url"] =
          support_.resolve_webauthn_origin(request) + "/register/" + invite.token;
      return support_.build_json_response(200, json{{"invite", item}}, {});
    }
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse AuthHttpService::HandleInviteDelete(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "DELETE") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto admin = support_.require_controller_admin_user(store, request);
    if (!admin.has_value()) {
      return support_.build_json_response(
          401,
          json{{"status", "unauthorized"},
               {"message", "admin session is required"}},
          {{"Set-Cookie", support_.clear_session_cookie_header(request)}});
    }
    const int invite_id = std::stoi(
        request.path.substr(std::string("/api/v1/auth/invites/").size()));
    const bool revoked = store.RevokeRegistrationInvite(
        invite_id, support_.utc_now_sql_timestamp());
    return support_.build_json_response(
        revoked ? 200 : 404, json{{"revoked", revoked}}, {});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse AuthHttpService::HandleSshKeys(
    const std::string& db_path,
    const HttpRequest& request) const {
  try {
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto session =
        support_.authenticate_controller_user_session(store, request);
    if (!session.has_value()) {
      return support_.build_json_response(
          401,
          json{{"status", "unauthorized"},
               {"message", "authentication required"}},
          {{"Set-Cookie", support_.clear_session_cookie_header(request)}});
    }
    if (request.method == "GET") {
      json items = json::array();
      for (const auto& key : store.LoadActiveUserSshKeys(session->first.id)) {
        items.push_back(support_.build_ssh_key_payload(key));
      }
      return support_.build_json_response(200, json{{"items", items}}, {});
    }
    if (request.method == "POST") {
      const json body = ParseJsonBody(request);
      const std::string public_key =
          support_.trim(body.value("public_key", std::string{}));
      if (public_key.empty()) {
        return support_.build_json_response(
            400,
            json{{"status", "bad_request"},
                 {"message", "public_key is required"}},
            {});
      }
      const comet::UserSshKeyRecord ssh_key{
          0,
          session->first.id,
          support_.trim(body.value("label", std::string{})),
          public_key,
          support_.compute_ssh_public_key_fingerprint(public_key),
          "",
          "",
          "",
      };
      store.InsertUserSshKey(ssh_key);
      const auto created = store.LoadActiveUserSshKeyByFingerprint(
          session->first.id, ssh_key.fingerprint);
      return support_.build_json_response(
          200,
          json{{"ssh_key",
                created.has_value()
                    ? support_.build_ssh_key_payload(*created)
                    : support_.build_ssh_key_payload(ssh_key)}},
          {});
    }
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse AuthHttpService::HandleSshKeyDelete(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "DELETE") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto session =
        support_.authenticate_controller_user_session(store, request);
    if (!session.has_value()) {
      return support_.build_json_response(
          401,
          json{{"status", "unauthorized"},
               {"message", "authentication required"}},
          {{"Set-Cookie", support_.clear_session_cookie_header(request)}});
    }
    const int ssh_key_id = std::stoi(
        request.path.substr(std::string("/api/v1/auth/ssh-keys/").size()));
    const auto ssh_key = store.LoadActiveUserSshKeyById(ssh_key_id);
    if (!ssh_key.has_value() || ssh_key->user_id != session->first.id) {
      return support_.build_json_response(
          404,
          json{{"status", "not_found"}, {"message", "SSH key not found"}},
          {});
    }
    const bool revoked =
        store.RevokeUserSshKey(ssh_key_id, support_.utc_now_sql_timestamp());
    return support_.build_json_response(
        revoked ? 200 : 404, json{{"revoked", revoked}}, {});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse AuthHttpService::HandleSshChallenge(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    const json body = ParseJsonBody(request);
    const std::string username = support_.trim(body.value("username", std::string{}));
    const std::string plane_name = support_.trim(body.value("plane_name", std::string{}));
    const std::string fingerprint = support_.trim(body.value("fingerprint", std::string{}));
    if (username.empty() || plane_name.empty() || fingerprint.empty()) {
      return support_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message",
                "username, plane_name, and fingerprint are required"}},
          {});
    }
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto desired_state = store.LoadDesiredState(plane_name);
    if (!desired_state.has_value() || !desired_state->protected_plane) {
      return support_.build_json_response(
          404,
          json{{"status", "not_found"},
               {"message", "protected plane not found"}},
          {});
    }
    const auto user = store.LoadUserByUsername(username);
    if (!user.has_value()) {
      return support_.build_json_response(
          404, json{{"status", "not_found"}, {"message", "user not found"}}, {});
    }
    const auto ssh_key =
        store.LoadActiveUserSshKeyByFingerprint(user->id, fingerprint);
    if (!ssh_key.has_value()) {
      return support_.build_json_response(
          404,
          json{{"status", "not_found"},
               {"message", "SSH key fingerprint not found for user"}},
          {});
    }
    PendingSshChallenge challenge;
    challenge.challenge_id = comet::RandomTokenBase64(24);
    challenge.user_id = user->id;
    challenge.ssh_key_id = ssh_key->id;
    challenge.username = user->username;
    challenge.plane_name = plane_name;
    challenge.fingerprint = fingerprint;
    challenge.challenge_token = comet::RandomTokenBase64(24);
    challenge.expires_at =
        support_.sql_timestamp_after_seconds(SshChallengeLifetimeSeconds());
    challenge.message = support_.build_ssh_challenge_message(
        challenge.username,
        challenge.plane_name,
        challenge.challenge_token,
        challenge.expires_at);
    support_.save_pending_ssh_challenge(challenge);
    return support_.build_json_response(
        200,
        json{{"challenge_id", challenge.challenge_id},
             {"challenge_token", challenge.challenge_token},
             {"expires_at", challenge.expires_at},
             {"message", challenge.message}},
        {});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse AuthHttpService::HandleSshVerify(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    const json body = ParseJsonBody(request);
    const std::string challenge_id = body.value("challenge_id", std::string{});
    const std::string signature = body.value("signature", std::string{});
    if (challenge_id.empty() || signature.empty()) {
      return support_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "challenge_id and signature are required"}},
          {});
    }
    const auto challenge = support_.load_pending_ssh_challenge(challenge_id);
    if (!challenge.has_value() ||
        challenge->expires_at < support_.utc_now_sql_timestamp()) {
      return support_.build_json_response(
          410,
          json{{"status", "expired"},
               {"message", "SSH challenge is missing or expired"}},
          {});
    }
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto ssh_key = store.LoadActiveUserSshKeyById(challenge->ssh_key_id);
    if (!ssh_key.has_value()) {
      return support_.build_json_response(
          404, json{{"status", "not_found"}, {"message", "SSH key not found"}}, {});
    }
    if (!support_.verify_ssh_detached_signature(
            challenge->username,
            ssh_key->public_key,
            challenge->message,
            signature)) {
      return support_.build_json_response(
          403,
          json{{"status", "forbidden"},
               {"message", "SSH signature verification failed"}},
          {});
    }
    store.TouchUserSshKey(ssh_key->id, support_.utc_now_sql_timestamp());
    const std::string session_token = support_.create_controller_session(
        store, challenge->user_id, "ssh", challenge->plane_name);
    support_.erase_pending_ssh_challenge(challenge_id);
    return support_.build_json_response(
        200,
        json{{"token", session_token},
             {"plane_name", challenge->plane_name},
             {"expires_at",
              support_.sql_timestamp_after_seconds(SshSessionLifetimeSeconds())}},
        {});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}
