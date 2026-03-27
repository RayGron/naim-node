#pragma once

#include <functional>
#include <map>
#include <optional>
#include <string>
#include <utility>

#include <nlohmann/json.hpp>

#include "controller_http_transport.h"
#include "controller_http_types.h"

#include "comet/models.h"
#include "comet/sqlite_store.h"

struct PendingWebAuthnFlow {
  std::string flow_id;
  std::string flow_kind;
  std::string username;
  std::string password_hash;
  std::string invite_token;
  int user_id = 0;
  std::string challenge;
  std::string rp_id;
  std::string origin;
  std::string expires_at;
};

struct PendingSshChallenge {
  std::string challenge_id;
  int user_id = 0;
  int ssh_key_id = 0;
  std::string username;
  std::string plane_name;
  std::string fingerprint;
  std::string challenge_token;
  std::string message;
  std::string expires_at;
};

class AuthHttpService {
 public:
  using BuildJsonResponseFn = std::function<HttpResponse(
      int,
      const nlohmann::json&,
      const std::map<std::string, std::string>&)>;
  using BuildUserPayloadFn =
      std::function<nlohmann::json(const comet::UserRecord&)>;
  using BuildInvitePayloadFn =
      std::function<nlohmann::json(const comet::RegistrationInviteRecord&)>;
  using BuildSshKeyPayloadFn =
      std::function<nlohmann::json(const comet::UserSshKeyRecord&)>;
  using AuthenticateControllerUserSessionFn = std::function<std::optional<
      std::pair<comet::UserRecord, comet::AuthSessionRecord>>(
      comet::ControllerStore&,
      const HttpRequest&)>;
  using RequireControllerAdminUserFn = std::function<
      std::optional<comet::UserRecord>(comet::ControllerStore&, const HttpRequest&)>;
  using ResolveWebAuthnRpIdFn =
      std::function<std::string(const HttpRequest&)>;
  using ResolveWebAuthnOriginFn =
      std::function<std::string(const HttpRequest&)>;
  using ResolveWebAuthnRpNameFn = std::function<std::string()>;
  using RunWebAuthnHelperFn =
      std::function<nlohmann::json(const std::string&, const nlohmann::json&)>;
  using SessionCookieHeaderFn =
      std::function<std::string(const std::string&, const HttpRequest&)>;
  using ClearSessionCookieHeaderFn =
      std::function<std::string(const HttpRequest&)>;
  using CreateControllerSessionFn = std::function<std::string(
      comet::ControllerStore&,
      int,
      const std::string&,
      const std::string&)>;
  using UtcNowSqlTimestampFn = std::function<std::string()>;
  using SqlTimestampAfterSecondsFn = std::function<std::string(int)>;
  using TrimFn = std::function<std::string(const std::string&)>;
  using BuildSshChallengeMessageFn = std::function<std::string(
      const std::string&,
      const std::string&,
      const std::string&,
      const std::string&)>;
  using SanitizeTokenForPathFn =
      std::function<std::string(const std::string&)>;
  using ComputeSshPublicKeyFingerprintFn =
      std::function<std::string(const std::string&)>;
  using VerifySshDetachedSignatureFn = std::function<bool(
      const std::string&,
      const std::string&,
      const std::string&,
      const std::string&)>;
  using LoadPendingWebAuthnFlowFn =
      std::function<std::optional<PendingWebAuthnFlow>(const std::string&)>;
  using SavePendingWebAuthnFlowFn =
      std::function<void(const PendingWebAuthnFlow&)>;
  using ErasePendingWebAuthnFlowFn =
      std::function<void(const std::string&)>;
  using LoadPendingSshChallengeFn =
      std::function<std::optional<PendingSshChallenge>(const std::string&)>;
  using SavePendingSshChallengeFn =
      std::function<void(const PendingSshChallenge&)>;
  using ErasePendingSshChallengeFn =
      std::function<void(const std::string&)>;

  struct Deps {
    BuildJsonResponseFn build_json_response;
    BuildUserPayloadFn build_user_payload;
    BuildInvitePayloadFn build_invite_payload;
    BuildSshKeyPayloadFn build_ssh_key_payload;
    AuthenticateControllerUserSessionFn authenticate_controller_user_session;
    RequireControllerAdminUserFn require_controller_admin_user;
    ResolveWebAuthnRpIdFn resolve_webauthn_rp_id;
    ResolveWebAuthnOriginFn resolve_webauthn_origin;
    ResolveWebAuthnRpNameFn resolve_webauthn_rp_name;
    RunWebAuthnHelperFn run_webauthn_helper;
    SessionCookieHeaderFn session_cookie_header;
    ClearSessionCookieHeaderFn clear_session_cookie_header;
    CreateControllerSessionFn create_controller_session;
    UtcNowSqlTimestampFn utc_now_sql_timestamp;
    SqlTimestampAfterSecondsFn sql_timestamp_after_seconds;
    TrimFn trim;
    BuildSshChallengeMessageFn build_ssh_challenge_message;
    SanitizeTokenForPathFn sanitize_token_for_path;
    ComputeSshPublicKeyFingerprintFn compute_ssh_public_key_fingerprint;
    VerifySshDetachedSignatureFn verify_ssh_detached_signature;
    LoadPendingWebAuthnFlowFn load_pending_webauthn_flow;
    SavePendingWebAuthnFlowFn save_pending_webauthn_flow;
    ErasePendingWebAuthnFlowFn erase_pending_webauthn_flow;
    LoadPendingSshChallengeFn load_pending_ssh_challenge;
    SavePendingSshChallengeFn save_pending_ssh_challenge;
    ErasePendingSshChallengeFn erase_pending_ssh_challenge;
  };

  explicit AuthHttpService(Deps deps);

  std::optional<HttpResponse> HandleRequest(
      const std::string& db_path,
      const HttpRequest& request) const;

 private:
  HttpResponse HandleState(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleMe(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleLogout(const HttpRequest& request) const;
  HttpResponse HandleBootstrapBegin(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleBootstrapFinish(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleLoginBegin(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleLoginFinish(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleInviteLookup(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleRegisterBegin(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleRegisterFinish(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleInvites(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleInviteDelete(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleSshKeys(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleSshKeyDelete(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleSshChallenge(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleSshVerify(
      const std::string& db_path,
      const HttpRequest& request) const;

  Deps deps_;
};
