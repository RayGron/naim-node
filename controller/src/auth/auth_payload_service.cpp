#include "auth/auth_payload_service.h"

namespace naim::controller {

nlohmann::json AuthPayloadService::BuildUserPayload(
    const naim::UserRecord& user) const {
  return nlohmann::json{
      {"id", user.id},
      {"username", user.username},
      {"role", user.role},
      {"created_at", user.created_at},
      {"last_login_at",
       user.last_login_at.empty() ? nlohmann::json(nullptr)
                                  : nlohmann::json(user.last_login_at)},
  };
}

nlohmann::json AuthPayloadService::BuildInvitePayload(
    const naim::RegistrationInviteRecord& invite) const {
  return nlohmann::json{
      {"id", invite.id},
      {"token", invite.token},
      {"created_by_user_id", invite.created_by_user_id},
      {"expires_at", invite.expires_at},
      {"created_at", invite.created_at},
      {"used_by_user_id",
       invite.used_by_user_id.has_value()
           ? nlohmann::json(*invite.used_by_user_id)
           : nlohmann::json(nullptr)},
      {"used_at",
       invite.used_at.empty() ? nlohmann::json(nullptr)
                              : nlohmann::json(invite.used_at)},
      {"revoked_at",
       invite.revoked_at.empty() ? nlohmann::json(nullptr)
                                 : nlohmann::json(invite.revoked_at)},
  };
}

nlohmann::json AuthPayloadService::BuildSshKeyPayload(
    const naim::UserSshKeyRecord& ssh_key) const {
  return nlohmann::json{
      {"id", ssh_key.id},
      {"user_id", ssh_key.user_id},
      {"label", ssh_key.label},
      {"fingerprint", ssh_key.fingerprint},
      {"public_key", ssh_key.public_key},
      {"created_at", ssh_key.created_at},
      {"revoked_at",
       ssh_key.revoked_at.empty() ? nlohmann::json(nullptr)
                                  : nlohmann::json(ssh_key.revoked_at)},
      {"last_used_at",
       ssh_key.last_used_at.empty() ? nlohmann::json(nullptr)
                                    : nlohmann::json(ssh_key.last_used_at)},
  };
}

}  // namespace naim::controller
