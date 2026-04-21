#pragma once

#include <nlohmann/json.hpp>

#include "naim/state/sqlite_store.h"

#include "naim/state/models.h"

namespace naim::controller {

class AuthPayloadService {
 public:
  nlohmann::json BuildUserPayload(const naim::UserRecord& user) const;
  nlohmann::json BuildInvitePayload(
      const naim::RegistrationInviteRecord& invite) const;
  nlohmann::json BuildSshKeyPayload(
      const naim::UserSshKeyRecord& ssh_key) const;
};

}  // namespace naim::controller
