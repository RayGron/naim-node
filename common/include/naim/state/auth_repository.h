#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "naim/state/sqlite_store.h"

namespace naim {

class AuthRepository final {
 public:
  explicit AuthRepository(sqlite3* db);

  int LoadUserCount() const;
  std::optional<UserRecord> LoadUserById(int user_id) const;
  std::optional<UserRecord> LoadUserByUsername(const std::string& username) const;
  std::vector<UserRecord> LoadUsers() const;

  UserRecord CreateBootstrapAdmin(
      const std::string& username,
      const std::string& password_hash);
  UserRecord CreateInvitedUser(
      const std::string& invite_token,
      const std::string& username,
      const std::string& password_hash);

  void UpdateUserLastLoginAt(int user_id, const std::string& last_login_at);

  void InsertWebAuthnCredential(const WebAuthnCredentialRecord& credential);
  void UpdateWebAuthnCredentialCounter(
      const std::string& credential_id,
      std::uint32_t counter,
      const std::string& last_used_at);
  std::vector<WebAuthnCredentialRecord> LoadWebAuthnCredentialsForUser(int user_id) const;
  std::optional<WebAuthnCredentialRecord> LoadWebAuthnCredentialById(
      const std::string& credential_id) const;

  RegistrationInviteRecord CreateRegistrationInvite(
      int created_by_user_id,
      const std::string& token,
      const std::string& expires_at);
  std::optional<RegistrationInviteRecord> LoadRegistrationInviteByToken(
      const std::string& token) const;
  std::vector<RegistrationInviteRecord> LoadActiveRegistrationInvites() const;
  bool MarkRegistrationInviteUsed(
      const std::string& token,
      int used_by_user_id,
      const std::string& used_at);
  bool RevokeRegistrationInvite(
      int invite_id,
      const std::string& revoked_at);

  void InsertUserSshKey(const UserSshKeyRecord& ssh_key);
  std::vector<UserSshKeyRecord> LoadActiveUserSshKeys(int user_id) const;
  std::optional<UserSshKeyRecord> LoadActiveUserSshKeyByFingerprint(
      int user_id,
      const std::string& fingerprint) const;
  std::optional<UserSshKeyRecord> LoadActiveUserSshKeyById(int ssh_key_id) const;
  bool RevokeUserSshKey(int ssh_key_id, const std::string& revoked_at);
  void TouchUserSshKey(int ssh_key_id, const std::string& last_used_at);

  void InsertAuthSession(const AuthSessionRecord& session);
  std::optional<AuthSessionRecord> LoadActiveAuthSession(
      const std::string& token,
      const std::optional<std::string>& session_kind,
      const std::optional<std::string>& plane_name) const;
  bool RevokeAuthSession(const std::string& token, const std::string& revoked_at);
  bool TouchAuthSession(const std::string& token, const std::string& last_used_at);

 private:
  static UserRecord ReadUser(sqlite3_stmt* statement);
  static WebAuthnCredentialRecord ReadWebAuthnCredential(sqlite3_stmt* statement);
  static RegistrationInviteRecord ReadRegistrationInvite(sqlite3_stmt* statement);
  static UserSshKeyRecord ReadUserSshKey(sqlite3_stmt* statement);
  static AuthSessionRecord ReadAuthSession(sqlite3_stmt* statement);

  sqlite3* db_;
};

}  // namespace naim
