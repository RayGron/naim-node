#pragma once

#include <string>

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
