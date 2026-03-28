#pragma once

#include <map>
#include <optional>
#include <string>
#include <utility>

#include <nlohmann/json.hpp>

#include "auth/auth_pending_flows.h"
#include "auth/auth_http_support.h"
#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"

#include "comet/state/models.h"
#include "comet/state/sqlite_store.h"

class AuthHttpService {
 public:
  explicit AuthHttpService(AuthHttpSupport support);

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

  AuthHttpSupport support_;
};
