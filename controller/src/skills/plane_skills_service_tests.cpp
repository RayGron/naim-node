#include <filesystem>
#include <iostream>
#include <sstream>
#include <atomic>
#include <stdexcept>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#include "comet/core/platform_compat.h"
#include "comet/state/sqlite_store.h"
#include "interaction/interaction_service.h"
#include "plane/plane_dashboard_skills_summary_service.h"
#include "skills/plane_skill_contextual_resolver_service.h"
#include "skills/plane_skills_service.h"

namespace {

namespace fs = std::filesystem;
using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

class SkillRuntimeTestServer {
 public:
  explicit SkillRuntimeTestServer(json skills_payload)
      : skills_payload_(std::move(skills_payload)) {
    comet::platform::EnsureSocketsInitialized();
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (!comet::platform::IsSocketValid(listen_fd_)) {
      throw std::runtime_error("failed to create test runtime socket");
    }

    int yes = 1;
#if defined(_WIN32)
    setsockopt(
        listen_fd_,
        SOL_SOCKET,
        SO_REUSEADDR,
        reinterpret_cast<const char*>(&yes),
        sizeof(yes));
#else
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#endif

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(0);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
      const auto error = comet::platform::LastSocketErrorMessage();
      comet::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to bind test runtime socket: " + error);
    }
    if (listen(listen_fd_, 8) != 0) {
      const auto error = comet::platform::LastSocketErrorMessage();
      comet::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to listen on test runtime socket: " + error);
    }

    sockaddr_in bound_addr{};
    socklen_t bound_size = sizeof(bound_addr);
    if (getsockname(
            listen_fd_,
            reinterpret_cast<sockaddr*>(&bound_addr),
            &bound_size) != 0) {
      const auto error = comet::platform::LastSocketErrorMessage();
      comet::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to inspect test runtime socket: " + error);
    }
    port_ = ntohs(bound_addr.sin_port);
    thread_ = std::thread([this]() { Serve(); });
  }

  ~SkillRuntimeTestServer() {
    stop_requested_.store(true);
    if (port_ > 0) {
      const auto wake_fd = socket(AF_INET, SOCK_STREAM, 0);
      if (comet::platform::IsSocketValid(wake_fd)) {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port_));
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        (void)connect(wake_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        comet::platform::CloseSocket(wake_fd);
      }
    }
    if (comet::platform::IsSocketValid(listen_fd_)) {
      comet::platform::CloseSocket(listen_fd_);
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  int port() const { return port_; }

 private:
  void Serve() {
    while (true) {
      sockaddr_in client_addr{};
      socklen_t client_size = sizeof(client_addr);
      const auto client_fd = accept(
          listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_size);
      if (!comet::platform::IsSocketValid(client_fd)) {
        return;
      }
      if (stop_requested_.load()) {
        comet::platform::CloseSocket(client_fd);
        return;
      }

      char buffer[4096];
      (void)recv(client_fd, buffer, sizeof(buffer), 0);

      const std::string body = json{{"skills", skills_payload_}}.dump();
      std::ostringstream response;
      response << "HTTP/1.1 200 OK\r\n";
      response << "Content-Type: application/json\r\n";
      response << "Content-Length: " << body.size() << "\r\n";
      response << "Connection: close\r\n\r\n";
      response << body;
      const auto payload = response.str();
      const char* data = payload.c_str();
      std::size_t remaining = payload.size();
      while (remaining > 0) {
        const auto written = send(client_fd, data, remaining, 0);
        if (written <= 0) {
          break;
        }
        data += written;
        remaining -= static_cast<std::size_t>(written);
      }
      comet::platform::CloseSocket(client_fd);
    }
  }

  json skills_payload_;
  std::atomic<bool> stop_requested_{false};
  comet::platform::SocketHandle listen_fd_ = comet::platform::kInvalidSocket;
  int port_ = 0;
  std::thread thread_;
};

comet::DesiredState BuildDesiredStateWithSkillsPort(
    const std::string& host_ip,
    const int host_port) {
  comet::DesiredState desired_state;
  desired_state.plane_name = "maglev";
  desired_state.plane_mode = comet::PlaneMode::Llm;
  comet::SkillsSettings skills_settings;
  skills_settings.enabled = true;
  desired_state.skills = skills_settings;

  comet::InstanceSpec skills;
  skills.name = "skills-maglev";
  skills.plane_name = "maglev";
  skills.node_name = "local-hostd";
  skills.role = comet::InstanceRole::Skills;
  comet::PublishedPort published_port;
  published_port.host_ip = host_ip;
  published_port.host_port = host_port;
  published_port.container_port = 18120;
  skills.published_ports.push_back(published_port);
  desired_state.instances.push_back(skills);
  return desired_state;
}

comet::DesiredState BuildDesiredState(
    const std::string& plane_name,
    const std::vector<std::string>& factory_skill_ids,
    bool skills_enabled = true) {
  comet::DesiredState desired_state;
  desired_state.plane_name = plane_name;
  desired_state.plane_mode = comet::PlaneMode::Llm;
  if (skills_enabled || !factory_skill_ids.empty()) {
    comet::SkillsSettings settings;
    settings.enabled = skills_enabled;
    settings.factory_skill_ids = factory_skill_ids;
    desired_state.skills = settings;
  }
  return desired_state;
}

std::string MakeTempDbPath() {
  const auto root =
      fs::temp_directory_path() / "comet-plane-skills-service-tests";
  fs::create_directories(root);
  return (root / "controller.sqlite").string();
}

}  // namespace

int main() {
  try {
    comet::controller::PlaneSkillsService service;

    {
      const auto target =
          service.ResolveTarget(BuildDesiredStateWithSkillsPort("127.0.0.1", 27978));
      Expect(target.has_value(), "skills target should resolve when a published port exists");
      Expect(target->host == "127.0.0.1", "skills target host should use published host_ip");
      Expect(target->port == 27978, "skills target port should use published host_port");
      Expect(
          target->raw == "http://127.0.0.1:27978",
          "skills target raw URL should use normalized published endpoint");
      std::cout << "ok: published-host-ip-target" << '\n';
    }

    {
      const auto target =
          service.ResolveTarget(BuildDesiredStateWithSkillsPort("0.0.0.0", 27978));
      Expect(target.has_value(), "skills target should resolve for wildcard published host_ip");
      Expect(
          target->host == "127.0.0.1",
          "skills target host should normalize wildcard host_ip for controller probes");
      Expect(
          target->raw == "http://127.0.0.1:27978",
          "skills target raw URL should normalize wildcard host_ip");
      std::cout << "ok: wildcard-host-ip-normalization" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "code-agent-root-cause-debug"},
                {"name", "root-cause-debug"},
                {"description",
                 "Debug bugs by reproducing them, isolating the invariant, and validating the root cause."},
                {"content",
                 "When handling a bug or regression, reproduce the issue, validate the root cause, and confirm the path."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances = BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"messages",
                    json::array(
                        {json{{"role", "user"},
                              {"content",
                               "Please debug this regression and find the root cause."}}})}});
      Expect(
          selection.mode == "contextual",
          "resolver should select a contextual skill for a matching prompt");
      Expect(
          selection.candidate_count == 1,
          "resolver should count enabled plane-local candidates");
      Expect(
          selection.selected_skill_ids.size() == 1 &&
              selection.selected_skill_ids.front() ==
                  "code-agent-root-cause-debug",
          "resolver should return the matching plane-local skill id");
      std::cout << "ok: contextual-resolver-selects-plane-local-skill" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "code-agent-safe-change"},
                {"name", "safe-change"},
                {"description", "Limit changes to the smallest safe patch."},
                {"content", "Keep the patch minimal and avoid unrelated edits."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances = BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"messages",
                    json::array(
                        {json{{"role", "user"},
                              {"content",
                               "Summarize the recent rollout status for this plane."}}})}});
      Expect(
          selection.mode == "none" && selection.candidate_count == 1 &&
              selection.selected_skill_ids.empty(),
          "resolver should return none when no candidate clears the score threshold");
      std::cout << "ok: contextual-resolver-no-match" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "code-agent-repo-map"},
                {"name", "repo-map"},
                {"description",
                 "Build a fast repository map before non-trivial changes."},
                {"content",
                 "Map the repository structure, entry points, and major dependencies before editing."},
                {"enabled", true}},
           json{{"id", "code-agent-test-first-fix"},
                {"name", "test-first-fix"},
                {"description",
                 "Explain a test-first bug-fix approach without starting execution unless the user explicitly asks for it."},
                {"content",
                 "First define the failing regression test, then make the smallest safe patch."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"messages",
                    json::array({json{{"role", "user"},
                                      {"content",
                                       "Построй карту репозитория перед тем, как вносить изменения."}}})}});
      Expect(
          selection.mode == "contextual",
          "resolver should support Cyrillic prompts for contextual selection");
      Expect(
          selection.selected_skill_ids.size() == 1 &&
              selection.selected_skill_ids.front() == "code-agent-repo-map",
          "resolver should select repo-map instead of drifting into another skill");
      std::cout << "ok: contextual-resolver-selects-cyrillic-repo-map" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "code-agent-deploy-path-check"},
                {"name", "deploy-path-check"},
                {"description",
                 "Explain the real deployment path and required live verification steps before executing them."},
                {"content",
                 "Describe rollout order, rebuild requirements, restarts, pull steps, and live verification."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"messages",
                    json::array({json{{"role", "user"},
                                      {"content",
                                       "Explain the deployment path and the live verification steps before rollout."}}})}});
      Expect(
          selection.mode == "contextual" &&
              selection.selected_skill_ids.size() == 1 &&
              selection.selected_skill_ids.front() == "code-agent-deploy-path-check",
          "resolver should select deploy-path-check for deployment path prompts");
      std::cout << "ok: contextual-resolver-selects-deploy-path-check" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "code-agent-state-schema-guard"},
                {"name", "state-schema-guard"},
                {"description",
                 "Protect desired-state, projectors, validators, renderers, and store changes as one contract."},
                {"content",
                 "Review desired-state schema changes together with the projector, validator, renderer, and store."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"messages",
                    json::array({json{{"role", "user"},
                                      {"content",
                                       "Check the desired-state schema together with the projector, validator, renderer, and store."}}})}});
      Expect(
          selection.mode == "contextual" &&
              selection.selected_skill_ids.size() == 1 &&
              selection.selected_skill_ids.front() == "code-agent-state-schema-guard",
          "resolver should select state-schema-guard for schema contract prompts");
      std::cout << "ok: contextual-resolver-selects-state-schema-guard" << '\n';
    }

    {
      const auto desired_state = BuildDesiredState("catalog-plane", {});

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"messages",
                    json::array(
                        {json{{"role", "user"},
                              {"content",
                               "Please map the repository before making changes."}}})}});
      Expect(
          selection.mode == "none" && selection.candidate_count == 0,
          "resolver should ignore skills present only in SkillsFactory");
      std::cout << "ok: contextual-resolver-ignores-factory-only-skills" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "code-agent-root-cause-debug"},
                {"name", "root-cause-debug"},
                {"description",
                 "Debug bugs by reproducing them, isolating the invariant, and validating the root cause."},
                {"content",
                 "When handling a bug or regression, reproduce the issue, validate the root cause, and confirm the path."},
                {"enabled", false}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances = BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"messages",
                    json::array(
                        {json{{"role", "user"},
                              {"content",
                               "Please debug this regression and find the root cause."}}})}});
      Expect(
          selection.mode == "none" && selection.candidate_count == 0,
          "resolver should exclude disabled plane-local skills");
      std::cout << "ok: contextual-resolver-excludes-disabled-skills" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "code-agent-root-cause-debug"},
                {"name", "root-cause-debug"},
                {"description",
                 "Debug bugs by reproducing them, isolating the invariant, and validating the root cause."},
                {"content",
                 "When handling a bug or regression, reproduce the issue, validate the root cause, and confirm the path."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances = BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto payload =
          comet::controller::PlaneSkillContextualResolverService().BuildDebugPayload(
              "",
              resolution,
              json{{"prompt", "Please debug this regression and find the root cause."}});
      Expect(
          payload.at("skill_resolution_mode").get<std::string>() == "contextual",
          "debug payload should report contextual mode for a match");
      Expect(
          payload.at("candidate_count").get<int>() == 1,
          "debug payload should include the plane-local candidate count");
      Expect(
          payload.at("selected_skill_ids").size() == 1 &&
              payload.at("selected_skill_ids").front().get<std::string>() ==
                  "code-agent-root-cause-debug",
          "debug payload should report the selected skill id");
      std::cout << "ok: contextual-resolver-debug-payload" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "lt-cypher-localtrade-auth-session"},
                {"name", "localtrade-auth-session"},
                {"description",
                 "Use for LocalTrade sign-in state, Access cookie reuse, GET /auth/me, and session validation."},
                {"content",
                 "Check whether the active LocalTrade session is valid and whether the Access cookie is required."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-user-streams"},
                {"name", "localtrade-user-streams"},
                {"description",
                 "Use for LocalTrade Socket.IO rooms such as balances and user_orders."},
                {"content",
                 "Explain protected rooms, public rooms, and Socket.IO room names."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-account-balances"},
                {"name", "localtrade-account-balances"},
                {"description",
                 "Use for LocalTrade balances, available balances, and totals."},
                {"content",
                 "Explain balance endpoints and when login is required."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt",
                    "Проверь авторизацию LocalTrade, активную сессию и Access cookie."}});
      Expect(
          selection.mode == "contextual" &&
              selection.selected_skill_ids.size() == 1 &&
              selection.selected_skill_ids.front() ==
                  "lt-cypher-localtrade-auth-session",
          "resolver should prefer auth-session over other LocalTrade skills for session prompts");
      std::cout << "ok: contextual-resolver-selects-localtrade-auth-session" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "lt-cypher-localtrade-spot-order-clarification"},
                {"name", "localtrade-spot-order-clarification"},
                {"description",
                 "Use when the user wants to place, inspect, or cancel a LocalTrade spot order."},
                {"content",
                 "Clarify limit order parameters, pairId, side, amount, rate, and require confirmation."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-market-data"},
                {"name", "localtrade-market-data"},
                {"description",
                 "Use for public pairs, charts, trades, and order book."},
                {"content",
                 "Explain pairs, chart endpoints, and public market-data rooms."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt",
                    "Хочу поставить лимитный ордер на покупку BTC и собрать параметры перед подтверждением."}});
      Expect(
          selection.mode == "contextual" &&
              !selection.selected_skill_ids.empty() &&
              selection.selected_skill_ids.front() ==
                  "lt-cypher-localtrade-spot-order-clarification",
          "resolver should prefer the LocalTrade spot-order clarification skill for Russian order prompts");
      std::cout << "ok: contextual-resolver-selects-localtrade-spot-order" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "lt-cypher-localtrade-market-data"},
                {"name", "localtrade-market-data"},
                {"description",
                 "Use for public LocalTrade pairs, charts, trades, order book, and pairs state without account data."},
                {"content",
                 "Prefer this skill for public pairs, public order book, public trades, and public market streams."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-user-streams"},
                {"name", "localtrade-user-streams"},
                {"description",
                 "Use for authenticated LocalTrade Socket.IO user rooms such as balances, user_orders, and user_trades."},
                {"content",
                 "Protected rooms require the Access cookie and are for account-specific streams."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-spot-order-clarification"},
                {"name", "localtrade-spot-order-clarification"},
                {"description",
                 "Use when the user wants to create, inspect, or cancel a LocalTrade spot order."},
                {"content",
                 "Collect limit-order parameters and require explicit confirmation."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt",
                    "Дай публичные пары и поток order book по BTC/USDT без данных моего аккаунта."}});
      Expect(
          selection.mode == "contextual" &&
              !selection.selected_skill_ids.empty() &&
              selection.selected_skill_ids.front() ==
                  "lt-cypher-localtrade-market-data",
          "resolver should prefer LocalTrade market-data over user-streams for public market prompts");
      std::cout << "ok: contextual-resolver-prefers-localtrade-market-data" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "lt-cypher-localtrade-account-balances"},
                {"name", "localtrade-account-balances"},
                {"description",
                 "Use for LocalTrade balances, available balances, and totals."},
                {"content",
                 "Explain balances endpoints and whether login is required."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-auth-session"},
                {"name", "localtrade-auth-session"},
                {"description",
                 "Use for LocalTrade sign-in state, Access cookie reuse, GET /auth/me, and session validation."},
                {"content",
                 "Check whether the active LocalTrade session is valid and whether the Access cookie is required."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt",
                    "Покажи мои доступные балансы на LocalTrade и скажи, нужен ли логин."}});
      Expect(
          selection.mode == "contextual" &&
              !selection.selected_skill_ids.empty() &&
              selection.selected_skill_ids.front() ==
                  "lt-cypher-localtrade-account-balances",
          "resolver should prefer LocalTrade account-balances over auth-session for balance prompts");
      std::cout << "ok: contextual-resolver-prefers-localtrade-account-balances" << '\n';
    }

    {
      const std::string db_path = MakeTempDbPath();
      fs::remove(db_path);
      comet::ControllerStore store(db_path);
      store.Initialize();
      const auto desired_state = BuildDesiredState(
          "catalog-plane", {"code-agent-root-cause-debug"});
      store.ReplaceDesiredState(desired_state, 1);
      comet::SkillsFactorySkillRecord record;
      record.id = "code-agent-root-cause-debug";
      record.name = "root-cause-debug";
      record.description =
          "Debug bugs by reproducing them, isolating the invariant, and validating the root cause.";
      record.content =
          "When handling a bug or regression, reproduce the issue, validate the root cause, and confirm the path.";
      store.UpsertSkillsFactorySkill(record);

      comet::controller::PlaneInteractionResolution resolution;
      resolution.db_path = db_path;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              db_path,
              resolution,
              json{{"messages",
                    json::array(
                        {json{{"role", "user"},
                              {"content",
                               "Please debug this regression and find the root cause."}}})}});
      Expect(
          selection.mode == "none" && selection.candidate_count == 0,
          "resolver should ignore controller catalog entries that are not present in runtime skillsd");
      std::cout << "ok: contextual-resolver-requires-runtime-skill-copy" << '\n';
    }

    {
      const auto payload =
          comet::controller::PlaneDashboardSkillsSummaryService::BuildPayload(
          BuildDesiredState("dashboard-plane", {"skill-alpha", "skill-beta"}, true),
          std::vector<comet::PlaneSkillBindingRecord>{
              comet::PlaneSkillBindingRecord{
                  "dashboard-plane", "skill-alpha", false, {}, {}, "", ""},
              comet::PlaneSkillBindingRecord{
                  "dashboard-plane", "skill-beta", true, {}, {}, "", ""},
          });
      Expect(
          payload.at("enabled").get<bool>(),
          "dashboard skills payload should report enabled state");
      Expect(
          payload.at("enabled_count").get<int>() == 1,
          "dashboard skills payload should count enabled plane-local skills");
      Expect(
          payload.at("total_count").get<int>() == 2,
          "dashboard skills payload should count attached plane-local skills");
      std::cout << "ok: dashboard-skills-counts" << '\n';
    }

    {
      const auto payload =
          comet::controller::PlaneDashboardSkillsSummaryService::BuildPayload(
          BuildDesiredState("dashboard-plane", {"skill-alpha"}, false),
          {});
      Expect(
          !payload.at("enabled").get<bool>(),
          "dashboard skills payload should report disabled state");
      Expect(
          payload.at("enabled_count").get<int>() == 0,
          "dashboard skills payload should show zero enabled count when disabled");
      std::cout << "ok: dashboard-skills-disabled" << '\n';
    }

    {
      comet::controller::InteractionRequestValidator validator;
      comet::controller::InteractionRequestContext request_context;
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = BuildDesiredState("interaction-plane", {});
      resolution.status_payload = json::object();
      const auto error = validator.ValidateAndNormalizeRequest(
          resolution,
          json{{"messages", json::array()}},
          &request_context);
      Expect(!error.has_value(), "validator should accept a basic interaction request");
      Expect(
          request_context.payload.at("auto_skills").get<bool>(),
          "validator should default auto_skills to true");
      std::cout << "ok: interaction-validator-default-auto-skills" << '\n';
    }

    {
      comet::controller::InteractionRequestValidator validator;
      comet::controller::InteractionRequestContext request_context;
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = BuildDesiredState("interaction-plane", {});
      resolution.status_payload = json::object();
      const auto error = validator.ValidateAndNormalizeRequest(
          resolution,
          json{{"messages", json::array()},
               {"auto_skills", "yes"}},
          &request_context);
      Expect(
          error.has_value() && error->code == "malformed_request",
          "validator should reject non-boolean auto_skills");
      std::cout << "ok: interaction-validator-rejects-malformed-auto-skills" << '\n';
    }

    {
      comet::controller::InteractionRequestValidator validator;
      comet::controller::InteractionRequestContext request_context;
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = BuildDesiredState("interaction-plane", {});
      resolution.status_payload = json::object();
      const auto error = validator.ValidateAndNormalizeRequest(
          resolution,
          json{{"messages", json::array()},
               {"auto_skills", false}},
          &request_context);
      Expect(!error.has_value(), "validator should accept auto_skills=false");
      Expect(
          !request_context.payload.at("auto_skills").get<bool>(),
          "validator should preserve explicit auto_skills=false");
      std::cout << "ok: interaction-validator-preserves-auto-skills-false" << '\n';
    }

    {
      comet::controller::InteractionSessionPresenter presenter;
      comet::controller::InteractionRequestContext request_context;
      request_context.request_id = "req-1";
      request_context.payload = json{
          {comet::controller::PlaneSkillsService::kAppliedSkillsPayloadKey,
           json::array({json{{"id", "skill-alpha"}, {"name", "safe-change"}}})},
          {comet::controller::PlaneSkillsService::kAutoAppliedSkillsPayloadKey,
           json::array({json{{"id", "skill-alpha"}, {"name", "safe-change"}}})},
          {comet::controller::PlaneSkillsService::kSkillResolutionModePayloadKey,
           "contextual"},
      };
      comet::controller::InteractionSessionResult result;
      result.session_id = "sess-1";
      result.model = "demo-model";
      result.content = "ok";
      result.completion_status = "completed";
      result.final_finish_reason = "stop";
      result.stop_reason = "natural_stop";
      comet::controller::PlaneInteractionResolution resolution;
      resolution.status_payload = json::object();
      const auto response = presenter.BuildResponseSpec(
          resolution,
          request_context,
          result);
      Expect(
          response.payload.at("auto_applied_skills").is_array(),
          "interaction response should expose auto_applied_skills");
      Expect(
          response.payload.at("skill_resolution_mode").get<std::string>() ==
              "contextual",
          "interaction response should expose skill_resolution_mode");
      std::cout << "ok: interaction-response-includes-skill-resolution-fields" << '\n';
    }

    return 0;
  } catch (const std::exception& error) {
    std::cerr << "plane_skills_service_tests failed: " << error.what() << '\n';
    return 1;
  }
}
