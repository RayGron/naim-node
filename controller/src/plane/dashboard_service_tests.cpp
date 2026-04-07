#include <atomic>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#include "comet/core/platform_compat.h"
#include "comet/state/sqlite_store.h"
#include "http/controller_http_transport.h"
#include "infra/controller_runtime_support_service.h"
#include "plane/dashboard_service.h"
#include "read_model/state_aggregate_loader.h"
#include "scheduler/scheduler_domain_service.h"
#include "scheduler/scheduler_view_service.h"

namespace fs = std::filesystem;
using nlohmann::json;

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

bool SetEnvVar(const std::string& key, const std::string& value) {
#if defined(_WIN32)
  return _putenv_s(key.c_str(), value.c_str()) == 0;
#else
  return ::setenv(key.c_str(), value.c_str(), 1) == 0;
#endif
}

bool UnsetEnvVar(const std::string& key) {
#if defined(_WIN32)
  return _putenv_s(key.c_str(), "") == 0;
#else
  return ::unsetenv(key.c_str()) == 0;
#endif
}

class ScopedEnvVar {
 public:
  ScopedEnvVar(std::string key, std::optional<std::string> value)
      : key_(std::move(key)) {
    const char* existing = std::getenv(key_.c_str());
    if (existing != nullptr) {
      previous_value_ = std::string(existing);
    }
    if (value.has_value()) {
      Expect(SetEnvVar(key_, *value), "failed to set env var " + key_);
    } else {
      Expect(UnsetEnvVar(key_), "failed to unset env var " + key_);
    }
  }

  ~ScopedEnvVar() {
    if (previous_value_.has_value()) {
      (void)SetEnvVar(key_, *previous_value_);
    } else {
      (void)UnsetEnvVar(key_);
    }
  }

 private:
  std::string key_;
  std::optional<std::string> previous_value_;
};

class HealthProbeTestServer {
 public:
  explicit HealthProbeTestServer(json payload)
      : payload_(std::move(payload)) {
    comet::platform::EnsureSocketsInitialized();
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (!comet::platform::IsSocketValid(listen_fd_)) {
      throw std::runtime_error("failed to create probe server socket");
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
      throw std::runtime_error("failed to bind probe server: " + error);
    }
    if (listen(listen_fd_, 8) != 0) {
      const auto error = comet::platform::LastSocketErrorMessage();
      comet::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to listen probe server: " + error);
    }

    sockaddr_in bound_addr{};
    socklen_t bound_size = sizeof(bound_addr);
    if (getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound_addr), &bound_size) != 0) {
      const auto error = comet::platform::LastSocketErrorMessage();
      comet::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to inspect probe server socket: " + error);
    }
    port_ = ntohs(bound_addr.sin_port);
    thread_ = std::thread([this]() { Serve(); });
  }

  ~HealthProbeTestServer() {
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

  std::string url() const {
    return "http://127.0.0.1:" + std::to_string(port_);
  }

 private:
  void Serve() {
    while (true) {
      sockaddr_in client_addr{};
      socklen_t client_size = sizeof(client_addr);
      const auto client_fd =
          accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_size);
      if (!comet::platform::IsSocketValid(client_fd)) {
        return;
      }
      if (stop_requested_.load()) {
        comet::platform::CloseSocket(client_fd);
        return;
      }

      char buffer[4096];
      (void)recv(client_fd, buffer, sizeof(buffer), 0);

      const std::string body = payload_.dump();
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

  json payload_;
  std::atomic<bool> stop_requested_{false};
  comet::platform::SocketHandle listen_fd_ = comet::platform::kInvalidSocket;
  int port_ = 0;
  std::thread thread_;
};

comet::controller::SchedulerDomainService MakeSchedulerDomainService() {
  const comet::controller::ControllerRuntimeSupportService runtime_support_service;
  return comet::controller::SchedulerDomainService({
      [&](const std::string& heartbeat_at) {
        return runtime_support_service.HeartbeatAgeSeconds(heartbeat_at);
      },
      [&](const std::optional<long long>& age_seconds, int stale_after_seconds) {
        return runtime_support_service.HealthFromAge(age_seconds, stale_after_seconds);
      },
      [&](const comet::HostObservation& observation) {
        return runtime_support_service.ParseRuntimeStatus(observation);
      },
      [&](const comet::HostObservation& observation) {
        return runtime_support_service.ParseGpuTelemetry(observation);
      },
      [&](const std::vector<comet::NodeAvailabilityOverride>& availability_overrides) {
        return runtime_support_service.BuildAvailabilityOverrideMap(availability_overrides);
      },
      [&](const std::map<std::string, comet::NodeAvailabilityOverride>& overrides,
          const std::string& node_name) {
        return runtime_support_service.ResolveNodeAvailability(overrides, node_name);
      },
      [](comet::NodeAvailability availability) {
        return availability == comet::NodeAvailability::Active;
      },
      [&](const std::string& timestamp_text) {
        return runtime_support_service.TimestampAgeSeconds(timestamp_text);
      },
      [](const std::vector<comet::HostObservation>&,
         const std::string&,
         int) -> std::optional<std::string> {
        return std::nullopt;
      },
      300,
      100,
      300,
      60,
      85,
      1024,
  });
}

comet::controller::DashboardService MakeDashboardService() {
  static const comet::controller::ControllerRuntimeSupportService runtime_support_service;
  static const SchedulerViewService scheduler_view_service;
  static const comet::controller::SchedulerDomainService scheduler_domain_service =
      MakeSchedulerDomainService();
  static const comet::controller::StateAggregateLoader state_aggregate_loader(
      scheduler_domain_service,
      scheduler_view_service,
      runtime_support_service,
      1);
  static const comet::controller::DashboardService dashboard_service(
      comet::controller::DashboardService::Deps{
          &state_aggregate_loader,
          [](const comet::EventRecord& event) {
            return json{
                {"id", event.id},
                {"message", event.message},
            };
          },
          [&](const std::vector<comet::NodeAvailabilityOverride>& overrides) {
            return runtime_support_service.BuildAvailabilityOverrideMap(overrides);
          },
          [&](const std::map<std::string, comet::NodeAvailabilityOverride>& overrides,
              const std::string& node_name) {
            return runtime_support_service.ResolveNodeAvailability(overrides, node_name);
          },
          [&](const std::string& heartbeat_at) {
            return runtime_support_service.HeartbeatAgeSeconds(heartbeat_at);
          },
          [&](const std::optional<long long>& age_seconds, int stale_after_seconds) {
            return runtime_support_service.HealthFromAge(age_seconds, stale_after_seconds);
          },
          [&](const comet::HostObservation& observation) {
            return runtime_support_service.ParseRuntimeStatus(observation);
          },
          [&](const comet::HostObservation& observation) {
            return runtime_support_service.ParseGpuTelemetry(observation);
          },
      });
  return dashboard_service;
}

std::string MakeTempDbPath(const std::string& test_name) {
  const fs::path root = fs::temp_directory_path() / "comet-dashboard-service-tests" / test_name;
  std::error_code error;
  fs::remove_all(root, error);
  fs::create_directories(root);
  return (root / "controller.sqlite").string();
}

void SeedHostRecord(
    comet::ControllerStore& store,
    const std::string& node_name,
    const std::string& registration_state,
    const std::string& session_state,
    const std::string& last_heartbeat_at,
    const std::string& updated_at) {
  comet::RegisteredHostRecord host;
  host.node_name = node_name;
  host.registration_state = registration_state;
  host.session_state = session_state;
  host.last_heartbeat_at = last_heartbeat_at;
  host.updated_at = updated_at;
  store.UpsertRegisteredHost(host);
}

void WriteWebUiState(
    const fs::path& web_ui_root,
    int listen_port,
    bool materialized,
    bool running,
    const std::string& status) {
  fs::create_directories(web_ui_root);
  const json state{
      {"compose_path", (web_ui_root / "docker-compose.yml").string()},
      {"controller_upstream", "http://127.0.0.1:18080"},
      {"image", "comet/web-ui:dev"},
      {"listen_port", listen_port},
      {"materialized", materialized},
      {"requested_controller_upstream", "http://127.0.0.1:18080"},
      {"running", running},
      {"status", status},
      {"web_ui_root", web_ui_root.string()},
  };
  std::ofstream out(web_ui_root / "web-ui-state.json");
  out << state.dump(2) << "\n";
}

json FindServiceItem(const json& payload, const std::string& id) {
  for (const auto& item : payload.at("self_services").at("items")) {
    if (item.value("id", std::string()) == id) {
      return item;
    }
  }
  throw std::runtime_error("missing self service item " + id);
}

void TestHealthySelfServicesPayload() {
  const auto db_path = MakeTempDbPath("healthy");
  comet::ControllerStore store(db_path);
  store.Initialize();
  const comet::controller::ControllerRuntimeSupportService runtime_support_service;
  const std::string now = runtime_support_service.UtcNowSqlTimestamp();
  SeedHostRecord(store, "local-hostd", "registered", "connected", now, now);

  HealthProbeTestServer skills_factory_server(
      json{{"service", "comet-skills-factory"}, {"status", "ok"}});
  HealthProbeTestServer web_ui_server(
      json{{"service", "comet-controller"}, {"status", "ok"}});
  const fs::path web_ui_root =
      fs::temp_directory_path() / "comet-dashboard-service-tests" / "healthy-web-ui";
  WriteWebUiState(web_ui_root, ParseControllerEndpointTarget(web_ui_server.url()).port, true, true, "running");

  ScopedEnvVar admin_upstream("COMET_CONTROLLER_ADMIN_UPSTREAM", std::nullopt);
  ScopedEnvVar internal_upstream("COMET_CONTROLLER_INTERNAL_UPSTREAM", std::nullopt);
  ScopedEnvVar skills_factory_upstream(
      "COMET_SKILLS_FACTORY_UPSTREAM",
      skills_factory_server.url());
  ScopedEnvVar web_ui_root_env("COMET_WEB_UI_ROOT", web_ui_root.string());
  ScopedEnvVar hostd_node("COMET_HOSTD_NODE_NAME", std::string("local-hostd"));

  const auto payload = MakeDashboardService().BuildPayload(db_path, 300, std::nullopt);
  const auto& items = payload.at("self_services").at("items");
  Expect(items.size() == 4, "self-services payload should contain four items");
  Expect(items.at(0).at("id").get<std::string>() == "controller", "controller item order mismatch");
  Expect(
      items.at(1).at("id").get<std::string>() == "skills-factory",
      "skills-factory item order mismatch");
  Expect(items.at(2).at("id").get<std::string>() == "hostd", "hostd item order mismatch");
  Expect(items.at(3).at("id").get<std::string>() == "web-ui", "web-ui item order mismatch");
  Expect(
      FindServiceItem(payload, "controller").at("health").get<std::string>() == "healthy",
      "controller should be healthy");
  Expect(
      FindServiceItem(payload, "skills-factory").at("health").get<std::string>() == "healthy",
      "skills factory should be healthy");
  Expect(
      FindServiceItem(payload, "hostd").at("health").get<std::string>() == "healthy",
      "hostd should be healthy with a fresh heartbeat");
  Expect(
      FindServiceItem(payload, "web-ui").at("health").get<std::string>() == "healthy",
      "web-ui should be healthy with a responding health endpoint");
  std::cout << "ok: healthy-self-services-payload" << '\n';
}

void TestHostdStaleHeartbeatWarning() {
  const auto db_path = MakeTempDbPath("hostd-stale");
  comet::ControllerStore store(db_path);
  store.Initialize();
  SeedHostRecord(
      store,
      "local-hostd",
      "registered",
      "connected",
      "2000-01-01 00:00:00",
      "2000-01-01 00:00:00");

  ScopedEnvVar admin_upstream("COMET_CONTROLLER_ADMIN_UPSTREAM", std::nullopt);
  ScopedEnvVar internal_upstream("COMET_CONTROLLER_INTERNAL_UPSTREAM", std::nullopt);
  ScopedEnvVar skills_factory_upstream("COMET_SKILLS_FACTORY_UPSTREAM", std::nullopt);
  ScopedEnvVar web_ui_root_env("COMET_WEB_UI_ROOT", std::nullopt);
  ScopedEnvVar hostd_node("COMET_HOSTD_NODE_NAME", std::string("local-hostd"));

  const auto payload = MakeDashboardService().BuildPayload(db_path, 300, std::nullopt);
  Expect(
      FindServiceItem(payload, "hostd").at("health").get<std::string>() == "warning",
      "hostd should be warning when the heartbeat is stale");
  std::cout << "ok: hostd-stale-heartbeat-warning" << '\n';
}

void TestWebUiMissingStateCritical() {
  const auto db_path = MakeTempDbPath("web-ui-missing");
  comet::ControllerStore store(db_path);
  store.Initialize();
  const comet::controller::ControllerRuntimeSupportService runtime_support_service;
  const std::string now = runtime_support_service.UtcNowSqlTimestamp();
  SeedHostRecord(store, "local-hostd", "registered", "connected", now, now);

  const fs::path missing_root =
      fs::temp_directory_path() / "comet-dashboard-service-tests" / "missing-web-ui";
  std::error_code error;
  fs::remove_all(missing_root, error);

  ScopedEnvVar admin_upstream("COMET_CONTROLLER_ADMIN_UPSTREAM", std::nullopt);
  ScopedEnvVar internal_upstream("COMET_CONTROLLER_INTERNAL_UPSTREAM", std::nullopt);
  ScopedEnvVar skills_factory_upstream("COMET_SKILLS_FACTORY_UPSTREAM", std::nullopt);
  ScopedEnvVar web_ui_root_env("COMET_WEB_UI_ROOT", missing_root.string());
  ScopedEnvVar hostd_node("COMET_HOSTD_NODE_NAME", std::string("local-hostd"));

  const auto payload = MakeDashboardService().BuildPayload(db_path, 300, std::nullopt);
  const auto web_ui = FindServiceItem(payload, "web-ui");
  Expect(
      web_ui.at("health").get<std::string>() == "critical" &&
          web_ui.at("state").get<std::string>() == "missing",
      "web-ui should be critical when the state file is missing");
  std::cout << "ok: web-ui-missing-state-critical" << '\n';
}

void TestSkillsFactoryProbeFailureDoesNotBreakPayload() {
  const auto db_path = MakeTempDbPath("skills-factory-failure");
  comet::ControllerStore store(db_path);
  store.Initialize();
  const comet::controller::ControllerRuntimeSupportService runtime_support_service;
  const std::string now = runtime_support_service.UtcNowSqlTimestamp();
  SeedHostRecord(store, "local-hostd", "registered", "connected", now, now);

  ScopedEnvVar admin_upstream("COMET_CONTROLLER_ADMIN_UPSTREAM", std::nullopt);
  ScopedEnvVar internal_upstream("COMET_CONTROLLER_INTERNAL_UPSTREAM", std::nullopt);
  ScopedEnvVar skills_factory_upstream(
      "COMET_SKILLS_FACTORY_UPSTREAM",
      std::string("http://127.0.0.1:9"));
  ScopedEnvVar web_ui_root_env("COMET_WEB_UI_ROOT", std::nullopt);
  ScopedEnvVar hostd_node("COMET_HOSTD_NODE_NAME", std::string("local-hostd"));

  const auto payload = MakeDashboardService().BuildPayload(db_path, 300, std::nullopt);
  Expect(
      payload.at("service").get<std::string>() == "comet-controller",
      "dashboard payload should still be produced when the skills factory probe fails");
  Expect(
      FindServiceItem(payload, "skills-factory").at("health").get<std::string>() == "critical",
      "skills factory should be critical when the probe fails");
  std::cout << "ok: skills-factory-probe-failure-does-not-break-payload" << '\n';
}

void TestRuntimePayloadIncludesKvCacheBytes() {
  const comet::controller::ControllerRuntimeSupportService runtime_support_service;
  const std::string now = runtime_support_service.UtcNowSqlTimestamp();

  comet::NodeInventory node;
  node.name = "node-a";
  node.gpu_devices = {"0"};

  comet::DesiredState desired_state;
  desired_state.plane_name = "plane-a";
  desired_state.nodes.push_back(node);

  comet::HostObservation observation;
  observation.node_name = "node-a";
  observation.plane_name = desired_state.plane_name;
  observation.status = comet::HostObservationStatus::Applied;
  observation.heartbeat_at = now;

  comet::RuntimeStatus runtime_status;
  runtime_status.plane_name = desired_state.plane_name;
  runtime_status.runtime_backend = "llama-rpc-head";
  runtime_status.runtime_phase = "running";
  runtime_status.launch_ready = true;
  runtime_status.kv_cache_bytes = 3ULL * 1024ULL * 1024ULL * 1024ULL;
  observation.runtime_status_json = comet::SerializeRuntimeStatusJson(runtime_status);

  std::map<std::string, comet::NodeInventory> dashboard_nodes;
  dashboard_nodes.emplace(node.name, node);
  std::map<std::string, comet::HostObservation> observation_by_node;
  observation_by_node.emplace(observation.node_name, observation);
  const std::map<std::string, comet::NodeAvailabilityOverride> availability_override_map;

  const auto nodes_payload = comet::controller::DashboardService::BuildNodesPayload(
      dashboard_nodes,
      observation_by_node,
      availability_override_map,
      desired_state,
      {},
      desired_state.plane_name,
      std::string("running"),
      1,
      300,
      [&](const std::string& heartbeat_at) {
        return runtime_support_service.HeartbeatAgeSeconds(heartbeat_at);
      },
      [&](const std::optional<long long>& age_seconds, int stale_after_seconds) {
        return runtime_support_service.HealthFromAge(age_seconds, stale_after_seconds);
      },
      [&](const std::map<std::string, comet::NodeAvailabilityOverride>& overrides,
          const std::string& node_name) {
        return runtime_support_service.ResolveNodeAvailability(overrides, node_name);
      },
      [&](const comet::HostObservation& value) {
        return runtime_support_service.ParseRuntimeStatus(value);
      },
      [](const comet::HostObservation&) -> std::optional<comet::GpuTelemetrySnapshot> {
        return std::nullopt;
      });

  Expect(nodes_payload.items.size() == 1, "expected a single node payload item");
  Expect(
      nodes_payload.items.at(0).at("kv_cache_bytes").get<std::uint64_t>() ==
          *runtime_status.kv_cache_bytes,
      "node payload should expose kv_cache_bytes");
  Expect(
      nodes_payload.kv_cache_bytes.has_value() &&
          *nodes_payload.kv_cache_bytes == *runtime_status.kv_cache_bytes,
      "nodes payload should aggregate kv_cache_bytes");

  const auto runtime_payload = comet::controller::DashboardService::BuildRuntimePayload(
      nodes_payload.observed_nodes,
      nodes_payload.ready_nodes,
      nodes_payload.not_ready_nodes,
      nodes_payload.degraded_gpu_nodes,
      nodes_payload.kv_cache_bytes);
  Expect(
      runtime_payload.at("kv_cache_bytes").get<std::uint64_t>() == *runtime_status.kv_cache_bytes,
      "runtime payload should expose aggregated kv_cache_bytes");

  std::cout << "ok: runtime-payload-includes-kv-cache-bytes" << '\n';
}

void TestPlaneScopedNodesIgnoreForeignRuntimeStatus() {
  const comet::controller::ControllerRuntimeSupportService runtime_support_service;
  const std::string now = runtime_support_service.UtcNowSqlTimestamp();

  comet::NodeInventory node;
  node.name = "node-a";
  node.gpu_devices = {"0"};

  comet::DesiredState desired_state;
  desired_state.plane_name = "maglev";
  desired_state.nodes.push_back(node);

  comet::HostObservation observation;
  observation.node_name = "node-a";
  observation.status = comet::HostObservationStatus::Applied;
  observation.heartbeat_at = now;

  comet::RuntimeStatus foreign_runtime_status;
  foreign_runtime_status.plane_name = "lt-cypher-ai";
  foreign_runtime_status.instance_name = "infer-lt-cypher-ai";
  foreign_runtime_status.runtime_backend = "llama-rpc-head";
  foreign_runtime_status.runtime_phase = "running";
  foreign_runtime_status.launch_ready = true;
  observation.runtime_status_json =
      comet::SerializeRuntimeStatusJson(foreign_runtime_status);

  std::map<std::string, comet::NodeInventory> dashboard_nodes;
  dashboard_nodes.emplace(node.name, node);
  std::map<std::string, comet::HostObservation> observation_by_node;
  observation_by_node.emplace(observation.node_name, observation);
  const std::map<std::string, comet::NodeAvailabilityOverride> availability_override_map;

  const auto nodes_payload = comet::controller::DashboardService::BuildNodesPayload(
      dashboard_nodes,
      observation_by_node,
      availability_override_map,
      desired_state,
      {},
      desired_state.plane_name,
      std::string("running"),
      1,
      300,
      [&](const std::string& heartbeat_at) {
        return runtime_support_service.HeartbeatAgeSeconds(heartbeat_at);
      },
      [&](const std::optional<long long>& age_seconds, int stale_after_seconds) {
        return runtime_support_service.HealthFromAge(age_seconds, stale_after_seconds);
      },
      [&](const std::map<std::string, comet::NodeAvailabilityOverride>& overrides,
          const std::string& node_name) {
        return runtime_support_service.ResolveNodeAvailability(overrides, node_name);
      },
      [&](const comet::HostObservation& value) {
        return runtime_support_service.ParseRuntimeStatus(value);
      },
      [](const comet::HostObservation&) -> std::optional<comet::GpuTelemetrySnapshot> {
        return std::nullopt;
      });

  Expect(nodes_payload.items.size() == 1, "expected a single node payload item");
  Expect(
      nodes_payload.ready_nodes == 0,
      "foreign runtime status must not mark the selected plane as ready");
  Expect(
      nodes_payload.items.at(0).at("runtime_launch_ready").is_null(),
      "foreign runtime status must not be exposed in plane-scoped node payload");

  std::cout << "ok: plane-scoped-nodes-ignore-foreign-runtime-status" << '\n';
}

}  // namespace

int main() {
  try {
    TestHealthySelfServicesPayload();
    TestHostdStaleHeartbeatWarning();
    TestWebUiMissingStateCritical();
    TestSkillsFactoryProbeFailureDoesNotBreakPayload();
    TestRuntimePayloadIncludesKvCacheBytes();
    TestPlaneScopedNodesIgnoreForeignRuntimeStatus();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "dashboard service tests failed: " << error.what() << '\n';
    return 1;
  }
}
