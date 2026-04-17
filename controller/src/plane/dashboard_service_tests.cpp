#include <atomic>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#include "naim/core/platform_compat.h"
#include "naim/state/sqlite_store.h"
#include "http/controller_http_transport.h"
#include "infra/controller_runtime_support_service.h"
#include "plane/dashboard_service.h"
#include "read_model/state_aggregate_loader.h"
#include "scheduler/scheduler_domain_service.h"
#include "scheduler/scheduler_domain_support.h"
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
    naim::platform::EnsureSocketsInitialized();
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (!naim::platform::IsSocketValid(listen_fd_)) {
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
      const auto error = naim::platform::LastSocketErrorMessage();
      naim::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to bind probe server: " + error);
    }
    if (listen(listen_fd_, 8) != 0) {
      const auto error = naim::platform::LastSocketErrorMessage();
      naim::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to listen probe server: " + error);
    }

    sockaddr_in bound_addr{};
    socklen_t bound_size = sizeof(bound_addr);
    if (getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound_addr), &bound_size) != 0) {
      const auto error = naim::platform::LastSocketErrorMessage();
      naim::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to inspect probe server socket: " + error);
    }
    port_ = ntohs(bound_addr.sin_port);
    thread_ = std::thread([this]() { Serve(); });
  }

  ~HealthProbeTestServer() {
    stop_requested_.store(true);
    if (port_ > 0) {
      const auto wake_fd = socket(AF_INET, SOCK_STREAM, 0);
      if (naim::platform::IsSocketValid(wake_fd)) {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port_));
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        (void)connect(wake_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        naim::platform::CloseSocket(wake_fd);
      }
    }
    if (naim::platform::IsSocketValid(listen_fd_)) {
      naim::platform::CloseSocket(listen_fd_);
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
      if (!naim::platform::IsSocketValid(client_fd)) {
        return;
      }
      if (stop_requested_.load()) {
        naim::platform::CloseSocket(client_fd);
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
      naim::platform::CloseSocket(client_fd);
    }
  }

  json payload_;
  std::atomic<bool> stop_requested_{false};
  naim::platform::SocketHandle listen_fd_ = naim::platform::kInvalidSocket;
  int port_ = 0;
  std::thread thread_;
};

class TestSchedulerDomainSupport final : public naim::controller::SchedulerDomainSupport {
 public:
  std::optional<long long> HeartbeatAgeSeconds(
      const std::string& heartbeat_at) const override {
    return runtime_support_service_.HeartbeatAgeSeconds(heartbeat_at);
  }

  std::string HealthFromAge(
      const std::optional<long long>& age_seconds,
      int stale_after_seconds) const override {
    return runtime_support_service_.HealthFromAge(age_seconds, stale_after_seconds);
  }

  std::optional<naim::RuntimeStatus> ParseRuntimeStatus(
      const naim::HostObservation& observation) const override {
    return runtime_support_service_.ParseRuntimeStatus(observation);
  }

  std::optional<naim::GpuTelemetrySnapshot> ParseGpuTelemetry(
      const naim::HostObservation& observation) const override {
    return runtime_support_service_.ParseGpuTelemetry(observation);
  }

  std::map<std::string, naim::NodeAvailabilityOverride> BuildAvailabilityOverrideMap(
      const std::vector<naim::NodeAvailabilityOverride>& availability_overrides) const override {
    return runtime_support_service_.BuildAvailabilityOverrideMap(availability_overrides);
  }

  naim::NodeAvailability ResolveNodeAvailability(
      const std::map<std::string, naim::NodeAvailabilityOverride>& overrides,
      const std::string& node_name) const override {
    return runtime_support_service_.ResolveNodeAvailability(overrides, node_name);
  }

  bool IsNodeSchedulable(naim::NodeAvailability availability) const override {
    return availability == naim::NodeAvailability::Active;
  }

  std::optional<long long> TimestampAgeSeconds(
      const std::string& timestamp_text) const override {
    return runtime_support_service_.TimestampAgeSeconds(timestamp_text);
  }

  std::optional<std::string> ObservedSchedulingGateReason(
      const std::vector<naim::HostObservation>&,
      const std::string&,
      int) const override {
    return std::nullopt;
  }

 private:
  naim::controller::ControllerRuntimeSupportService runtime_support_service_;
};

naim::controller::SchedulerDomainService MakeSchedulerDomainService() {
  return naim::controller::SchedulerDomainService(
      std::make_shared<TestSchedulerDomainSupport>(),
      naim::controller::SchedulerDomainPolicyConfig{
          300,
          100,
          300,
          60,
          85,
          1024,
      });
}

naim::controller::DashboardService MakeDashboardService() {
  static const naim::controller::ControllerRuntimeSupportService runtime_support_service;
  static const SchedulerViewService scheduler_view_service;
  static const naim::controller::SchedulerDomainService scheduler_domain_service =
      MakeSchedulerDomainService();
  static const naim::controller::StateAggregateLoader state_aggregate_loader(
      scheduler_domain_service,
      scheduler_view_service,
      runtime_support_service,
      1);
  static const naim::controller::DashboardService dashboard_service(
      naim::controller::DashboardService::Deps{
          &state_aggregate_loader,
          [](const naim::EventRecord& event) {
            return json{
                {"id", event.id},
                {"message", event.message},
            };
          },
          [&](const std::vector<naim::NodeAvailabilityOverride>& overrides) {
            return runtime_support_service.BuildAvailabilityOverrideMap(overrides);
          },
          [&](const std::map<std::string, naim::NodeAvailabilityOverride>& overrides,
              const std::string& node_name) {
            return runtime_support_service.ResolveNodeAvailability(overrides, node_name);
          },
          [&](const std::string& heartbeat_at) {
            return runtime_support_service.HeartbeatAgeSeconds(heartbeat_at);
          },
          [&](const std::optional<long long>& age_seconds, int stale_after_seconds) {
            return runtime_support_service.HealthFromAge(age_seconds, stale_after_seconds);
          },
          [&](const naim::HostObservation& observation) {
            return runtime_support_service.ParseRuntimeStatus(observation);
          },
          [&](const naim::HostObservation& observation) {
            return runtime_support_service.ParseGpuTelemetry(observation);
          },
      });
  return dashboard_service;
}

std::string MakeTempDbPath(const std::string& test_name) {
  const fs::path root = fs::temp_directory_path() / "naim-dashboard-service-tests" / test_name;
  std::error_code error;
  fs::remove_all(root, error);
  fs::create_directories(root);
  return (root / "controller.sqlite").string();
}

void SeedHostRecord(
    naim::ControllerStore& store,
    const std::string& node_name,
    const std::string& registration_state,
    const std::string& session_state,
    const std::string& last_heartbeat_at,
    const std::string& updated_at) {
  naim::RegisteredHostRecord host;
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
      {"image", "naim/web-ui:dev"},
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

json FindPlaneServiceTarget(const json& placement_payload, const std::string& service) {
  for (const auto& item : placement_payload.at("service_targets")) {
    if (item.value("service", std::string()) == service) {
      return item;
    }
  }
  throw std::runtime_error("missing plane service target " + service);
}

void TestHealthySelfServicesPayload() {
  const auto db_path = MakeTempDbPath("healthy");
  naim::ControllerStore store(db_path);
  store.Initialize();
  const naim::controller::ControllerRuntimeSupportService runtime_support_service;
  const std::string now = runtime_support_service.UtcNowSqlTimestamp();
  SeedHostRecord(store, "local-hostd", "registered", "connected", now, now);

  HealthProbeTestServer skills_factory_server(
      json{{"service", "naim-skills-factory"}, {"status", "ok"}});
  HealthProbeTestServer web_ui_server(
      json{{"service", "naim-controller"}, {"status", "ok"}});
  const fs::path web_ui_root =
      fs::temp_directory_path() / "naim-dashboard-service-tests" / "healthy-web-ui";
  WriteWebUiState(web_ui_root, ParseControllerEndpointTarget(web_ui_server.url()).port, true, true, "running");

  ScopedEnvVar admin_upstream("NAIM_CONTROLLER_ADMIN_UPSTREAM", std::nullopt);
  ScopedEnvVar internal_upstream("NAIM_CONTROLLER_INTERNAL_UPSTREAM", std::nullopt);
  ScopedEnvVar skills_factory_upstream(
      "NAIM_SKILLS_FACTORY_UPSTREAM",
      skills_factory_server.url());
  ScopedEnvVar web_ui_root_env("NAIM_WEB_UI_ROOT", web_ui_root.string());
  ScopedEnvVar hostd_node("NAIM_HOSTD_NODE_NAME", std::string("local-hostd"));

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
  naim::ControllerStore store(db_path);
  store.Initialize();
  SeedHostRecord(
      store,
      "local-hostd",
      "registered",
      "connected",
      "2000-01-01 00:00:00",
      "2000-01-01 00:00:00");

  ScopedEnvVar admin_upstream("NAIM_CONTROLLER_ADMIN_UPSTREAM", std::nullopt);
  ScopedEnvVar internal_upstream("NAIM_CONTROLLER_INTERNAL_UPSTREAM", std::nullopt);
  ScopedEnvVar skills_factory_upstream("NAIM_SKILLS_FACTORY_UPSTREAM", std::nullopt);
  ScopedEnvVar web_ui_root_env("NAIM_WEB_UI_ROOT", std::nullopt);
  ScopedEnvVar hostd_node("NAIM_HOSTD_NODE_NAME", std::string("local-hostd"));

  const auto payload = MakeDashboardService().BuildPayload(db_path, 300, std::nullopt);
  Expect(
      FindServiceItem(payload, "hostd").at("health").get<std::string>() == "warning",
      "hostd should be warning when the heartbeat is stale");
  std::cout << "ok: hostd-stale-heartbeat-warning" << '\n';
}

void TestWebUiMissingStateCritical() {
  const auto db_path = MakeTempDbPath("web-ui-missing");
  naim::ControllerStore store(db_path);
  store.Initialize();
  const naim::controller::ControllerRuntimeSupportService runtime_support_service;
  const std::string now = runtime_support_service.UtcNowSqlTimestamp();
  SeedHostRecord(store, "local-hostd", "registered", "connected", now, now);

  const fs::path missing_root =
      fs::temp_directory_path() / "naim-dashboard-service-tests" / "missing-web-ui";
  std::error_code error;
  fs::remove_all(missing_root, error);

  ScopedEnvVar admin_upstream("NAIM_CONTROLLER_ADMIN_UPSTREAM", std::nullopt);
  ScopedEnvVar internal_upstream("NAIM_CONTROLLER_INTERNAL_UPSTREAM", std::nullopt);
  ScopedEnvVar skills_factory_upstream("NAIM_SKILLS_FACTORY_UPSTREAM", std::nullopt);
  ScopedEnvVar web_ui_root_env("NAIM_WEB_UI_ROOT", missing_root.string());
  ScopedEnvVar hostd_node("NAIM_HOSTD_NODE_NAME", std::string("local-hostd"));

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
  naim::ControllerStore store(db_path);
  store.Initialize();
  const naim::controller::ControllerRuntimeSupportService runtime_support_service;
  const std::string now = runtime_support_service.UtcNowSqlTimestamp();
  SeedHostRecord(store, "local-hostd", "registered", "connected", now, now);

  ScopedEnvVar admin_upstream("NAIM_CONTROLLER_ADMIN_UPSTREAM", std::nullopt);
  ScopedEnvVar internal_upstream("NAIM_CONTROLLER_INTERNAL_UPSTREAM", std::nullopt);
  ScopedEnvVar skills_factory_upstream(
      "NAIM_SKILLS_FACTORY_UPSTREAM",
      std::string("http://127.0.0.1:9"));
  ScopedEnvVar web_ui_root_env("NAIM_WEB_UI_ROOT", std::nullopt);
  ScopedEnvVar hostd_node("NAIM_HOSTD_NODE_NAME", std::string("local-hostd"));

  const auto payload = MakeDashboardService().BuildPayload(db_path, 300, std::nullopt);
  Expect(
      payload.at("service").get<std::string>() == "naim-controller",
      "dashboard payload should still be produced when the skills factory probe fails");
  Expect(
      FindServiceItem(payload, "skills-factory").at("health").get<std::string>() == "critical",
      "skills factory should be critical when the probe fails");
  std::cout << "ok: skills-factory-probe-failure-does-not-break-payload" << '\n';
}

void TestPeerLinksIncludedWithoutDesiredPlane() {
  const auto db_path = MakeTempDbPath("peer-links-without-plane");
  naim::ControllerStore store(db_path);
  store.Initialize();

  naim::HostPeerLinkRecord hpc_to_storage;
  hpc_to_storage.observer_node_name = "hpc1";
  hpc_to_storage.peer_node_name = "storage1";
  hpc_to_storage.peer_endpoint = "http://192.168.88.252:29999";
  hpc_to_storage.local_interface = "vmbr0";
  hpc_to_storage.remote_address = "192.168.88.252";
  hpc_to_storage.seen_udp = true;
  hpc_to_storage.tcp_reachable = true;
  hpc_to_storage.rtt_ms = 1;
  hpc_to_storage.last_seen_at = "2026-04-17 18:56:06";
  hpc_to_storage.last_probe_at = "2026-04-17 18:56:06";
  store.UpsertHostPeerLink(hpc_to_storage);

  naim::HostPeerLinkRecord storage_to_hpc;
  storage_to_hpc.observer_node_name = "storage1";
  storage_to_hpc.peer_node_name = "hpc1";
  storage_to_hpc.peer_endpoint = "http://192.168.88.13:29999";
  storage_to_hpc.local_interface = "enp12s0";
  storage_to_hpc.remote_address = "192.168.88.13";
  storage_to_hpc.seen_udp = true;
  storage_to_hpc.tcp_reachable = true;
  storage_to_hpc.rtt_ms = 1;
  storage_to_hpc.last_seen_at = "2026-04-17 18:56:16";
  storage_to_hpc.last_probe_at = "2026-04-17 18:56:16";
  store.UpsertHostPeerLink(storage_to_hpc);

  ScopedEnvVar admin_upstream("NAIM_CONTROLLER_ADMIN_UPSTREAM", std::nullopt);
  ScopedEnvVar internal_upstream("NAIM_CONTROLLER_INTERNAL_UPSTREAM", std::nullopt);
  ScopedEnvVar skills_factory_upstream("NAIM_SKILLS_FACTORY_UPSTREAM", std::nullopt);
  ScopedEnvVar web_ui_root_env("NAIM_WEB_UI_ROOT", std::nullopt);
  ScopedEnvVar hostd_node("NAIM_HOSTD_NODE_NAME", std::string("local-hostd"));

  const auto payload = MakeDashboardService().BuildPayload(db_path, 300, std::nullopt);
  const auto summary = payload.at("peer_links").at("summary");
  Expect(summary.at("total").get<int>() == 2, "dashboard should include peer links without a desired plane");
  Expect(summary.at("direct").get<int>() == 2, "bidirectional reachable peer links should be direct");
  std::cout << "ok: peer-links-included-without-desired-plane" << '\n';
}

void TestRuntimePayloadIncludesKvCacheBytes() {
  const naim::controller::ControllerRuntimeSupportService runtime_support_service;
  const std::string now = runtime_support_service.UtcNowSqlTimestamp();

  naim::NodeInventory node;
  node.name = "node-a";
  node.gpu_devices = {"0"};

  naim::DesiredState desired_state;
  desired_state.plane_name = "plane-a";
  desired_state.nodes.push_back(node);

  naim::HostObservation observation;
  observation.node_name = "node-a";
  observation.plane_name = desired_state.plane_name;
  observation.status = naim::HostObservationStatus::Applied;
  observation.heartbeat_at = now;

  naim::RuntimeStatus runtime_status;
  runtime_status.plane_name = desired_state.plane_name;
  runtime_status.runtime_backend = "llama-rpc-head";
  runtime_status.runtime_phase = "running";
  runtime_status.launch_ready = true;
  runtime_status.kv_cache_bytes = 3ULL * 1024ULL * 1024ULL * 1024ULL;
  observation.runtime_status_json = naim::SerializeRuntimeStatusJson(runtime_status);

  std::map<std::string, naim::NodeInventory> dashboard_nodes;
  dashboard_nodes.emplace(node.name, node);
  std::map<std::string, naim::HostObservation> observation_by_node;
  observation_by_node.emplace(observation.node_name, observation);
  const std::map<std::string, naim::NodeAvailabilityOverride> availability_override_map;

  const auto nodes_payload = naim::controller::DashboardService::BuildNodesPayload(
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
      [&](const std::map<std::string, naim::NodeAvailabilityOverride>& overrides,
          const std::string& node_name) {
        return runtime_support_service.ResolveNodeAvailability(overrides, node_name);
      },
      [&](const naim::HostObservation& value) {
        return runtime_support_service.ParseRuntimeStatus(value);
      },
      [](const naim::HostObservation&) -> std::optional<naim::GpuTelemetrySnapshot> {
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

  const auto runtime_payload = naim::controller::DashboardService::BuildRuntimePayload(
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
  const naim::controller::ControllerRuntimeSupportService runtime_support_service;
  const std::string now = runtime_support_service.UtcNowSqlTimestamp();

  naim::NodeInventory node;
  node.name = "node-a";
  node.gpu_devices = {"0"};

  naim::DesiredState desired_state;
  desired_state.plane_name = "maglev";
  desired_state.nodes.push_back(node);

  naim::HostObservation observation;
  observation.node_name = "node-a";
  observation.status = naim::HostObservationStatus::Applied;
  observation.heartbeat_at = now;

  naim::RuntimeStatus foreign_runtime_status;
  foreign_runtime_status.plane_name = "lt-cypher-ai";
  foreign_runtime_status.instance_name = "infer-lt-cypher-ai";
  foreign_runtime_status.runtime_backend = "llama-rpc-head";
  foreign_runtime_status.runtime_phase = "running";
  foreign_runtime_status.launch_ready = true;
  observation.runtime_status_json =
      naim::SerializeRuntimeStatusJson(foreign_runtime_status);

  std::map<std::string, naim::NodeInventory> dashboard_nodes;
  dashboard_nodes.emplace(node.name, node);
  std::map<std::string, naim::HostObservation> observation_by_node;
  observation_by_node.emplace(observation.node_name, observation);
  const std::map<std::string, naim::NodeAvailabilityOverride> availability_override_map;

  const auto nodes_payload = naim::controller::DashboardService::BuildNodesPayload(
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
      [&](const std::map<std::string, naim::NodeAvailabilityOverride>& overrides,
          const std::string& node_name) {
        return runtime_support_service.ResolveNodeAvailability(overrides, node_name);
      },
      [&](const naim::HostObservation& value) {
        return runtime_support_service.ParseRuntimeStatus(value);
      },
      [](const naim::HostObservation&) -> std::optional<naim::GpuTelemetrySnapshot> {
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

void TestPlanePayloadExposesExecutionNodeTargets() {
  naim::DesiredState desired_state;
  desired_state.plane_name = "placement-plane";
  desired_state.plane_mode = naim::PlaneMode::Llm;
  desired_state.placement_target = std::string("node:worker-a");
  desired_state.app_host = naim::ExternalAppHostConfig{
      "10.0.0.15",
      std::optional<std::string>("/tmp/id_ed25519"),
      std::nullopt,
      std::nullopt,
  };
  desired_state.skills = naim::SkillsSettings{true, {"skill-a"}};

  naim::NodeInventory node;
  node.name = "worker-a";
  desired_state.nodes.push_back(node);

  naim::InstanceSpec infer;
  infer.name = "infer-placement-plane";
  infer.role = naim::InstanceRole::Infer;
  infer.node_name = "worker-a";
  desired_state.instances.push_back(infer);

  naim::InstanceSpec worker;
  worker.name = "worker-placement-plane";
  worker.role = naim::InstanceRole::Worker;
  worker.node_name = "worker-a";
  desired_state.instances.push_back(worker);

  naim::InstanceSpec app;
  app.name = "app-placement-plane";
  app.role = naim::InstanceRole::App;
  app.node_name = "worker-a";
  desired_state.instances.push_back(app);

  naim::InstanceSpec skills;
  skills.name = "skills-placement-plane";
  skills.role = naim::InstanceRole::Skills;
  skills.node_name = "worker-a";
  desired_state.instances.push_back(skills);

  naim::PlaneRecord plane_record;
  plane_record.name = "placement-plane";
  plane_record.plane_mode = "llm";
  plane_record.generation = 3;
  plane_record.applied_generation = 2;
  plane_record.state = "running";

  const auto payload = naim::controller::DashboardService::BuildPlanePayload(
      desired_state,
      3,
      plane_record,
      2);
  const auto& placement = payload.at("placement");
  Expect(
      placement.at("mode").get<std::string>() == "execution-node",
      "plane payload should expose execution-node mode");
  Expect(
      placement.at("execution_node").get<std::string>() == "worker-a",
      "plane payload should expose selected execution node");
  Expect(
      placement.at("app_host").at("enabled").get<bool>(),
      "plane payload should expose enabled external app host");
  Expect(
      placement.at("app_host").at("address").get<std::string>() == "10.0.0.15",
      "plane payload should expose external app host address");
  Expect(
      placement.at("app_host").at("auth_mode").get<std::string>() == "ssh-key",
      "plane payload should expose external app host auth mode");
  Expect(
      FindPlaneServiceTarget(placement, "app").at("target_type").get<std::string>() ==
          "external-app-host",
      "app target should resolve to external app host");
  Expect(
      FindPlaneServiceTarget(placement, "skills-runtime").at("binding").get<std::string>() ==
          "skills-follow-app",
      "skills runtime should follow the app host");
  Expect(
      FindPlaneServiceTarget(placement, "skills-factory").at("target").get<std::string>() ==
          "naim-controller",
      "skills factory should stay on naim");
  std::cout << "ok: plane-payload-exposes-execution-node-targets" << '\n';
}

void TestPlanePayloadExposesLegacyCompatibilityMode() {
  naim::DesiredState desired_state;
  desired_state.plane_name = "legacy-plane";
  desired_state.plane_mode = naim::PlaneMode::Llm;

  naim::NodeInventory infer_node;
  infer_node.name = "controller-node";
  desired_state.nodes.push_back(infer_node);
  naim::NodeInventory worker_node;
  worker_node.name = "worker-node-a";
  desired_state.nodes.push_back(worker_node);

  naim::InstanceSpec infer;
  infer.name = "infer-legacy-plane";
  infer.role = naim::InstanceRole::Infer;
  infer.node_name = "controller-node";
  desired_state.instances.push_back(infer);

  naim::InstanceSpec worker;
  worker.name = "worker-legacy-plane";
  worker.role = naim::InstanceRole::Worker;
  worker.node_name = "worker-node-a";
  desired_state.instances.push_back(worker);

  const auto payload = naim::controller::DashboardService::BuildPlanePayload(
      desired_state,
      1,
      std::nullopt,
      0);
  const auto& placement = payload.at("placement");
  Expect(
      placement.at("mode").get<std::string>() == "legacy-topology-compatibility",
      "plane payload should expose legacy compatibility mode when placement_target is absent");
  Expect(
      placement.at("execution_node").is_null(),
      "legacy compatibility payload should not invent an execution node");
  Expect(
      FindPlaneServiceTarget(placement, "infer").at("target").get<std::string>() ==
          "controller-node",
      "infer target should reflect legacy node placement");
  Expect(
      FindPlaneServiceTarget(placement, "worker").at("target_type").get<std::string>() ==
          "node-group",
      "worker target should expose grouped node placement");
  std::cout << "ok: plane-payload-exposes-legacy-compatibility-mode" << '\n';
}

}  // namespace

int main() {
  try {
    TestHealthySelfServicesPayload();
    TestHostdStaleHeartbeatWarning();
    TestWebUiMissingStateCritical();
    TestSkillsFactoryProbeFailureDoesNotBreakPayload();
    TestPeerLinksIncludedWithoutDesiredPlane();
    TestRuntimePayloadIncludesKvCacheBytes();
    TestPlaneScopedNodesIgnoreForeignRuntimeStatus();
    TestPlanePayloadExposesExecutionNodeTargets();
    TestPlanePayloadExposesLegacyCompatibilityMode();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "dashboard service tests failed: " << error.what() << '\n';
    return 1;
  }
}
