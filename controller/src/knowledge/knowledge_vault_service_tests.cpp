#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <string>

#include "knowledge/knowledge_vault_http_service.h"
#include "knowledge/knowledge_vault_service.h"
#include "knowledge/knowledge_vault_service_repository.h"
#include "naim/state/sqlite_store.h"

namespace fs = std::filesystem;

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::string MakeTempDbPath(const std::string& test_name) {
  const auto unique_suffix = std::chrono::steady_clock::now().time_since_epoch().count();
  const fs::path root =
      fs::temp_directory_path() /
      ("naim-knowledge-vault-service-tests-" + std::to_string(unique_suffix)) /
      test_name;
  std::error_code error;
  fs::remove_all(root, error);
  error.clear();
  fs::create_directories(root, error);
  Expect(!error, "failed to create test database directory: " + error.message());
  return (root / "controller.sqlite").string();
}

naim::controller::KnowledgeVaultServiceRecord BuildServiceRecord() {
  naim::controller::KnowledgeVaultServiceRecord record;
  record.service_id = "kv_default";
  record.node_name = "storage1";
  record.image = "chainzano.com/naim/knowledge-runtime:latest";
  record.endpoint_host = "127.0.0.1";
  record.endpoint_port = 18200;
  record.status = "starting";
  return record;
}

naim::controller::KnowledgeVaultServiceRecord BuildReadyServiceRecord() {
  auto record = BuildServiceRecord();
  record.status = "ready";
  record.schema_version = "knowledge.v1";
  record.index_epoch = "idx_0";
  return record;
}

void SeedApplyAssignment(
    const std::string& db_path,
    naim::HostAssignmentStatus status,
    const std::string& status_message = "") {
  naim::ControllerStore store(db_path);
  store.Initialize();

  naim::HostAssignment assignment;
  assignment.node_name = "storage1";
  assignment.plane_name = "knowledge-vault:kv_default";
  assignment.assignment_type = "knowledge-vault-apply";
  assignment.desired_state_json = "{}";
  assignment.status = naim::HostAssignmentStatus::Pending;
  assignment.status_message = "";
  assignment.attempt_count = 0;
  assignment.max_attempts = 3;
  store.EnqueueHostAssignments({assignment}, "test");
  if (status != naim::HostAssignmentStatus::Pending) {
    const auto assignments = store.LoadHostAssignments(
        assignment.node_name,
        naim::HostAssignmentStatus::Pending,
        assignment.plane_name);
    Expect(!assignments.empty(), "expected seeded assignment");
    store.UpdateHostAssignmentStatus(assignments.back().id, status, status_message);
  }
}

std::size_t CountProxyAssignments(const std::string& db_path) {
  naim::ControllerStore store(db_path);
  store.Initialize();
  std::size_t count = 0;
  for (const auto& assignment : store.LoadHostAssignments(std::nullopt, std::nullopt, std::nullopt)) {
    if (assignment.assignment_type == "knowledge-vault-http-proxy") {
      ++count;
    }
  }
  return count;
}

void TestStatusDoesNotProxyWhileApplyIsPending() {
  const std::string db_path = MakeTempDbPath("pending");
  naim::controller::KnowledgeVaultServiceRepository{}.UpsertService(
      db_path,
      BuildServiceRecord());
  SeedApplyAssignment(db_path, naim::HostAssignmentStatus::Pending);

  const auto status = naim::controller::KnowledgeVaultService{}.BuildStatus(db_path);
  Expect(status.value("status", std::string{}) == "starting", "status should remain starting");
  Expect(status.contains("apply_assignment"), "status should expose apply assignment");
  Expect(CountProxyAssignments(db_path) == 0, "pending apply should not enqueue proxy requests");
}

void TestStatusReportsApplyFailureWithoutProxy() {
  const std::string db_path = MakeTempDbPath("failed");
  naim::controller::KnowledgeVaultServiceRepository{}.UpsertService(
      db_path,
      BuildServiceRecord());
  SeedApplyAssignment(
      db_path,
      naim::HostAssignmentStatus::Failed,
      "failed to pull knowledge vault image");

  const auto status = naim::controller::KnowledgeVaultService{}.BuildStatus(db_path);
  Expect(status.value("status", std::string{}) == "failed", "failed apply should surface failed");
  Expect(
      status.value("runtime_error", std::string{}).find("failed to pull") != std::string::npos,
      "failed apply should surface pull error");
  Expect(CountProxyAssignments(db_path) == 0, "failed apply should not enqueue proxy requests");
}

void TestReadyStatusUsesCacheWithoutProxy() {
  const std::string db_path = MakeTempDbPath("ready");
  naim::controller::KnowledgeVaultServiceRepository{}.UpsertService(
      db_path,
      BuildReadyServiceRecord());

  const auto status = naim::controller::KnowledgeVaultService{}.BuildStatus(db_path);
  Expect(status.value("status", std::string{}) == "ready", "ready status should remain ready");
  Expect(
      status.value("schema_version", std::string{}) == "knowledge.v1",
      "ready status should include cached schema");
  Expect(CountProxyAssignments(db_path) == 0, "ready status should not enqueue proxy requests");
}

naim::DesiredState BuildKnowledgePlaneState() {
  naim::DesiredState desired_state;
  desired_state.plane_name = "knowledge-plane";
  naim::KnowledgeSettings knowledge;
  knowledge.enabled = true;
  knowledge.selected_knowledge_ids = {"knowledge.alpha", "knowledge.beta"};
  desired_state.knowledge = knowledge;
  return desired_state;
}

void TestPlaneScopedContextRequestAddsPlaneAndSelectedKnowledge() {
  HttpRequest request;
  request.method = "POST";
  request.path = "/api/v1/planes/knowledge-plane/knowledge-vault/context";
  request.headers["Content-Type"] = "application/json";
  request.body = R"({"query":"what is known"})";

  const auto rewritten =
      naim::controller::KnowledgeVaultHttpService::BuildPlaneScopedRequest(
          request,
          BuildKnowledgePlaneState(),
          "knowledge-plane");
  const auto body = nlohmann::json::parse(rewritten.body);

  Expect(
      rewritten.path == "/api/v1/knowledge-vault/context",
      "plane context path should rewrite to canonical knowledge route");
  Expect(
      body.value("plane_id", std::string{}) == "knowledge-plane",
      "plane context should include plane id");
  Expect(
      body.at("selected_knowledge_ids") ==
          nlohmann::json::array({"knowledge.alpha", "knowledge.beta"}),
      "plane context should default to desired selected knowledge ids");
  Expect(body.value("query", std::string{}) == "what is known", "request body should be preserved");
}

void TestPlaneScopedGraphRequestDefaultsToSelectedKnowledge() {
  HttpRequest request;
  request.method = "POST";
  request.path = "/api/v1/planes/knowledge-plane/knowledge-vault/graph-neighborhood";
  request.headers["Content-Type"] = "application/json";
  request.body = R"({"depth":2})";

  const auto rewritten =
      naim::controller::KnowledgeVaultHttpService::BuildPlaneScopedRequest(
          request,
          BuildKnowledgePlaneState(),
          "knowledge-plane");
  const auto body = nlohmann::json::parse(rewritten.body);

  Expect(
      rewritten.path == "/api/v1/knowledge-vault/graph-neighborhood",
      "plane graph path should rewrite to canonical knowledge route");
  Expect(
      body.at("knowledge_ids") ==
          nlohmann::json::array({"knowledge.alpha", "knowledge.beta"}),
      "plane graph should default to selected knowledge ids");
  Expect(body.value("depth", 0) == 2, "graph request body should be preserved");
}

void TestPlaneScopedSearchRequestKeepsExplicitBody() {
  HttpRequest request;
  request.method = "POST";
  request.path = "/api/v1/planes/knowledge-plane/knowledge-vault/search";
  request.headers["Content-Type"] = "application/json";
  request.body = R"({"query":"alpha","limit":5})";

  const auto rewritten =
      naim::controller::KnowledgeVaultHttpService::BuildPlaneScopedRequest(
          request,
          BuildKnowledgePlaneState(),
          "knowledge-plane");
  const auto body = nlohmann::json::parse(rewritten.body);

  Expect(
      rewritten.path == "/api/v1/knowledge-vault/search",
      "plane search path should rewrite to canonical knowledge route");
  Expect(
      body.value("plane_id", std::string{}) == "knowledge-plane",
      "plane search should include plane id");
  Expect(body.value("query", std::string{}) == "alpha", "search query should be preserved");
  Expect(body.value("limit", 0) == 5, "search limit should be preserved");
  Expect(!body.contains("selected_knowledge_ids"), "search should not force selected ids");
}

}  // namespace

int main() {
  TestStatusDoesNotProxyWhileApplyIsPending();
  TestStatusReportsApplyFailureWithoutProxy();
  TestReadyStatusUsesCacheWithoutProxy();
  TestPlaneScopedContextRequestAddsPlaneAndSelectedKnowledge();
  TestPlaneScopedGraphRequestDefaultsToSelectedKnowledge();
  TestPlaneScopedSearchRequestKeepsExplicitBody();
  return 0;
}
