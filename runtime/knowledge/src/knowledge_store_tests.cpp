#include "knowledge/knowledge_store.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::filesystem::path TempStorePath() {
  const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
  auto root = std::filesystem::temp_directory_path() /
              ("naim-knowledge-store-test-" + std::to_string(stamp));
  std::filesystem::create_directories(root);
  return root / "store";
}

}  // namespace

int main() {
  const auto store_path = TempStorePath();
  naim::knowledge_runtime::KnowledgeStore store(store_path);
  store.Open();
  Expect(
      store.Status("kv-test").value("storage_engine", std::string{}) == "rocksdb",
      "canonical store should report rocksdb storage");

  const auto ingest = store.IngestSource(nlohmann::json{
      {"source_kind", "document"},
      {"source_ref", "doc://store-test"},
      {"content", "Knowledge store scheduled merge and Markdown export test."},
      {"scope_ids", nlohmann::json::array({"scope.default"})},
      {"metadata", nlohmann::json{{"title", "Store Test"}}},
  });
  Expect(ingest.value("status", std::string{}) == "accepted", "source ingest should accept");
  Expect(!ingest.value("chunks", nlohmann::json::array()).empty(), "source ingest should chunk content");
  Expect(!ingest.value("claims", nlohmann::json::array()).empty(), "source ingest should extract claims");

  const auto duplicate = store.IngestSource(nlohmann::json{
      {"source_kind", "document"},
      {"source_ref", "doc://store-test"},
      {"content", "Knowledge store scheduled merge and Markdown export test."},
      {"scope_ids", nlohmann::json::array({"scope.default"})},
  });
  Expect(duplicate.value("status", std::string{}) == "duplicate", "source ingest should deduplicate");

  const auto search = store.Search(nlohmann::json{
      {"query", "scheduled merge"},
      {"scope_id", "scope.default"},
  });
  Expect(!search.value("results", nlohmann::json::array()).empty(), "search should return result");

  const auto redacted = store.Search(nlohmann::json{
      {"query", "scheduled merge"},
      {"scope_id", "scope.other"},
  });
  Expect(
      redacted.at("results").front().at("redaction").value("reason", std::string{}) ==
          "out_of_scope",
      "search should redact out-of-scope result");

  const auto capsule = store.BuildCapsule(nlohmann::json{
      {"plane_id", "plane-test"},
      {"capsule_id", "cap-test"},
      {"included", nlohmann::json::array({ingest.value("source_block_id", std::string{})})},
  });
  Expect(capsule.value("capsule_id", std::string{}) == "cap-test", "capsule should build");
  Expect(
      store.ReadCapsule("cap-test").value("status", std::string{}) == "valid",
      "capsule should validate");
  Expect(
      capsule.at("manifest").value("storage_engine", std::string{}) == "rocksdb",
      "capsule manifest should advertise rocksdb-compatible storage");

  store.CatalogUpsert(nlohmann::json{
      {"object_id", ingest.value("source_block_id", std::string{})},
      {"shard_id", "storage-hpc1"},
      {"scope_ids", nlohmann::json::array({"scope.default"})},
      {"hints", nlohmann::json::array({"scheduled merge", "knowledge store"})},
      {"boundary_edges",
       nlohmann::json::array({nlohmann::json{
           {"edge_id", "edge-cross-shard"},
           {"from_object_id", ingest.value("source_block_id", std::string{})},
           {"to_object_id", "remote-block"},
           {"from_shard_id", "storage-hpc1"},
           {"to_shard_id", "storage-main"},
           {"type", "references"},
           {"scope_ids", nlohmann::json::array({"scope.default"})},
       }})},
      {"shard_health", nlohmann::json{{"status", "healthy"}, {"detail", nlohmann::json::object()}}},
  });
  const auto route = store.QueryRoute(nlohmann::json{
      {"query", "scheduled merge"},
      {"scope_ids", nlohmann::json::array({"scope.default"})},
  });
  Expect(!route.value("shard_requests", nlohmann::json::array()).empty(), "query route should plan shard fetches");

  store.ScheduleReplicaMerge(nlohmann::json{
      {"plane_id", "plane-test"},
      {"capsule_id", "cap-test"},
      {"cadence", "daily"},
  });
  store.WriteOverlay(nlohmann::json{
      {"plane_id", "plane-test"},
      {"capsule_id", "cap-test"},
      {"change_type", "claim_add"},
      {"base_versions", nlohmann::json::object()},
      {"proposed_blocks",
       nlohmann::json::array({nlohmann::json{
           {"knowledge_id", "knowledge.store-test"},
           {"title", "Merged Store Claim"},
           {"body", "Merged through scheduled replica reconciliation."},
           {"scope_ids", nlohmann::json::array({"scope.default"})},
       }})},
  });
  const auto due = store.RunScheduledReplicaMerges(nlohmann::json{
      {"plane_id", "plane-test"},
      {"force", true},
  });
  Expect(due.at("jobs").front().value("status", std::string{}) == "completed", "scheduled merge should complete");
  const auto daily = store.ReconcileDailyReplicaSchedules(nlohmann::json{{"plane_id", "plane-test"}});
  Expect(daily.value("status", std::string{}) == "completed", "daily reconcile should complete");

  store.WriteOverlay(nlohmann::json{
      {"plane_id", "plane-test"},
      {"capsule_id", "cap-test"},
      {"change_type", "claim_add"},
      {"confidence", 0.1},
      {"proposed_blocks", nlohmann::json::array()},
  });
  store.TriggerReplicaMerge(nlohmann::json{{"plane_id", "plane-test"}, {"capsule_id", "cap-test"}});
  const auto reviews = store.ListReviewItems(nlohmann::json{{"status", "pending"}});
  Expect(!reviews.value("items", nlohmann::json::array()).empty(), "low confidence overlay should enter review");

  const auto context = store.Context(nlohmann::json{
      {"query", "scheduled replica"},
      {"scope_id", "scope.default"},
      {"request_id", "req-store-test"},
      {"token_budget", 1200},
      {"plane_id", "plane-test"},
      {"capsule_id", "cap-test"},
  });
  Expect(!context.value("context", nlohmann::json::array()).empty(), "context should return bundle");

  const auto repair = store.RunRepair(nlohmann::json{{"apply", true}, {"full_rebuild", true}});
  Expect(repair.contains("report_id"), "repair should persist a report");

  const auto markdown = store.MarkdownExport(nlohmann::json{
      {"scope_ids", nlohmann::json::array({"scope.default"})},
  });
  Expect(!markdown.value("files", nlohmann::json::array()).empty(), "markdown export should produce files");
  Expect(
      markdown.at("files").front().value("content", std::string{}).find("related:") != std::string::npos,
      "markdown export should include Obsidian-compatible frontmatter");

  const auto import = store.MarkdownImport(nlohmann::json{
      {"plane_id", "plane-test"},
      {"capsule_id", "cap-test"},
      {"files",
       nlohmann::json::array({nlohmann::json{
           {"path", "Imported.md"},
           {"content", "# Imported\n\nImported proposal body."},
           {"scope_ids", nlohmann::json::array({"scope.default"})},
       }})},
  });
  Expect(
      !import.value("accepted_for_review", nlohmann::json::array()).empty(),
      "markdown import should create proposals");

  std::cout << "ok: knowledge-store-integration\n";
  return 0;
}
