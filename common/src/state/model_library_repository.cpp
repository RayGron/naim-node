#include "naim/state/model_library_repository.h"

#include "naim/state/sqlite_statement.h"
#include "naim/state/sqlite_store_support.h"

#include <cstdint>
#include <nlohmann/json.hpp>

namespace naim {

namespace {

using Statement = SqliteStatement;
using sqlite_store_support::ToColumnText;
using nlohmann::json;

}  // namespace

ModelLibraryRepository::ModelLibraryRepository(sqlite3* db) : db_(db) {}

void ModelLibraryRepository::UpsertModelLibraryDownloadJob(
    const ModelLibraryDownloadJobRecord& job) {
  Statement statement(
      db_,
      "INSERT INTO model_library_download_jobs("
      "id, job_kind, status, phase, model_id, node_name, target_root, target_subdir, detected_source_format, "
      "desired_output_format, source_urls_json, target_paths_json, quantizations_json, "
      "retained_output_paths_json, current_item, staging_directory, bytes_total, "
      "bytes_done, part_count, keep_base_gguf, error_message, hidden, created_at, updated_at) "
      "VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24) "
      "ON CONFLICT(id) DO UPDATE SET "
      "job_kind = excluded.job_kind, "
      "status = excluded.status, "
      "phase = excluded.phase, "
      "model_id = excluded.model_id, "
      "node_name = excluded.node_name, "
      "target_root = excluded.target_root, "
      "target_subdir = excluded.target_subdir, "
      "detected_source_format = excluded.detected_source_format, "
      "desired_output_format = excluded.desired_output_format, "
      "source_urls_json = excluded.source_urls_json, "
      "target_paths_json = excluded.target_paths_json, "
      "quantizations_json = excluded.quantizations_json, "
      "retained_output_paths_json = excluded.retained_output_paths_json, "
      "current_item = excluded.current_item, "
      "staging_directory = excluded.staging_directory, "
      "bytes_total = excluded.bytes_total, "
      "bytes_done = excluded.bytes_done, "
      "part_count = excluded.part_count, "
      "keep_base_gguf = excluded.keep_base_gguf, "
      "error_message = excluded.error_message, "
      "hidden = excluded.hidden, "
      "created_at = excluded.created_at, "
      "updated_at = excluded.updated_at;");
  statement.BindText(1, job.id);
  statement.BindText(2, job.job_kind);
  statement.BindText(3, job.status);
  statement.BindText(4, job.phase);
  statement.BindText(5, job.model_id);
  statement.BindText(6, job.node_name);
  statement.BindText(7, job.target_root);
  statement.BindText(8, job.target_subdir);
  statement.BindText(9, job.detected_source_format);
  statement.BindText(10, job.desired_output_format);
  statement.BindText(11, SerializeStringArray(job.source_urls));
  statement.BindText(12, SerializeStringArray(job.target_paths));
  statement.BindText(13, SerializeStringArray(job.quantizations));
  statement.BindText(14, SerializeStringArray(job.retained_output_paths));
  statement.BindText(15, job.current_item);
  statement.BindText(16, job.staging_directory);
  statement.BindOptionalInt64(
      17,
      job.bytes_total.has_value()
          ? std::optional<std::int64_t>(
                static_cast<std::int64_t>(*job.bytes_total))
          : std::nullopt);
  statement.BindInt64(18, static_cast<std::int64_t>(job.bytes_done));
  statement.BindInt(19, job.part_count);
  statement.BindInt(20, job.keep_base_gguf ? 1 : 0);
  statement.BindText(21, job.error_message);
  statement.BindInt(22, job.hidden ? 1 : 0);
  statement.BindText(23, job.created_at);
  statement.BindText(24, job.updated_at);
  statement.StepDone();
}

std::optional<ModelLibraryDownloadJobRecord>
ModelLibraryRepository::LoadModelLibraryDownloadJob(
    const std::string& job_id) const {
  Statement statement(
      db_,
      "SELECT id, job_kind, status, phase, model_id, node_name, target_root, target_subdir, detected_source_format, "
      "desired_output_format, source_urls_json, target_paths_json, quantizations_json, "
      "retained_output_paths_json, current_item, staging_directory, bytes_total, bytes_done, "
      "part_count, keep_base_gguf, error_message, hidden, created_at, updated_at "
      "FROM model_library_download_jobs WHERE id = ?1;");
  statement.BindText(1, job_id);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ReadModelLibraryDownloadJob(statement.raw());
}

std::vector<ModelLibraryDownloadJobRecord>
ModelLibraryRepository::LoadModelLibraryDownloadJobs(
    const std::optional<std::string>& status) const {
  std::string sql =
      "SELECT id, job_kind, status, phase, model_id, node_name, target_root, target_subdir, detected_source_format, "
      "desired_output_format, source_urls_json, target_paths_json, quantizations_json, "
      "retained_output_paths_json, current_item, staging_directory, bytes_total, bytes_done, "
      "part_count, keep_base_gguf, error_message, hidden, created_at, updated_at "
      "FROM model_library_download_jobs";
  if (status.has_value()) {
    sql += " WHERE status = ?1";
  }
  sql += " ORDER BY created_at DESC, id DESC;";
  Statement statement(db_, sql);
  if (status.has_value()) {
    statement.BindText(1, *status);
  }
  std::vector<ModelLibraryDownloadJobRecord> jobs;
  while (statement.StepRow()) {
    jobs.push_back(ReadModelLibraryDownloadJob(statement.raw()));
  }
  return jobs;
}

bool ModelLibraryRepository::DeleteModelLibraryDownloadJob(
    const std::string& job_id) {
  Statement statement(
      db_,
      "DELETE FROM model_library_download_jobs WHERE id = ?1;");
  statement.BindText(1, job_id);
  statement.StepDone();
  return sqlite3_changes(db_) > 0;
}

std::string ModelLibraryRepository::SerializeStringArray(
    const std::vector<std::string>& values) {
  return json(values).dump();
}

std::vector<std::string> ModelLibraryRepository::ParseStringArray(
    const std::string& payload) {
  if (payload.empty()) {
    return {};
  }
  try {
    const json parsed = json::parse(payload);
    if (!parsed.is_array()) {
      return {};
    }
    return parsed.get<std::vector<std::string>>();
  } catch (...) {
    return {};
  }
}

ModelLibraryDownloadJobRecord
ModelLibraryRepository::ReadModelLibraryDownloadJob(sqlite3_stmt* statement) {
  ModelLibraryDownloadJobRecord job;
  job.id = ToColumnText(statement, 0);
  job.job_kind = ToColumnText(statement, 1);
  job.status = ToColumnText(statement, 2);
  job.phase = ToColumnText(statement, 3);
  job.model_id = ToColumnText(statement, 4);
  job.node_name = ToColumnText(statement, 5);
  job.target_root = ToColumnText(statement, 6);
  job.target_subdir = ToColumnText(statement, 7);
  job.detected_source_format = ToColumnText(statement, 8);
  job.desired_output_format = ToColumnText(statement, 9);
  job.source_urls = ParseStringArray(ToColumnText(statement, 10));
  job.target_paths = ParseStringArray(ToColumnText(statement, 11));
  job.quantizations = ParseStringArray(ToColumnText(statement, 12));
  job.retained_output_paths = ParseStringArray(ToColumnText(statement, 13));
  job.current_item = ToColumnText(statement, 14);
  job.staging_directory = ToColumnText(statement, 15);
  if (sqlite3_column_type(statement, 16) != SQLITE_NULL) {
    job.bytes_total =
        static_cast<std::uintmax_t>(sqlite3_column_int64(statement, 16));
  }
  job.bytes_done = static_cast<std::uintmax_t>(sqlite3_column_int64(statement, 17));
  job.part_count = sqlite3_column_int(statement, 18);
  job.keep_base_gguf = sqlite3_column_int(statement, 19) != 0;
  job.error_message = ToColumnText(statement, 20);
  job.hidden = sqlite3_column_int(statement, 21) != 0;
  job.created_at = ToColumnText(statement, 22);
  job.updated_at = ToColumnText(statement, 23);
  return job;
}

}  // namespace naim
