#include "naim/state/event_repository.h"

#include "naim/state/sqlite_statement.h"

namespace naim {

namespace {

using Statement = SqliteStatement;

std::string ToColumnText(sqlite3_stmt* statement, int column_index) {
  const unsigned char* text = sqlite3_column_text(statement, column_index);
  if (text == nullptr) {
    return "";
  }
  return reinterpret_cast<const char*>(text);
}

std::optional<int> ToOptionalColumnInt(sqlite3_stmt* statement, int column_index) {
  if (sqlite3_column_type(statement, column_index) == SQLITE_NULL) {
    return std::nullopt;
  }
  return sqlite3_column_int(statement, column_index);
}

}  // namespace

EventRepository::EventRepository(sqlite3* db) : db_(db) {}

void EventRepository::AppendEvent(const EventRecord& event) {
  Statement statement(
      db_,
      "INSERT INTO event_log("
      "plane_name, node_name, worker_name, assignment_id, rollout_action_id, "
      "category, event_type, severity, message, payload_json"
      ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10);");
  statement.BindText(1, event.plane_name);
  statement.BindText(2, event.node_name);
  statement.BindText(3, event.worker_name);
  statement.BindOptionalInt(4, event.assignment_id);
  statement.BindOptionalInt(5, event.rollout_action_id);
  statement.BindText(6, event.category);
  statement.BindText(7, event.event_type);
  statement.BindText(8, event.severity);
  statement.BindText(9, event.message);
  statement.BindText(10, event.payload_json);
  statement.StepDone();
}

std::vector<EventRecord> EventRepository::LoadEvents(
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit,
    const std::optional<int>& since_id,
    bool ascending) const {
  std::vector<std::string> clauses;
  if (plane_name.has_value()) {
    clauses.push_back("plane_name = ?" + std::to_string(clauses.size() + 1));
  }
  if (node_name.has_value()) {
    clauses.push_back("node_name = ?" + std::to_string(clauses.size() + 1));
  }
  if (worker_name.has_value()) {
    clauses.push_back("worker_name = ?" + std::to_string(clauses.size() + 1));
  }
  if (category.has_value()) {
    clauses.push_back("category = ?" + std::to_string(clauses.size() + 1));
  }
  if (since_id.has_value()) {
    clauses.push_back("id > ?" + std::to_string(clauses.size() + 1));
  }

  std::string sql =
      "SELECT id, plane_name, node_name, worker_name, assignment_id, rollout_action_id, "
      "category, event_type, severity, message, payload_json, created_at "
      "FROM event_log";
  if (!clauses.empty()) {
    sql += " WHERE ";
    for (std::size_t index = 0; index < clauses.size(); ++index) {
      if (index > 0) {
        sql += " AND ";
      }
      sql += clauses[index];
    }
  }
  sql += std::string(" ORDER BY id ") + (ascending ? "ASC" : "DESC") +
         " LIMIT ?" + std::to_string(clauses.size() + 1) + ";";

  Statement statement(db_, sql);
  int bind_index = 1;
  if (plane_name.has_value()) {
    statement.BindText(bind_index++, *plane_name);
  }
  if (node_name.has_value()) {
    statement.BindText(bind_index++, *node_name);
  }
  if (worker_name.has_value()) {
    statement.BindText(bind_index++, *worker_name);
  }
  if (category.has_value()) {
    statement.BindText(bind_index++, *category);
  }
  if (since_id.has_value()) {
    statement.BindInt(bind_index++, *since_id);
  }
  statement.BindInt(bind_index, limit > 0 ? limit : 100);

  std::vector<EventRecord> events;
  while (statement.StepRow()) {
    events.push_back(ReadEvent(statement.raw()));
  }
  return events;
}

EventRecord EventRepository::ReadEvent(sqlite3_stmt* statement) {
  return EventRecord{
      sqlite3_column_int(statement, 0),
      ToColumnText(statement, 1),
      ToColumnText(statement, 2),
      ToColumnText(statement, 3),
      ToOptionalColumnInt(statement, 4),
      ToOptionalColumnInt(statement, 5),
      ToColumnText(statement, 6),
      ToColumnText(statement, 7),
      ToColumnText(statement, 8),
      ToColumnText(statement, 9),
      ToColumnText(statement, 10),
      ToColumnText(statement, 11),
  };
}

}  // namespace naim
