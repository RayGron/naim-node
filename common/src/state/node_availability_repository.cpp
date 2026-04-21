#include "naim/state/node_availability_repository.h"

#include <string>

#include "naim/state/sqlite_statement.h"
#include "naim/state/sqlite_store_support.h"

namespace naim {

namespace {

using Statement = SqliteStatement;
using sqlite_store_support::AvailabilityOverrideFromStatement;

}  // namespace

NodeAvailabilityRepository::NodeAvailabilityRepository(sqlite3* db) : db_(db) {}

void NodeAvailabilityRepository::UpsertNodeAvailabilityOverride(
    const NodeAvailabilityOverride& availability_override) {
  Statement statement(
      db_,
      "INSERT INTO node_availability_overrides("
      "node_name, availability, status_message, updated_at"
      ") VALUES(?1, ?2, ?3, CURRENT_TIMESTAMP) "
      "ON CONFLICT(node_name) DO UPDATE SET "
      "availability = excluded.availability, "
      "status_message = excluded.status_message, "
      "updated_at = CURRENT_TIMESTAMP;");
  statement.BindText(1, availability_override.node_name);
  statement.BindText(2, ToString(availability_override.availability));
  statement.BindText(3, availability_override.status_message);
  statement.StepDone();
}

std::optional<NodeAvailabilityOverride> NodeAvailabilityRepository::LoadNodeAvailabilityOverride(
    const std::string& node_name) const {
  Statement statement(
      db_,
      "SELECT node_name, availability, status_message, updated_at "
      "FROM node_availability_overrides WHERE node_name = ?1;");
  statement.BindText(1, node_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return AvailabilityOverrideFromStatement(statement.raw());
}

std::vector<NodeAvailabilityOverride> NodeAvailabilityRepository::LoadNodeAvailabilityOverrides(
    const std::optional<std::string>& node_name) const {
  std::string sql =
      "SELECT node_name, availability, status_message, updated_at "
      "FROM node_availability_overrides";
  if (node_name.has_value()) {
    sql += " WHERE node_name = ?1";
  }
  sql += " ORDER BY node_name ASC;";

  Statement statement(db_, sql);
  if (node_name.has_value()) {
    statement.BindText(1, *node_name);
  }

  std::vector<NodeAvailabilityOverride> overrides;
  while (statement.StepRow()) {
    overrides.push_back(AvailabilityOverrideFromStatement(statement.raw()));
  }
  return overrides;
}

}  // namespace naim
