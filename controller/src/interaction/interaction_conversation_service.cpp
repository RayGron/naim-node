#include "interaction/interaction_conversation_service.h"

#include <algorithm>
#include <filesystem>
#include <stdexcept>
#include <vector>

#include "app/controller_time_support.h"
#include "interaction/interaction_conversation_archive_service.h"
#include "interaction/interaction_conversation_payload_builder.h"
#include "interaction/interaction_conversation_record_builder.h"
#include "interaction/interaction_request_identity_support.h"
#include "skills/plane_skills_service.h"

namespace naim::controller {

using nlohmann::json;

std::optional<InteractionValidationError> InteractionConversationService::PrepareRequest(
    const std::string& db_path,
    const PlaneInteractionResolution& resolution,
    const InteractionConversationPrincipal& principal,
    InteractionRequestContext* context) const {
  const InteractionConversationArchiveService archive_service;
  const InteractionConversationPayloadBuilder payload_builder;
  const InteractionRequestIdentitySupport request_identity_support;
  if (context == nullptr) {
    throw std::invalid_argument("interaction request context is required");
  }

  archive_service.MaybeArchiveInactiveSessions(db_path);

  context->owner_kind = principal.owner_kind;
  context->owner_user_id = principal.owner_user_id;
  context->auth_session_kind = principal.auth_session_kind;
  context->delta_messages = context->client_messages;

  const std::string plane_name =
      resolution.status_payload.value("plane_name", std::string{});
  naim::ControllerStore store(db_path);
  store.Initialize();

  std::optional<naim::InteractionSessionRecord> session;
  std::vector<naim::InteractionMessageRecord> stored_records;
  std::vector<naim::InteractionSummaryRecord> summary_records;
  std::vector<json> stored_messages;

  if (context->requested_session_id.has_value()) {
    session = store.LoadInteractionSessionForOwner(
        plane_name,
        *context->requested_session_id,
        principal.owner_kind,
        principal.owner_user_id);
    if (!session.has_value()) {
      const auto owner_any_plane = store.LoadInteractionSessionForOwnerAnyPlane(
          *context->requested_session_id,
          principal.owner_kind,
          principal.owner_user_id);
      if (owner_any_plane.has_value() &&
          owner_any_plane->plane_name != plane_name) {
        return InteractionValidationError{
            "session_plane_mismatch",
            "conversation session belongs to a different plane",
            false,
            json::object(),
        };
      }
      if (const auto restore_error = archive_service.RestoreArchivedSession(
              store,
              plane_name,
              *context->requested_session_id,
              principal.owner_kind,
              principal.owner_user_id);
          !restore_error.has_value() ||
          restore_error->code != "session_not_found") {
        if (restore_error.has_value()) {
          return restore_error;
        }
        context->session_restored_from_archive = true;
        session = store.LoadInteractionSessionForOwner(
            plane_name,
            *context->requested_session_id,
            principal.owner_kind,
            principal.owner_user_id);
      }
    } else if (session->state == "archived") {
      if (const auto restore_error = archive_service.RestoreArchivedSession(
              store,
              plane_name,
              session->session_id,
              principal.owner_kind,
              principal.owner_user_id);
          restore_error.has_value()) {
        return restore_error;
      }
      context->session_restored_from_archive = true;
      session = store.LoadInteractionSessionForOwner(
          plane_name,
          session->session_id,
          principal.owner_kind,
          principal.owner_user_id);
    }

    if (!session.has_value()) {
      return InteractionValidationError{
          "session_not_found",
          "conversation session was not found",
          false,
          json::object(),
      };
    }

    stored_records = store.LoadInteractionMessages(session->session_id);
    summary_records = store.LoadInteractionSummaries(session->session_id);
    stored_messages = payload_builder.MessageRecordsToJson(stored_records);
    context->expected_session_version = session->version;
    context->conversation_session_id = session->session_id;
    context->session_context_state = payload_builder.ParseJsonObject(session->context_state_json);

    const std::size_t prefix =
        payload_builder.CommonPrefixLength(stored_messages, context->client_messages);
    if (prefix > 0 && context->client_messages.is_array()) {
      json delta = json::array();
      for (std::size_t index = prefix; index < context->client_messages.size(); ++index) {
        delta.push_back(context->client_messages[index]);
      }
      context->delta_messages = std::move(delta);
    }
    if (!context->delta_messages.is_array() || context->delta_messages.empty()) {
      return InteractionValidationError{
          "session_delta_invalid",
          "continuation request must include at least one new message",
          false,
          json::object(),
      };
    }
  } else {
    context->conversation_session_id =
        request_identity_support.GenerateSessionId();
    context->expected_session_version = 0;
    context->session_context_state = json::object();
  }

  context->payload["session_id"] = context->conversation_session_id;
  context->payload[kInteractionSessionContextStatePayloadKey] =
      context->session_context_state;
  context->payload["messages"] =
      payload_builder.BuildPromptMessages(
          summary_records, stored_messages, context->delta_messages);
  return std::nullopt;
}

std::optional<InteractionValidationError> InteractionConversationService::PersistResponse(
    const std::string& db_path,
    const PlaneInteractionResolution& resolution,
    InteractionRequestContext* context,
    const InteractionSessionResult& result) const {
  const InteractionConversationArchiveService archive_service;
  const InteractionConversationPayloadBuilder payload_builder;
  const InteractionConversationRecordBuilder record_builder;
  if (context == nullptr || context->conversation_session_id.empty() ||
      result.segments.empty()) {
    return std::nullopt;
  }

  const std::string plane_name =
      resolution.status_payload.value("plane_name", std::string{});
  const std::string now = ControllerTimeSupport::UtcNowSqlTimestamp();

  naim::ControllerStore store(db_path);
  store.Initialize();

  auto session = store.LoadInteractionSessionForOwner(
      plane_name,
      context->conversation_session_id,
      context->owner_kind,
      context->owner_user_id);

  std::vector<naim::InteractionMessageRecord> message_records =
      session.has_value() ? store.LoadInteractionMessages(session->session_id)
                          : std::vector<naim::InteractionMessageRecord>{};

  const json usage{
      {"prompt_tokens", result.total_prompt_tokens},
      {"completion_tokens", result.total_completion_tokens},
      {"total_tokens", result.total_tokens},
  };
  message_records = record_builder.AssignSessionId(
      record_builder.AppendNewMessageRecords(
          std::move(message_records),
          context->delta_messages,
          result.content,
          usage,
          now),
      context->conversation_session_id);

  std::vector<json> all_messages = payload_builder.MessageRecordsToJson(message_records);
  json context_state = context->payload.contains(kInteractionSessionContextStatePayloadKey) &&
                               context->payload.at(kInteractionSessionContextStatePayloadKey)
                                   .is_object()
                           ? context->payload.at(kInteractionSessionContextStatePayloadKey)
                           : context->session_context_state;
  json applied_skill_ids = json::array();
  if (context->payload.contains(PlaneSkillsService::kAppliedSkillsPayloadKey) &&
      context->payload.at(PlaneSkillsService::kAppliedSkillsPayloadKey).is_array()) {
    for (const auto& item : context->payload.at(PlaneSkillsService::kAppliedSkillsPayloadKey)) {
      if (item.is_object() && item.contains("id") && item.at("id").is_string()) {
        applied_skill_ids.push_back(item.at("id").get<std::string>());
      }
    }
  }
  context_state["applied_skill_ids"] = applied_skill_ids;

  const auto summary_records = payload_builder.BuildSummaryRecords(
      context->conversation_session_id,
      all_messages,
      context_state,
      now,
      std::max(1, resolution.desired_state.inference.max_model_len),
      context->payload.value("messages", json::array()));

  naim::InteractionSessionRecord updated;
  updated.session_id = context->conversation_session_id;
  updated.plane_name = plane_name;
  updated.owner_kind = context->owner_kind;
  updated.owner_user_id = context->owner_user_id;
  updated.auth_session_kind = context->auth_session_kind;
  updated.state = "active";
  updated.last_used_at = now;
  updated.archive_path = session.has_value() ? session->archive_path : "";
  updated.archive_codec = session.has_value() ? session->archive_codec : "";
  updated.archive_sha256 = session.has_value() ? session->archive_sha256 : "";
  updated.context_state_json = payload_builder.JsonString(context_state);
  updated.latest_prompt_tokens = result.total_prompt_tokens;
  updated.estimated_context_tokens =
      payload_builder.EstimateTokensForJson(
          context->payload.value("messages", json::array()));
  updated.compression_state = summary_records.empty() ? "none" : "compressed";
  updated.version = session.has_value() ? session->version + 1 : 1;
  updated.created_at = session.has_value() ? session->created_at : now;
  updated.updated_at = now;

  if (session.has_value()) {
    updated.archived_at.clear();
    if (!store.UpdateInteractionSessionVersioned(updated, session->version)) {
      return InteractionValidationError{
          "session_conflict",
          "conversation session was modified concurrently",
          true,
          json::object(),
      };
    }
  } else {
    store.UpsertInteractionSession(updated);
  }

  store.ReplaceInteractionMessages(updated.session_id, message_records);
  store.ReplaceInteractionSummaries(updated.session_id, summary_records);
  archive_service.MaybeArchiveInactiveSessions(db_path);
  return std::nullopt;
}

nlohmann::json InteractionConversationService::BuildSessionsListPayload(
    const std::string& db_path,
    const std::string& plane_name,
    int user_id) const {
  const InteractionConversationPayloadBuilder payload_builder;
  naim::ControllerStore store(db_path);
  store.Initialize();
  json sessions = json::array();
  for (const auto& session : store.LoadInteractionSessionsForUser(plane_name, user_id)) {
    const auto messages = store.LoadInteractionMessages(session.session_id);
    const auto summaries = store.LoadInteractionSummaries(session.session_id);
    sessions.push_back(
        payload_builder.BuildSessionSummaryPayload(
            session, messages.size(), summaries.size()));
  }
  return json{
      {"plane_name", plane_name},
      {"sessions", sessions},
  };
}

std::optional<nlohmann::json> InteractionConversationService::BuildSessionDetailPayload(
    const std::string& db_path,
    const std::string& plane_name,
    int user_id,
    const std::string& session_id) const {
  const InteractionConversationPayloadBuilder payload_builder;
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto session =
      store.LoadInteractionSessionForOwner(plane_name, session_id, "user", user_id);
  if (!session.has_value()) {
    return std::nullopt;
  }

  const auto messages = store.LoadInteractionMessages(session_id);
  const auto summaries = store.LoadInteractionSummaries(session_id);

  return json{
      {"session",
       payload_builder.BuildSessionSummaryPayload(
           *session, messages.size(), summaries.size())},
      {"messages", payload_builder.BuildSessionMessagesPayload(messages)},
      {"summaries", payload_builder.BuildSessionSummariesPayload(summaries)},
  };
}

bool InteractionConversationService::DeleteSession(
    const std::string& db_path,
    const std::string& plane_name,
    int user_id,
    const std::string& session_id) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto session =
      store.LoadInteractionSessionForOwner(plane_name, session_id, "user", user_id);
  const auto archive =
      store.LoadInteractionArchiveForOwner(plane_name, session_id, "user", user_id);
  if (!store.DeleteInteractionSessionForOwner(plane_name, session_id, "user", user_id)) {
    return false;
  }
  const std::string archive_path =
      archive.has_value() ? archive->archive_path
                          : (session.has_value() ? session->archive_path : "");
  if (!archive_path.empty()) {
    std::error_code error;
    std::filesystem::remove(std::filesystem::path(archive_path), error);
  }
  return true;
}

}  // namespace naim::controller
