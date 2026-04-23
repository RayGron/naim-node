#include "interaction/interaction_context_compression_service.h"

#include <algorithm>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "interaction/interaction_conversation_payload_builder.h"

namespace naim::controller {

namespace {

using nlohmann::json;

constexpr const char* kKnowledgeSystemInstructionPayloadKey =
    "__naim_knowledge_system_instruction";
constexpr const char* kKnowledgeContextPayloadKey = "__naim_knowledge_context";
constexpr int kDialogRecentTailCount = 6;
constexpr int kSummaryExcerptBytes = 200;
constexpr int kKnowledgeExcerptBytes = 800;

std::string TrimCopy(const std::string& value) {
  const auto begin = value.find_first_not_of(" \t\r\n");
  if (begin == std::string::npos) {
    return "";
  }
  const auto end = value.find_last_not_of(" \t\r\n");
  return value.substr(begin, end - begin + 1);
}

std::string JsonString(const json& value) {
  return value.dump(-1, ' ', false, json::error_handler_t::replace);
}

std::string ExcerptText(const json& message, std::size_t max_bytes) {
  std::string text;
  if (message.is_object()) {
    if (message.contains("content") && message.at("content").is_string()) {
      text = message.at("content").get<std::string>();
    } else if (message.contains("content")) {
      text = JsonString(message.at("content"));
    }
  } else if (message.is_string()) {
    text = message.get<std::string>();
  } else {
    text = JsonString(message);
  }
  text = TrimCopy(text);
  if (text.size() > max_bytes) {
    text.resize(max_bytes);
    text += "...[truncated]";
  }
  return text;
}

std::string ItemIdentityKey(const json& item) {
  if (!item.is_object()) {
    return JsonString(item);
  }
  const std::string knowledge_id = item.value("knowledge_id", std::string{});
  const std::string block_id = item.value("block_id", std::string{});
  const std::string version_id = item.value("version_id", std::string{});
  const std::string content_hash = item.value("content_hash", std::string{});
  if (!knowledge_id.empty() || !block_id.empty() || !version_id.empty() || !content_hash.empty()) {
    return knowledge_id + "|" + block_id + "|" + version_id + "|" + content_hash;
  }
  return item.value("title", std::string{}) + "|" + item.value("text", std::string{});
}

double ItemRank(const json& item) {
  if (!item.is_object()) {
    return 0.0;
  }
  double rank = 0.0;
  if (item.contains("score") && item.at("score").is_number()) {
    rank += item.at("score").get<double>() * 1000.0;
  }
  if (item.contains("relevance") && item.at("relevance").is_number()) {
    rank += item.at("relevance").get<double>() * 100.0;
  }
  if (item.contains("confidence") && item.at("confidence").is_number()) {
    rank += item.at("confidence").get<double>() * 10.0;
  }
  if (item.contains("relation_distance") && item.at("relation_distance").is_number_integer()) {
    rank -= static_cast<double>(item.at("relation_distance").get<int>());
  }
  return rank;
}

json BuildDialogSummaryMessage(
    const std::vector<json>& messages,
    std::size_t max_items) {
  std::ostringstream summary;
  summary << "Context Compression summary of older dialog turns. Preserve decisions, stable facts, "
             "and unresolved threads from this compressed history.";
  std::size_t emitted = 0;
  for (const auto& message : messages) {
    if (!message.is_object()) {
      continue;
    }
    const std::string role = message.value("role", std::string{});
    if (role.empty() || role == "system") {
      continue;
    }
    const std::string excerpt = ExcerptText(message, kSummaryExcerptBytes);
    if (excerpt.empty()) {
      continue;
    }
    summary << "\n- " << role << ": " << excerpt;
    ++emitted;
    if (emitted >= max_items) {
      break;
    }
  }
  return json{{"role", "system"}, {"content", summary.str()}};
}

std::string BuildKnowledgeInstruction(const json& context_payload) {
  const auto context = context_payload.value("context", json::array());
  if (!context.is_array() || context.empty()) {
    return {};
  }
  std::string instruction =
      "Knowledge Base context: use the following canonical knowledge snippets when "
      "they are relevant. Preserve provenance and do not invent facts outside this context.";
  int index = 1;
  for (const auto& item : context) {
    if (!item.is_object()) {
      continue;
    }
    instruction += "\n\n[" + std::to_string(index) + "] ";
    instruction += item.value("title", item.value("knowledge_id", std::string("knowledge")));
    instruction += "\nknowledge_id: " + item.value("knowledge_id", std::string{});
    instruction += "\nblock_id: " + item.value("block_id", std::string{});
    instruction += "\ntext: " + item.value("text", std::string{});
    ++index;
  }
  return instruction;
}

json EnsureObject(const json& value) {
  return value.is_object() ? value : json::object();
}

}  // namespace

void InteractionContextCompressionService::Apply(
    const PlaneInteractionResolution& resolution,
    InteractionRequestContext* request_context) const {
  if (request_context == nullptr ||
      !resolution.desired_state.context_compression.has_value() ||
      !resolution.desired_state.context_compression->enabled) {
    return;
  }

  const auto& feature = *resolution.desired_state.context_compression;
  InteractionConversationPayloadBuilder payload_builder;
  json context_state = request_context->payload.contains(kInteractionSessionContextStatePayloadKey) &&
                               request_context->payload.at(kInteractionSessionContextStatePayloadKey)
                                   .is_object()
                           ? request_context->payload.at(kInteractionSessionContextStatePayloadKey)
                           : request_context->session_context_state;
  context_state = EnsureObject(context_state);

  json compression_state = {
      {"enabled", true},
      {"mode", feature.mode},
      {"target", feature.target},
      {"memory_priority", feature.memory_priority},
      {"compressor_id", "context-compression-v1"},
      {"policy_version", "v1"},
      {"warnings", json::array()},
      {"status", "none"},
  };

  json messages =
      request_context->payload.contains("messages") && request_context->payload.at("messages").is_array()
          ? request_context->payload.at("messages")
          : json::array();
  const int dialog_estimate_before = payload_builder.EstimateTokensForJson(messages);
  bool dialog_compressed = false;
  bool dialog_fallback_truncated = false;

  if (messages.is_array()) {
    std::vector<json> system_messages;
    std::vector<json> non_system_messages;
    for (const auto& message : messages) {
      if (message.is_object() && message.value("role", std::string{}) == "system") {
        system_messages.push_back(message);
      } else {
        non_system_messages.push_back(message);
      }
    }

    const int dialog_soft_limit =
        std::max(256, (resolution.desired_state.inference.max_model_len * 55) / 100);
    if (dialog_estimate_before > dialog_soft_limit &&
        non_system_messages.size() > static_cast<std::size_t>(kDialogRecentTailCount)) {
      const std::size_t older_count =
          non_system_messages.size() - static_cast<std::size_t>(kDialogRecentTailCount);
      std::vector<json> older_messages(
          non_system_messages.begin(),
          non_system_messages.begin() + static_cast<std::ptrdiff_t>(older_count));
      std::vector<json> recent_messages(
          non_system_messages.end() - static_cast<std::ptrdiff_t>(kDialogRecentTailCount),
          non_system_messages.end());

      json compressed_messages = json::array();
      for (const auto& message : system_messages) {
        compressed_messages.push_back(message);
      }
      compressed_messages.push_back(BuildDialogSummaryMessage(older_messages, 10));
      for (const auto& message : recent_messages) {
        compressed_messages.push_back(message);
      }

      const int compressed_estimate = payload_builder.EstimateTokensForJson(compressed_messages);
      if (compressed_estimate < dialog_estimate_before) {
        messages = std::move(compressed_messages);
        dialog_compressed = true;
      } else {
        json fallback_messages = json::array();
        for (const auto& message : system_messages) {
          fallback_messages.push_back(message);
        }
        for (auto it = non_system_messages.end() - static_cast<std::ptrdiff_t>(std::min<std::size_t>(
                 non_system_messages.size(),
                 4));
             it != non_system_messages.end();
             ++it) {
          fallback_messages.push_back(*it);
        }
        messages = std::move(fallback_messages);
        dialog_fallback_truncated = true;
        compression_state["warnings"].push_back(
            "dialog context exceeded soft budget and used fallback truncation");
      }
    }
  }

  request_context->payload["messages"] = messages;
  const int dialog_estimate_after = payload_builder.EstimateTokensForJson(messages);

  json knowledge_context = request_context->payload.contains(kKnowledgeContextPayloadKey) &&
                                   request_context->payload.at(kKnowledgeContextPayloadKey).is_object()
                               ? request_context->payload.at(kKnowledgeContextPayloadKey)
                               : json::object();
  std::size_t knowledge_entries_before = 0;
  std::size_t knowledge_entries_after = 0;
  std::size_t knowledge_bytes_before = JsonString(knowledge_context).size();
  std::size_t knowledge_bytes_after = knowledge_bytes_before;
  bool knowledge_compressed = false;

  if (knowledge_context.contains("context") && knowledge_context.at("context").is_array()) {
    std::vector<json> entries;
    for (const auto& item : knowledge_context.at("context")) {
      if (item.is_object()) {
        entries.push_back(item);
      }
    }
    knowledge_entries_before = entries.size();
    std::stable_sort(
        entries.begin(),
        entries.end(),
        [](const json& left, const json& right) { return ItemRank(left) > ItemRank(right); });
    std::set<std::string> seen_keys;
    json trimmed_entries = json::array();
    const int knowledge_token_budget = std::max(
        256,
        std::min(
            resolution.desired_state.knowledge.has_value()
                ? resolution.desired_state.knowledge->context_policy.token_budget
                : 12000,
            std::max(512, resolution.desired_state.inference.max_model_len / 3)));
    int consumed_tokens = 0;
    for (const auto& item : entries) {
      const std::string identity_key = ItemIdentityKey(item);
      if (!seen_keys.insert(identity_key).second) {
        continue;
      }
      json trimmed = item;
      if (trimmed.contains("text") && trimmed.at("text").is_string()) {
        trimmed["text"] = ExcerptText(trimmed.at("text"), kKnowledgeExcerptBytes);
      }
      const int item_tokens = payload_builder.EstimateTokensForJson(trimmed);
      if (!trimmed_entries.empty() && consumed_tokens + item_tokens > knowledge_token_budget) {
        compression_state["warnings"].push_back(
            "knowledge context trimmed to fit compression budget");
        break;
      }
      consumed_tokens += item_tokens;
      trimmed_entries.push_back(std::move(trimmed));
    }
    knowledge_entries_after = trimmed_entries.size();
    knowledge_context["context"] = std::move(trimmed_entries);
    request_context->payload[kKnowledgeContextPayloadKey] = knowledge_context;
    request_context->payload[kKnowledgeSystemInstructionPayloadKey] =
        BuildKnowledgeInstruction(knowledge_context);
    knowledge_bytes_after = JsonString(knowledge_context).size();
    knowledge_compressed = knowledge_entries_after < knowledge_entries_before ||
                           knowledge_bytes_after < knowledge_bytes_before;
  }

  if (dialog_fallback_truncated) {
    compression_state["status"] = "fallback_truncated";
  } else if (dialog_compressed || knowledge_compressed) {
    compression_state["status"] = "compressed";
  }
  compression_state["dialog_estimate_before"] = dialog_estimate_before;
  compression_state["dialog_estimate_after"] = dialog_estimate_after;
  compression_state["knowledge_entries_before"] = static_cast<int>(knowledge_entries_before);
  compression_state["knowledge_entries_after"] = static_cast<int>(knowledge_entries_after);
  compression_state["knowledge_bytes_before"] = static_cast<int>(knowledge_bytes_before);
  compression_state["knowledge_bytes_after"] = static_cast<int>(knowledge_bytes_after);
  compression_state["compression_ratio"] =
      dialog_estimate_before > 0
          ? static_cast<double>(dialog_estimate_after) /
                static_cast<double>(dialog_estimate_before)
          : 1.0;

  context_state["context_compression"] = std::move(compression_state);
  request_context->session_context_state = context_state;
  request_context->payload[kInteractionSessionContextStatePayloadKey] = context_state;
}

}  // namespace naim::controller
