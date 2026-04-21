#include "naim/runtime/model_adapter.h"

#include <algorithm>
#include <cctype>
#include <sstream>

namespace naim::runtime {

namespace {

using nlohmann::json;

std::string ToLowerCopy(std::string_view value) {
  std::string lowered;
  lowered.reserve(value.size());
  for (const unsigned char ch : value) {
    lowered.push_back(static_cast<char>(std::tolower(ch)));
  }
  return lowered;
}

bool ContainsCaseInsensitive(std::string_view haystack, std::string_view needle) {
  if (haystack.empty() || needle.empty()) {
    return false;
  }
  return ToLowerCopy(haystack).contains(ToLowerCopy(needle));
}

std::string TrimLeadingWhitespace(std::string value) {
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.front())) != 0) {
    value.erase(value.begin());
  }
  return value;
}

std::string Trim(std::string value) {
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.front())) != 0) {
    value.erase(value.begin());
  }
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.back())) != 0) {
    value.pop_back();
  }
  return value;
}

std::vector<std::string> JsonStringArray(const json& value, const char* key) {
  if (!value.contains(key) || !value.at(key).is_array()) {
    return {};
  }
  std::vector<std::string> result;
  for (const auto& item : value.at(key)) {
    if (item.is_string()) {
      result.push_back(item.get<std::string>());
    }
  }
  return result;
}

bool HasArgument(const std::vector<std::string>& args, std::string_view flag) {
  return std::any_of(
      args.begin(),
      args.end(),
      [&](const std::string& current) { return current == flag; });
}

std::string JsonString(const json& object, const char* key) {
  if (!object.contains(key) || object.at(key).is_null()) {
    return {};
  }
  if (object.at(key).is_string()) {
    return object.at(key).get<std::string>();
  }
  return object.at(key).dump();
}

std::string NormalizeChatRole(const std::string& role) {
  const std::string normalized = ToLowerCopy(role);
  if (normalized == "system" || normalized == "user" || normalized == "assistant" ||
      normalized == "tool") {
    return normalized;
  }
  return "user";
}

std::string StripRepeatedAssistantPrefixes(std::string value) {
  while (true) {
    const std::string trimmed = TrimLeadingWhitespace(value);
    if (trimmed.rfind("assistant:", 0) == 0) {
      value = trimmed.substr(std::string("assistant:").size());
      continue;
    }
    if (trimmed.rfind("assistant\n", 0) == 0) {
      value = trimmed.substr(std::string("assistant\n").size());
      continue;
    }
    if (trimmed.rfind("<|im_start|>assistant", 0) == 0) {
      value = trimmed.substr(std::string("<|im_start|>assistant").size());
      continue;
    }
    return TrimLeadingWhitespace(value);
  }
}

bool StartsWithAssistantMarker(const std::string& line) {
  const std::string trimmed = TrimLeadingWhitespace(line);
  return trimmed.rfind("assistant:", 0) == 0 ||
         trimmed.rfind("<|im_start|>assistant", 0) == 0 ||
         trimmed == "assistant";
}

std::string StripAssistantMarkerFromLine(const std::string& line) {
  std::string trimmed = TrimLeadingWhitespace(line);
  if (trimmed.rfind("assistant:", 0) == 0) {
    return Trim(trimmed.substr(std::string("assistant:").size()));
  }
  if (trimmed.rfind("<|im_start|>assistant", 0) == 0) {
    return Trim(trimmed.substr(std::string("<|im_start|>assistant").size()));
  }
  if (trimmed == "assistant") {
    return "";
  }
  return Trim(trimmed);
}

std::string CollapseAssistantTaggedTranscript(const std::string& value) {
  std::stringstream stream(value);
  std::string line;
  std::vector<std::string> cleaned_lines;
  bool saw_assistant_line = false;
  while (std::getline(stream, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (StartsWithAssistantMarker(line)) {
      saw_assistant_line = true;
    }
    if (!saw_assistant_line) {
      continue;
    }
    const std::string cleaned = StripAssistantMarkerFromLine(line);
    if (cleaned.empty()) {
      continue;
    }
    if (!cleaned_lines.empty() && cleaned_lines.back() == cleaned) {
      continue;
    }
    cleaned_lines.push_back(cleaned);
  }
  if (!saw_assistant_line || cleaned_lines.empty()) {
    return value;
  }
  std::ostringstream out;
  for (std::size_t index = 0; index < cleaned_lines.size(); ++index) {
    if (index > 0) {
      out << "\n";
    }
    out << cleaned_lines[index];
  }
  return out.str();
}

std::string TruncateAtFirstMarker(
    const std::string& value,
    const std::vector<std::string>& markers) {
  std::size_t cut = value.size();
  for (const auto& marker : markers) {
    const std::size_t pos = value.find(marker);
    if (pos != std::string::npos) {
      cut = std::min(cut, pos);
    }
  }
  return value.substr(0, cut);
}

std::string RemoveThinkBlocks(std::string value) {
  while (true) {
    const std::size_t begin = value.find("<think>");
    if (begin == std::string::npos) {
      return value;
    }
    const std::size_t end = value.find("</think>", begin);
    if (end == std::string::npos) {
      return value.substr(0, begin);
    }
    value.erase(begin, end + std::string("</think>").size() - begin);
  }
}

std::string StripLeadingGemmaChannelBlocks(std::string value) {
  while (true) {
    const std::string trimmed = TrimLeadingWhitespace(value);
    if (!trimmed.starts_with("<|channel>")) {
      return value;
    }
    const std::size_t close = trimmed.find("<channel|>");
    if (close == std::string::npos) {
      return value;
    }
    value = trimmed.substr(close + std::string("<channel|>").size());
  }
}

std::string RemoveGemmaChannelBlocks(std::string value) {
  constexpr std::string_view kOpen = "<|channel>";
  constexpr std::string_view kClose = "<channel|>";
  while (true) {
    const std::size_t begin = value.find(kOpen);
    if (begin == std::string::npos) {
      break;
    }
    const std::size_t end = value.find(kClose, begin + kOpen.size());
    if (end == std::string::npos) {
      value.erase(begin);
      break;
    }
    value.erase(begin, end + kClose.size() - begin);
  }
  while (true) {
    const std::size_t close = value.find(kClose);
    if (close == std::string::npos) {
      break;
    }
    value.erase(close, kClose.size());
  }
  return value;
}

}  // namespace

ModelIdentity ModelAdapter::IdentityFromActiveModelJson(const json& value) {
  ModelIdentity identity;
  identity.model_id = value.value("model_id", std::string{});
  identity.served_model_name = value.value("served_model_name", std::string{});
  identity.cached_local_model_path = value.value("cached_local_model_path", std::string{});
  identity.cached_runtime_model_path = value.value(
      "cached_runtime_model_path",
      value.value("model_path", std::string{}));
  identity.runtime_profile = value.value("runtime_profile", std::string{});
  identity.llama_args = JsonStringArray(value, "llama_args");
  return identity;
}

ModelFamily ModelAdapter::DetectFamily(const ModelIdentity& identity) {
  const auto looks_like = [&](std::string_view needle) {
    return ContainsCaseInsensitive(identity.model_id, needle) ||
           ContainsCaseInsensitive(identity.served_model_name, needle) ||
           ContainsCaseInsensitive(identity.cached_local_model_path, needle) ||
           ContainsCaseInsensitive(identity.cached_runtime_model_path, needle) ||
           ContainsCaseInsensitive(identity.runtime_profile, needle);
  };
  if (looks_like("gemma-4") || looks_like("gemma4")) {
    return ModelFamily::Gemma4;
  }
  if (looks_like("qwen")) {
    return ModelFamily::QwenLegacyChat;
  }
  return ModelFamily::Generic;
}

void ModelAdapter::AdaptInteractionPayload(
    json* payload,
    const ModelIdentity& identity,
    const ModelAdapterPolicy& policy) {
  if (payload == nullptr || !payload->is_object()) {
    return;
  }
  if (!payload->contains("chat_template_kwargs") ||
      !payload->at("chat_template_kwargs").is_object()) {
    (*payload)["chat_template_kwargs"] = json::object();
  }
  if (DetectFamily(identity) == ModelFamily::Gemma4) {
    (*payload)["chat_template_kwargs"]["enable_thinking"] = policy.thinking_enabled;
  }
}

void ModelAdapter::AdaptLaunchArgs(
    std::vector<std::string>* args,
    const ModelIdentity& identity) {
  if (args == nullptr) {
    return;
  }
  for (const auto& value : identity.llama_args) {
    args->push_back(value);
  }
  if (DetectFamily(identity) != ModelFamily::Gemma4) {
    return;
  }
  if (!HasArgument(*args, "--jinja") && !HasArgument(*args, "--no-jinja")) {
    args->push_back("--jinja");
  }
  if (!HasArgument(*args, "--chat-template") &&
      !HasArgument(*args, "--chat-template-file")) {
    args->push_back("--chat-template-file");
    args->push_back("/runtime/infer/templates/google-gemma-4.jinja");
  }
  if (!HasArgument(*args, "--chat-template-kwargs")) {
    args->push_back("--chat-template-kwargs");
    args->push_back(R"({"enable_thinking":false})");
  }
  if (!HasArgument(*args, "--reasoning-format")) {
    args->push_back("--reasoning-format");
    args->push_back("none");
  }
}

std::string ModelAdapter::BuildLegacyChatPrompt(
    const json& payload,
    const ModelIdentity& identity) {
  std::ostringstream prompt;
  const bool qwen = DetectFamily(identity) == ModelFamily::QwenLegacyChat;
  for (const auto& message : payload.at("messages")) {
    if (!message.is_object()) {
      continue;
    }
    const std::string content = JsonString(message, "content");
    if (content.empty()) {
      continue;
    }
    if (qwen) {
      prompt << "<|im_start|>" << NormalizeChatRole(message.value("role", std::string{"user"}))
             << "\n"
             << content << "<|im_end|>\n";
      continue;
    }
    prompt << message.value("role", std::string{"user"}) << ": " << content << "\n";
  }
  prompt << (qwen ? "<|im_start|>assistant\n" : "assistant: ");
  return prompt.str();
}

std::string ModelAdapter::SanitizeVisibleText(
    std::string text,
    const ModelIdentity& identity) {
  switch (DetectFamily(identity)) {
    case ModelFamily::Gemma4:
      text = RemoveThinkBlocks(std::move(text));
      text = RemoveGemmaChannelBlocks(std::move(text));
      text = StripLeadingGemmaChannelBlocks(std::move(text));
      text = TruncateAtFirstMarker(
          text,
          {
              "<|im_end|>",
              "<|endoftext|>",
              "<|eot_id|>",
              "\nuser:",
              "\nsystem:",
              "\n<|im_start|>user",
              "\n<|im_start|>system",
          });
      text = CollapseAssistantTaggedTranscript(text);
      text = StripRepeatedAssistantPrefixes(std::move(text));
      text = StripLeadingGemmaChannelBlocks(std::move(text));
      return Trim(text);
    case ModelFamily::QwenLegacyChat:
      text = RemoveThinkBlocks(std::move(text));
      text = TruncateAtFirstMarker(
          text,
          {
              "<|im_end|>",
              "<|endoftext|>",
              "<|eot_id|>",
              "\nuser:",
              "\nsystem:",
              "\n<|im_start|>user",
              "\n<|im_start|>system",
          });
      text = CollapseAssistantTaggedTranscript(text);
      text = StripRepeatedAssistantPrefixes(std::move(text));
      return Trim(text);
    case ModelFamily::Generic:
      return text;
  }
  return text;
}

void ModelAdapter::SanitizeChatCompletionPayload(
    json* payload,
    const ModelIdentity& identity) {
  if (payload == nullptr || !payload->is_object() || !payload->contains("choices") ||
      !payload->at("choices").is_array()) {
    return;
  }
  for (auto& choice : (*payload)["choices"]) {
    if (!choice.is_object()) {
      continue;
    }
    if (choice.contains("message") && choice.at("message").is_object()) {
      auto& message = choice["message"];
      if (message.contains("content") && message.at("content").is_string()) {
        message["content"] = SanitizeVisibleText(
            message.at("content").get<std::string>(), identity);
      }
      message.erase("reasoning_content");
    }
    if (choice.contains("text") && choice.at("text").is_string()) {
      choice["text"] = SanitizeVisibleText(choice.at("text").get<std::string>(), identity);
    }
    choice.erase("reasoning_content");
  }
}

std::string ToString(ModelFamily family) {
  switch (family) {
    case ModelFamily::Generic:
      return "generic";
    case ModelFamily::QwenLegacyChat:
      return "qwen_legacy_chat";
    case ModelFamily::Gemma4:
      return "gemma4";
  }
  return "generic";
}

}  // namespace naim::runtime
