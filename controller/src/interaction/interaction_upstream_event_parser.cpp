#include "interaction/interaction_upstream_event_parser.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <stdexcept>

namespace naim::controller {

std::string InteractionUpstreamEventParser::TrimCopy(
    const std::string& value) const {
  std::size_t start = 0;
  while (start < value.size() &&
         std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(start, end - start);
}

bool InteractionUpstreamEventParser::DecodeAvailableChunkedHttpBody(
    std::string& encoded,
    std::string* decoded,
    bool* stream_finished) const {
  bool progressed = false;
  while (true) {
    const std::size_t line_end = encoded.find("\r\n");
    if (line_end == std::string::npos) {
      return progressed;
    }
    std::string chunk_size_text = encoded.substr(0, line_end);
    const std::size_t extensions = chunk_size_text.find(';');
    if (extensions != std::string::npos) {
      chunk_size_text = chunk_size_text.substr(0, extensions);
    }
    chunk_size_text = TrimCopy(chunk_size_text);
    std::size_t chunk_size = 0;
    try {
      chunk_size =
          static_cast<std::size_t>(std::stoull(chunk_size_text, nullptr, 16));
    } catch (const std::exception&) {
      throw std::runtime_error(
          "invalid HTTP chunk size '" + chunk_size_text + "'");
    }

    const std::size_t chunk_data_begin = line_end + 2;
    if (chunk_size == 0) {
      if (encoded.size() < chunk_data_begin + 2) {
        return progressed;
      }
      encoded.erase(0, chunk_data_begin + 2);
      *stream_finished = true;
      return true;
    }

    if (encoded.size() < chunk_data_begin + chunk_size + 2) {
      return progressed;
    }
    decoded->append(encoded, chunk_data_begin, chunk_size);
    encoded.erase(0, chunk_data_begin + chunk_size + 2);
    progressed = true;
  }
}

std::string InteractionUpstreamEventParser::ExtractInteractionFinishReason(
    const nlohmann::json& payload) const {
  if (!payload.contains("choices") || !payload.at("choices").is_array() ||
      payload.at("choices").empty()) {
    return "stop";
  }
  const auto& choice = payload.at("choices").at(0);
  return choice.value("finish_reason", std::string{"stop"});
}

nlohmann::json InteractionUpstreamEventParser::ExtractInteractionUsage(
    const nlohmann::json& payload) const {
  if (!payload.contains("usage") || !payload.at("usage").is_object()) {
    return nlohmann::json{
        {"prompt_tokens", 0},
        {"completion_tokens", 0},
        {"total_tokens", 0},
    };
  }
  const auto& usage = payload.at("usage");
  return nlohmann::json{
      {"prompt_tokens", usage.value("prompt_tokens", 0)},
      {"completion_tokens", usage.value("completion_tokens", 0)},
      {"total_tokens", usage.value("total_tokens", 0)},
  };
}

bool InteractionUpstreamEventParser::TryConsumeSseFrame(
    std::string& buffer,
    InteractionSseFrame* frame) const {
  const std::size_t separator = buffer.find("\n\n");
  if (separator == std::string::npos) {
    return false;
  }
  const std::string raw_frame = buffer.substr(0, separator);
  buffer.erase(0, separator + 2);
  frame->event_name = "message";
  frame->data.clear();
  std::stringstream stream(raw_frame);
  std::string line;
  std::vector<std::string> data_lines;
  while (std::getline(stream, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    const std::string trimmed_line = TrimCopy(line);
    if (line.empty() || line[0] == ':') {
      continue;
    }
    if (!trimmed_line.empty()) {
      const std::size_t extensions = trimmed_line.find(';');
      const std::string chunk_size_candidate =
          TrimCopy(trimmed_line.substr(
              0, extensions == std::string::npos ? trimmed_line.size()
                                                 : extensions));
      const bool looks_like_chunk_size =
          !chunk_size_candidate.empty() &&
          std::all_of(
              chunk_size_candidate.begin(),
              chunk_size_candidate.end(),
              [](unsigned char ch) { return std::isxdigit(ch) != 0; });
      if (looks_like_chunk_size) {
        continue;
      }
    }
    if (line.rfind("event:", 0) == 0) {
      frame->event_name = TrimCopy(line.substr(6));
      continue;
    }
    if (line.rfind("data:", 0) == 0) {
      const std::size_t offset = line.size() > 5 && line[5] == ' ' ? 6 : 5;
      data_lines.push_back(line.substr(offset));
    }
  }
  for (std::size_t index = 0; index < data_lines.size(); ++index) {
    if (index > 0) {
      frame->data.push_back('\n');
    }
    frame->data += data_lines[index];
  }
  return true;
}

}  // namespace naim::controller
