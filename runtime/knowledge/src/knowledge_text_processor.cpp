#include "knowledge/knowledge_text_processor.h"

#include <algorithm>
#include <cctype>
#include <set>
#include <sstream>

#include <rocksdb/write_batch.h>

namespace naim::knowledge_runtime {

bool KnowledgeTextProcessor::JsonArrayContainsString(
    const nlohmann::json& values,
    const std::string& expected) {
  if (!values.is_array()) {
    return false;
  }
  for (const auto& value : values) {
    if (value.is_string() && value.get<std::string>() == expected) {
      return true;
    }
  }
  return false;
}

std::string KnowledgeTextProcessor::Lowercase(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

void KnowledgeTextProcessor::IndexTerms(
    rocksdb::WriteBatch& batch,
    const nlohmann::json& block) {
  const std::string block_id = block.value("block_id", std::string{});
  if (block_id.empty()) {
    return;
  }

  std::set<std::string> terms;
  auto add_terms = [&](const std::string& text) {
    std::string current;
    for (unsigned char ch : text) {
      if (std::isalnum(ch)) {
        current.push_back(static_cast<char>(std::tolower(ch)));
      } else if (!current.empty()) {
        terms.insert(current);
        current.clear();
      }
    }
    if (!current.empty()) {
      terms.insert(current);
    }
  };

  add_terms(block.value("title", std::string{}));
  add_terms(block.value("body", std::string{}));
  for (const auto& term : terms) {
    batch.Put("terms:" + term + ":" + block_id, block.dump());
  }
}

std::vector<nlohmann::json> KnowledgeTextProcessor::ChunkSource(
    const std::string& source_key,
    const std::string& block_id,
    const std::string& content) {
  std::vector<nlohmann::json> chunks;
  std::string current;
  int index = 0;
  auto flush = [&]() {
    if (current.empty()) {
      return;
    }
    chunks.push_back(nlohmann::json{
        {"chunk_id", source_key.substr(0, 16) + ".chunk." + std::to_string(index++)},
        {"source_key", source_key},
        {"block_id", block_id},
        {"text", current},
        {"token_estimate", std::max<std::size_t>(1, current.size() / 4)},
    });
    current.clear();
  };

  std::istringstream input(content);
  std::string line;
  while (std::getline(input, line)) {
    if (line.empty()) {
      flush();
      continue;
    }
    if (current.size() + line.size() > 1200) {
      flush();
    }
    if (!current.empty()) {
      current += "\n";
    }
    current += line;
  }
  flush();

  if (chunks.empty() && !content.empty()) {
    chunks.push_back(nlohmann::json{
        {"chunk_id", source_key.substr(0, 16) + ".chunk.0"},
        {"source_key", source_key},
        {"block_id", block_id},
        {"text", content},
        {"token_estimate", std::max<std::size_t>(1, content.size() / 4)},
    });
  }
  return chunks;
}

std::vector<nlohmann::json> KnowledgeTextProcessor::ExtractClaims(
    const std::string& source_key,
    const std::string& block_id,
    const std::vector<nlohmann::json>& chunks) {
  std::vector<nlohmann::json> claims;
  int index = 0;
  for (const auto& chunk : chunks) {
    std::istringstream input(chunk.value("text", std::string{}));
    std::string sentence;
    while (std::getline(input, sentence, '.')) {
      sentence.erase(
          sentence.begin(),
          std::find_if(sentence.begin(), sentence.end(), [](unsigned char ch) {
            return std::isspace(ch) == 0;
          }));
      if (sentence.size() < 16) {
        continue;
      }
      claims.push_back(nlohmann::json{
          {"claim_id", source_key.substr(0, 16) + ".claim." + std::to_string(index++)},
          {"source_key", source_key},
          {"block_id", block_id},
          {"text", sentence},
          {"confidence", 0.65},
      });
    }
  }
  return claims;
}

}  // namespace naim::knowledge_runtime
