#include "skills/plane_skill_contextual_resolver_service.h"

#include <algorithm>
#include <cctype>
#include <set>
#include <sstream>
#include <stdexcept>
#include <unordered_set>
#include <utility>

#include "http/controller_http_transport.h"

namespace comet::controller {

namespace {

struct ContextualSkillCandidate {
  std::string id;
  std::string name;
  std::string description;
  std::string content;
};

constexpr int kMinimumContextualScore = 6;
constexpr std::size_t kMaximumSelectedSkills = 3;

std::vector<std::pair<std::string, std::string>> DefaultJsonHeaders() {
  return {{"Content-Type", "application/json"}};
}

std::string NormalizeControllerTargetHost(const PublishedPort& port) {
  if (port.host_ip.empty() || port.host_ip == "0.0.0.0") {
    return "127.0.0.1";
  }
  return port.host_ip;
}

const InstanceSpec* FindSkillsInstance(const DesiredState& desired_state) {
  const auto it = std::find_if(
      desired_state.instances.begin(),
      desired_state.instances.end(),
      [](const InstanceSpec& instance) {
        return instance.role == InstanceRole::Skills;
      });
  if (it == desired_state.instances.end()) {
    return nullptr;
  }
  return &*it;
}

std::optional<ControllerEndpointTarget> ResolvePlaneLocalSkillsTarget(
    const DesiredState& desired_state) {
  const auto* skills = FindSkillsInstance(desired_state);
  if (skills == nullptr) {
    return std::nullopt;
  }
  const auto published = std::find_if(
      skills->published_ports.begin(),
      skills->published_ports.end(),
      [](const PublishedPort& port) { return port.host_port > 0; });
  if (published == skills->published_ports.end()) {
    return std::nullopt;
  }
  ControllerEndpointTarget target;
  target.host = NormalizeControllerTargetHost(*published);
  target.port = published->host_port;
  target.raw = "http://" + target.host + ":" + std::to_string(target.port);
  return target;
}

std::string NormalizeSkillText(const std::string& value) {
  std::string normalized;
  normalized.reserve(value.size());
  for (unsigned char ch : value) {
    if (std::isalnum(ch) != 0) {
      normalized.push_back(static_cast<char>(std::tolower(ch)));
    } else {
      normalized.push_back(' ');
    }
  }
  return normalized;
}

std::vector<std::string> TokenizeRelevantTerms(const std::string& value) {
  static const std::unordered_set<std::string> kIgnoredTokens = {
      "about",  "agent",   "apply",   "available", "build",   "catalog",
      "change", "code",    "context", "controller","debug",   "default",
      "enabled","field",   "from",    "have",      "interaction",
      "into",   "local",   "mode",    "must",      "plane",   "request",
      "response","runtime","service", "should",    "skill",   "skills",
      "source", "state",   "that",    "their",     "this",    "through",
      "when",   "with",    "without",
  };

  std::vector<std::string> tokens;
  std::set<std::string> seen;
  std::stringstream stream(NormalizeSkillText(value));
  std::string token;
  while (stream >> token) {
    if (token.size() < 4 || kIgnoredTokens.count(token) > 0) {
      continue;
    }
    if (seen.insert(token).second) {
      tokens.push_back(token);
    }
  }
  return tokens;
}

std::string JoinTerms(const std::vector<std::string>& items) {
  if (items.empty()) {
    return "";
  }
  std::string joined;
  for (std::size_t index = 0; index < items.size(); ++index) {
    if (index > 0) {
      joined += ", ";
    }
    joined += items[index];
  }
  return joined;
}

std::vector<ContextualSkillCandidate> LoadPlaneLocalCandidates(
    const DesiredState& desired_state) {
  if (!desired_state.skills.has_value() || !desired_state.skills->enabled) {
    return {};
  }

  const auto target = ResolvePlaneLocalSkillsTarget(desired_state);
  if (!target.has_value()) {
    return {};
  }

  HttpResponse response;
  try {
    response = SendControllerHttpRequest(
        *target, "GET", "/v1/skills", "", DefaultJsonHeaders());
  } catch (const std::exception&) {
    return {};
  }
  if (response.status_code != 200 || response.body.empty()) {
    return {};
  }

  const auto payload = nlohmann::json::parse(response.body, nullptr, false);
  if (payload.is_discarded() || !payload.is_object() ||
      !payload.contains("skills") || !payload.at("skills").is_array()) {
    return {};
  }

  std::vector<ContextualSkillCandidate> candidates;
  for (const auto& item : payload.at("skills")) {
    if (!item.is_object()) {
      continue;
    }
    if (item.contains("enabled") && item.at("enabled").is_boolean() &&
        !item.at("enabled").get<bool>()) {
      continue;
    }
    candidates.push_back(ContextualSkillCandidate{
        item.value("id", std::string{}),
        item.value("name", std::string{}),
        item.value("description", std::string{}),
        item.value("content", std::string{}),
    });
  }
  return candidates;
}

int ScoreCandidate(
    const std::string& prompt_text,
    const ContextualSkillCandidate& candidate,
    std::string* rationale) {
  const std::string prompt_normalized = NormalizeSkillText(prompt_text);
  const auto prompt_terms = TokenizeRelevantTerms(prompt_text);
  const auto name_terms = TokenizeRelevantTerms(candidate.name);
  const auto description_terms = TokenizeRelevantTerms(candidate.description);
  const auto content_terms = TokenizeRelevantTerms(candidate.content);

  int score = 0;
  std::vector<std::string> matched_name_terms;
  std::vector<std::string> matched_description_terms;
  std::vector<std::string> matched_content_terms;

  const std::string full_name = NormalizeSkillText(candidate.name);
  if (!candidate.name.empty() &&
      prompt_normalized.find(full_name) != std::string::npos) {
    score += 12;
  }

  for (const auto& term : prompt_terms) {
    if (std::find(name_terms.begin(), name_terms.end(), term) != name_terms.end()) {
      matched_name_terms.push_back(term);
      score += 5;
      continue;
    }
    if (std::find(description_terms.begin(), description_terms.end(), term) !=
        description_terms.end()) {
      matched_description_terms.push_back(term);
      score += 2;
      continue;
    }
    if (std::find(content_terms.begin(), content_terms.end(), term) !=
        content_terms.end()) {
      matched_content_terms.push_back(term);
      score += 1;
    }
  }

  if (matched_description_terms.size() > 3) {
    score -= static_cast<int>(matched_description_terms.size() - 3);
  }
  if (matched_content_terms.size() > 4) {
    score -= static_cast<int>(matched_content_terms.size() - 4);
  }

  if (rationale != nullptr) {
    std::vector<std::string> parts;
    if (!matched_name_terms.empty()) {
      parts.push_back("name terms: " + JoinTerms(matched_name_terms));
    }
    if (!matched_description_terms.empty()) {
      parts.push_back(
          "description terms: " + JoinTerms(matched_description_terms));
    }
    if (!matched_content_terms.empty()) {
      parts.push_back("content terms: " + JoinTerms(matched_content_terms));
    }
    *rationale = JoinTerms(parts);
  }

  return score;
}

}  // namespace

std::string PlaneSkillContextualResolverService::ExtractPromptText(
    const nlohmann::json& payload) const {
  if (payload.contains("prompt") && !payload.at("prompt").is_null()) {
    if (!payload.at("prompt").is_string()) {
      throw std::invalid_argument("prompt must be a string when provided");
    }
    return payload.at("prompt").get<std::string>();
  }
  if (!payload.contains("messages") || !payload.at("messages").is_array()) {
    throw std::invalid_argument(
        "contextual skill resolution requires either messages or prompt");
  }

  std::string text;
  for (const auto& message : payload.at("messages")) {
    if (!message.is_object() ||
        message.value("role", std::string{}) != "user" ||
        !message.contains("content") ||
        !message.at("content").is_string()) {
      continue;
    }
    if (!text.empty()) {
      text += "\n";
    }
    text += message.at("content").get<std::string>();
  }
  return text;
}

ContextualSkillSelection PlaneSkillContextualResolverService::Resolve(
    const std::string& db_path,
    const PlaneInteractionResolution& resolution,
    const nlohmann::json& payload) const {
  (void)db_path;
  ContextualSkillSelection selection;
  if (!resolution.desired_state.skills.has_value() ||
      !resolution.desired_state.skills->enabled) {
    return selection;
  }

  const auto prompt_text = ExtractPromptText(payload);
  const auto candidates = LoadPlaneLocalCandidates(resolution.desired_state);
  selection.candidate_count = static_cast<int>(candidates.size());
  if (prompt_text.empty() || candidates.empty()) {
    return selection;
  }

  std::vector<std::pair<int, nlohmann::json>> scored;
  for (const auto& candidate : candidates) {
    std::string rationale;
    const int score = ScoreCandidate(prompt_text, candidate, &rationale);
    if (score < kMinimumContextualScore) {
      continue;
    }
    scored.push_back({
        score,
        nlohmann::json{
            {"id", candidate.id},
            {"name", candidate.name},
            {"source", "contextual"},
            {"score", score},
            {"rationale", rationale},
        },
    });
  }

  std::sort(
      scored.begin(),
      scored.end(),
      [](const auto& left, const auto& right) {
        if (left.first != right.first) {
          return left.first > right.first;
        }
        return left.second.value("id", std::string{}) <
               right.second.value("id", std::string{});
      });

  for (std::size_t index = 0;
       index < scored.size() && index < kMaximumSelectedSkills;
       ++index) {
    selection.selected_skills.push_back(scored[index].second);
    selection.selected_skill_ids.push_back(
        scored[index].second.value("id", std::string{}));
  }
  if (!selection.selected_skill_ids.empty()) {
    selection.mode = "contextual";
  }
  return selection;
}

nlohmann::json PlaneSkillContextualResolverService::BuildDebugPayload(
    const std::string& db_path,
    const PlaneInteractionResolution& resolution,
    const nlohmann::json& payload) const {
  const auto selection = Resolve(db_path, resolution, payload);
  return nlohmann::json{
      {"plane_name", resolution.desired_state.plane_name},
      {"skills_enabled",
       resolution.desired_state.skills.has_value() &&
           resolution.desired_state.skills->enabled},
      {"skill_resolution_mode", selection.mode},
      {"candidate_count", selection.candidate_count},
      {"selected_skill_ids", selection.selected_skill_ids},
      {"selected_skills", selection.selected_skills},
      {"prompt_text", ExtractPromptText(payload)},
  };
}

}  // namespace comet::controller
