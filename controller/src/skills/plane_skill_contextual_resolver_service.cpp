#include "skills/plane_skill_contextual_resolver_service.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <set>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
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
  auto next_code_point = [](const std::string& input, std::size_t* offset) {
    if (*offset >= input.size()) {
      return static_cast<char32_t>(0);
    }

    const auto lead = static_cast<unsigned char>(input[*offset]);
    if ((lead & 0x80U) == 0) {
      ++(*offset);
      return static_cast<char32_t>(lead);
    }

    const auto fail = [offset]() {
      ++(*offset);
      return static_cast<char32_t>(' ');
    };

    if ((lead & 0xE0U) == 0xC0U) {
      if (*offset + 1 >= input.size()) {
        return fail();
      }
      const auto b1 = static_cast<unsigned char>(input[*offset + 1]);
      if ((b1 & 0xC0U) != 0x80U) {
        return fail();
      }
      *offset += 2;
      return static_cast<char32_t>(((lead & 0x1FU) << 6) | (b1 & 0x3FU));
    }

    if ((lead & 0xF0U) == 0xE0U) {
      if (*offset + 2 >= input.size()) {
        return fail();
      }
      const auto b1 = static_cast<unsigned char>(input[*offset + 1]);
      const auto b2 = static_cast<unsigned char>(input[*offset + 2]);
      if ((b1 & 0xC0U) != 0x80U || (b2 & 0xC0U) != 0x80U) {
        return fail();
      }
      *offset += 3;
      return static_cast<char32_t>(
          ((lead & 0x0FU) << 12) | ((b1 & 0x3FU) << 6) | (b2 & 0x3FU));
    }

    if ((lead & 0xF8U) == 0xF0U) {
      if (*offset + 3 >= input.size()) {
        return fail();
      }
      const auto b1 = static_cast<unsigned char>(input[*offset + 1]);
      const auto b2 = static_cast<unsigned char>(input[*offset + 2]);
      const auto b3 = static_cast<unsigned char>(input[*offset + 3]);
      if ((b1 & 0xC0U) != 0x80U || (b2 & 0xC0U) != 0x80U ||
          (b3 & 0xC0U) != 0x80U) {
        return fail();
      }
      *offset += 4;
      return static_cast<char32_t>(((lead & 0x07U) << 18) |
                                   ((b1 & 0x3FU) << 12) |
                                   ((b2 & 0x3FU) << 6) |
                                   (b3 & 0x3FU));
    }

    return fail();
  };

  auto append_utf8 = [](char32_t code_point, std::string* output) {
    if (code_point <= 0x7F) {
      output->push_back(static_cast<char>(code_point));
      return;
    }
    if (code_point <= 0x7FF) {
      output->push_back(
          static_cast<char>(0xC0U | ((code_point >> 6) & 0x1FU)));
      output->push_back(static_cast<char>(0x80U | (code_point & 0x3FU)));
      return;
    }
    output->push_back(
        static_cast<char>(0xE0U | ((code_point >> 12) & 0x0FU)));
    output->push_back(
        static_cast<char>(0x80U | ((code_point >> 6) & 0x3FU)));
    output->push_back(static_cast<char>(0x80U | (code_point & 0x3FU)));
  };

  auto lowercase_code_point = [](char32_t code_point) {
    if (code_point >= U'A' && code_point <= U'Z') {
      return static_cast<char32_t>(code_point + 32);
    }
    if (code_point >= 0x0410 && code_point <= 0x042F) {
      return static_cast<char32_t>(code_point + 0x20);
    }
    if (code_point == 0x0401) {
      return static_cast<char32_t>(0x0451);
    }
    return code_point;
  };

  auto is_token_code_point = [](char32_t code_point) {
    if (code_point >= U'0' && code_point <= U'9') {
      return true;
    }
    if (code_point >= U'a' && code_point <= U'z') {
      return true;
    }
    if (code_point >= U'A' && code_point <= U'Z') {
      return true;
    }
    if ((code_point >= 0x0410 && code_point <= 0x044F) || code_point == 0x0401 ||
        code_point == 0x0451) {
      return true;
    }
    return false;
  };

  std::string normalized;
  normalized.reserve(value.size() * 2);
  std::size_t offset = 0;
  while (offset < value.size()) {
    const char32_t raw = next_code_point(value, &offset);
    const char32_t code_point = lowercase_code_point(raw);
    if (is_token_code_point(code_point)) {
      append_utf8(code_point, &normalized);
    } else {
      normalized.push_back(' ');
    }
  }
  return normalized;
}

std::vector<std::string> TokenizeRelevantTerms(const std::string& value) {
  static const std::unordered_set<std::string> kIgnoredTokens = {
      "about",      "after",      "agent",      "apply",     "available",
      "before",     "build",      "check",      "catalog",   "change",
      "code",       "context",    "controller", "cypher",    "debug",
      "default",
      "enabled",    "explain",    "field",      "from",      "have",
      "interaction","into",       "local",      "mode",      "must",
      "localtrade",
      "plane",      "please",     "request",    "response",  "runtime",
      "service",    "should",     "skill",      "skills",    "source",
      "that",       "their",      "this",       "through",   "when",
      "with",       "without",    "пожалуйста", "нужно",     "просто",
      "обычной",    "человеческой","речи",      "сделай",    "сделать",
      "надо",       "через",      "перед",      "тем",       "как",
      "объясни",    "cookie",
  };

  static const std::unordered_map<std::string, std::string> kCanonicalAliases = {
      {"repo", "repository"},
      {"repositories", "repository"},
      {"репо", "repository"},
      {"репозиторий", "repository"},
      {"репозитория", "repository"},
      {"репозитории", "repository"},
      {"карта", "map"},
      {"карту", "map"},
      {"картой", "map"},
      {"auth", "session"},
      {"authorization", "session"},
      {"authorize", "session"},
      {"login", "session"},
      {"logout", "session"},
      {"acces", "session"},
      {"cookie", "session"},
      {"session", "session"},
      {"sessions", "session"},
      {"авторизация", "session"},
      {"авторизоваться", "session"},
      {"авторизуйся", "session"},
      {"логин", "session"},
      {"логина", "session"},
      {"сессия", "session"},
      {"сессии", "session"},
      {"сессию", "session"},
      {"куки", "session"},
      {"причина", "cause"},
      {"корень", "cause"},
      {"регрессия", "regression"},
      {"регрессии", "regression"},
      {"отладить", "debug"},
      {"отладка", "debug"},
      {"разобрать", "debug"},
      {"безопасный", "safe"},
      {"безопасно", "safe"},
      {"минимальный", "minimal"},
      {"минимально", "minimal"},
      {"патч", "patch"},
      {"исправление", "fix"},
      {"тест", "test"},
      {"тесты", "test"},
      {"деплой", "deploy"},
      {"выкатка", "deploy"},
      {"релиз", "deploy"},
      {"контракт", "contract"},
      {"эндпоинт", "endpoint"},
      {"интерфейс", "ui"},
      {"бандл", "bundle"},
      {"сборка", "build"},
      {"схема", "schema"},
      {"balanc", "balance"},
      {"balance", "balance"},
      {"balances", "balance"},
      {"баланс", "balance"},
      {"балансы", "balance"},
      {"баланса", "balance"},
      {"балансов", "balance"},
      {"остаток", "balance"},
      {"остатки", "balance"},
      {"доступные", "available"},
      {"доступный", "available"},
      {"доступно", "available"},
      {"публичный", "public"},
      {"публичные", "public"},
      {"публичная", "public"},
      {"публичное", "public"},
      {"стакан", "orderbook"},
      {"график", "chart"},
      {"графики", "chart"},
      {"пары", "pairs"},
      {"пара", "pairs"},
      {"котировки", "market"},
      {"маркет", "market"},
      {"рынок", "market"},
      {"рыночные", "market"},
      {"данные", "market"},
      {"аккаунта", "account"},
      {"аккаунт", "account"},
      {"ордера", "order"},
      {"ордер", "order"},
      {"ордерами", "order"},
      {"ордеров", "order"},
      {"лимитный", "limit"},
      {"лимитная", "limit"},
      {"лимитныйй", "limit"},
      {"рыночный", "market"},
      {"покупка", "buy"},
      {"купить", "buy"},
      {"покупку", "buy"},
      {"продажа", "sell"},
      {"продать", "sell"},
      {"подтверждение", "confirm"},
      {"подтвердить", "confirm"},
      {"подтверди", "confirm"},
      {"confirmation", "confirm"},
      {"подписаться", "subscribe"},
      {"подписки", "subscribe"},
      {"подписка", "subscribe"},
      {"сравни", "compare"},
      {"сравнить", "compare"},
      {"сравнение", "compare"},
      {"найди", "discover"},
      {"найти", "discover"},
      {"подбери", "discover"},
      {"лучших", "discover"},
      {"сильных", "discover"},
      {"subscribing", "subscribe"},
      {"subscribe", "subscribe"},
      {"following", "follow"},
      {"unfollowing", "unfollow"},
      {"трейдер", "trader"},
      {"трейдера", "trader"},
      {"трейдеров", "trader"},
      {"копитрейдинг", "copytrading"},
      {"копи", "copytrading"},
      {"копи-трейдинг", "copytrading"},
      {"копитрейдинга", "copytrading"},
      {"просадка", "drawdown"},
      {"просадке", "drawdown"},
      {"комнаты", "room"},
      {"комната", "room"},
      {"канал", "stream"},
      {"каналы", "stream"},
      {"каналов", "stream"},
      {"пользовательские", "user"},
      {"пользовательских", "user"},
      {"приватные", "protected"},
      {"приватных", "protected"},
      {"защищенные", "protected"},
      {"защищённых", "protected"},
      {"поток", "stream"},
      {"потоки", "stream"},
      {"streams", "stream"},
      {"socket", "socketio"},
      {"socketio", "socketio"},
      {"валидатор", "validator"},
      {"валидаторы", "validator"},
      {"проектор", "projector"},
      {"проекторы", "projector"},
      {"рендерер", "renderer"},
      {"рендереры", "renderer"},
      {"хранилище", "store"},
      {"жизненный", "lifecycle"},
      {"цикл", "lifecycle"},
      {"закрытие", "closeout"},
      {"тикет", "issue"},
      {"рефакторинг", "refactor"},
      {"правила", "rules"},
      {"скилл", "skill"},
      {"скиллы", "skill"},
      {"навык", "skill"},
      {"навыки", "skill"},
      {"триггер", "trigger"},
      {"триггеры", "trigger"},
      {"авторинг", "authoring"},
  };

  auto canonicalize = [&](std::string token) {
    if (token.size() > 5 && token.ends_with("ies")) {
      token = token.substr(0, token.size() - 3) + "y";
    } else if (token.size() > 5 && token.ends_with("es")) {
      token = token.substr(0, token.size() - 2);
    } else if (token.size() > 4 && token.ends_with('s')) {
      token.pop_back();
    }

    const auto alias = kCanonicalAliases.find(token);
    if (alias != kCanonicalAliases.end()) {
      return alias->second;
    }
    return token;
  };

  std::vector<std::string> tokens;
  std::set<std::string> seen;
  std::stringstream stream(NormalizeSkillText(value));
  std::string token;
  while (stream >> token) {
    token = canonicalize(token);
    const bool important_short_token = token == "api" || token == "ui";
    if ((!important_short_token && token.size() < 3) ||
        kIgnoredTokens.count(token) > 0) {
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
  const auto id_terms = TokenizeRelevantTerms(candidate.id);
  const auto name_terms = TokenizeRelevantTerms(candidate.name);
  const auto description_terms = TokenizeRelevantTerms(candidate.description);
  const auto content_terms = TokenizeRelevantTerms(candidate.content);

  auto contains_term = [](const std::vector<std::string>& terms,
                          std::initializer_list<const char*> candidates) {
    for (const auto* candidate_term : candidates) {
      if (std::find(terms.begin(), terms.end(), candidate_term) != terms.end()) {
        return true;
      }
    }
    return false;
  };

  int score = 0;
  std::vector<std::string> matched_id_terms;
  std::vector<std::string> matched_name_terms;
  std::vector<std::string> matched_description_terms;
  std::vector<std::string> matched_content_terms;

  const std::string full_name = NormalizeSkillText(candidate.name);
  if (!candidate.name.empty() &&
      prompt_normalized.find(full_name) != std::string::npos) {
    score += 12;
  }

  for (const auto& term : prompt_terms) {
    if (std::find(id_terms.begin(), id_terms.end(), term) != id_terms.end()) {
      matched_id_terms.push_back(term);
      score += 6;
      continue;
    }
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

  const bool prompt_session = contains_term(
      prompt_terms, {"session", "auth", "login", "logout"});
  const bool prompt_balance = contains_term(
      prompt_terms, {"balance", "available"});
  const bool prompt_public_market = contains_term(
      prompt_terms,
      {"public", "pairs", "market", "chart", "orderbook", "pairs_state"});
  const bool prompt_protected_user = contains_term(
      prompt_terms, {"user", "protected", "balance", "session", "auth"});
  const bool prompt_copy_action = contains_term(
      prompt_terms, {"subscribe", "follow", "unfollow"});
  const bool prompt_copy_discovery = contains_term(
      prompt_terms,
      {"discover", "compare", "copytrading", "trader", "drawdown", "sharpe",
       "pnl", "roi"});
  const bool prompt_spot_order = contains_term(
      prompt_terms, {"order", "limit", "buy", "sell", "confirm"});
  const bool prompt_streams = contains_term(
      prompt_terms, {"socketio", "room", "stream", "user"});

  const bool candidate_session = contains_term(
      id_terms, {"session", "auth"}) ||
      contains_term(name_terms, {"session", "auth"}) ||
      contains_term(description_terms, {"session", "auth"});
  const bool candidate_balance = contains_term(
      id_terms, {"balance"}) ||
      contains_term(name_terms, {"balance"}) ||
      contains_term(description_terms, {"balance", "available"});
  const bool candidate_public_market = contains_term(
      id_terms, {"market"}) ||
      contains_term(name_terms, {"market"}) ||
      contains_term(description_terms,
                    {"public", "pairs", "chart", "orderbook", "pairs_state"});
  const bool candidate_copy_action = contains_term(
      id_terms, {"subscribe", "follow", "unfollow"}) ||
      contains_term(name_terms, {"subscribe", "follow", "unfollow"}) ||
      contains_term(description_terms,
                    {"subscribe", "follow", "unfollow", "confirm"});
  const bool candidate_copy_discovery = contains_term(
      id_terms, {"copytrading", "trader", "discovery"}) ||
      contains_term(name_terms, {"copytrading", "trader", "discovery"}) ||
      contains_term(description_terms,
                    {"compare", "discover", "copytrading", "trader", "roi",
                     "drawdown", "sharpe", "pnl"});
  const bool candidate_spot_order = contains_term(
      id_terms, {"order", "limit"}) ||
      contains_term(name_terms, {"order", "limit"}) ||
      contains_term(description_terms, {"order", "limit", "buy", "sell", "confirm"});
  const bool candidate_streams = contains_term(
      id_terms, {"stream", "user"}) ||
      contains_term(name_terms, {"stream", "user"}) ||
      contains_term(description_terms, {"socketio", "room", "stream", "user"});

  if (prompt_session) {
    score += candidate_session ? 8 : 0;
    if (candidate_streams && !candidate_session) {
      score -= 2;
    }
  }
  if (prompt_balance) {
    score += candidate_balance ? 7 : 0;
    if (candidate_session && !candidate_balance) {
      score -= 2;
    }
  }
  if (prompt_public_market) {
    score += candidate_public_market ? 8 : 0;
    if ((candidate_streams || candidate_session || candidate_balance) &&
        !candidate_public_market) {
      score -= 3;
    }
    if (candidate_streams && !prompt_protected_user) {
      score -= 6;
    }
  }
  if (prompt_copy_action) {
    score += candidate_copy_action ? 8 : 0;
    if (!candidate_copy_action &&
        (candidate_public_market || candidate_streams || candidate_balance)) {
      score -= 2;
    }
  }
  if (prompt_copy_discovery) {
    score += candidate_copy_discovery ? 8 : 0;
    if (candidate_copy_action && !prompt_copy_action) {
      score -= 4;
    }
  }
  if (prompt_spot_order) {
    score += candidate_spot_order ? 8 : 0;
    if (candidate_public_market && !candidate_spot_order) {
      score -= 2;
    }
  }
  if (prompt_streams) {
    score += candidate_streams ? 8 : 0;
    if (candidate_public_market && !candidate_streams) {
      score -= 1;
    }
    if (candidate_streams && prompt_public_market && !prompt_protected_user) {
      score -= 4;
    }
    if (candidate_balance && !candidate_streams) {
      score -= 4;
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
    if (!matched_id_terms.empty()) {
      parts.push_back("id terms: " + JoinTerms(matched_id_terms));
    }
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
