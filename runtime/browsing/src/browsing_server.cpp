#include "browsing/browsing_server.h"

#include <algorithm>
#include <array>
#include <memory>
#include <csignal>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sys/types.h>
#include <unistd.h>

#include "browsing/process_support.h"
#include "browsing/cef_browser_backend.h"
#include "browsing/cef_support.h"
#include "comet/runtime/runtime_status.h"
#include "comet/security/crypto_utils.h"
#include "http/controller_http_server_support.h"
#include "infra/controller_network_manager.h"

namespace comet::browsing {

namespace {

std::atomic<bool>* g_stop_requested = nullptr;

void SignalHandler(int) {
  if (g_stop_requested != nullptr) {
    g_stop_requested->store(true);
  }
}

class ApiError final : public std::exception {
 public:
  ApiError(int status, std::string code, std::string message)
      : status_(status), code_(std::move(code)), message_(std::move(message)) {}

  const char* what() const noexcept override {
    return message_.c_str();
  }

  int status() const {
    return status_;
  }

  const std::string& code() const {
    return code_;
  }

  const std::string& message() const {
    return message_;
  }

 private:
  int status_ = 500;
  std::string code_;
  std::string message_;
};

std::string LowercaseCopy(std::string value) {
  std::transform(
      value.begin(),
      value.end(),
      value.begin(),
      [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return value;
}

std::string TrimCopy(const std::string& value) {
  const auto first = std::find_if_not(
      value.begin(),
      value.end(),
      [](unsigned char ch) { return std::isspace(ch); });
  const auto last = std::find_if_not(
      value.rbegin(),
      value.rend(),
      [](unsigned char ch) { return std::isspace(ch); })
                        .base();
  if (first >= last) {
    return {};
  }
  return std::string(first, last);
}

std::string ShellSafeToken(std::string value) {
  for (char& ch : value) {
    const unsigned char byte = static_cast<unsigned char>(ch);
    if (!std::isalnum(byte) && ch != '-' && ch != '_') {
      ch = '_';
    }
  }
  return value;
}

std::string ReadFileOrThrow(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("failed to read file: " + path.string());
  }
  std::ostringstream buffer;
  buffer << input.rdbuf();
  return buffer.str();
}

std::string HashText(const std::string& value) {
  const std::size_t hash = std::hash<std::string>{}(value);
  std::ostringstream out;
  out << std::hex << std::setw(16) << std::setfill('0') << hash;
  return out.str();
}

std::string UrlEncode(const std::string& value) {
  std::ostringstream out;
  out << std::hex << std::uppercase;
  for (const unsigned char ch : value) {
    if (std::isalnum(ch) || ch == '-' || ch == '_' || ch == '.' || ch == '~') {
      out << static_cast<char>(ch);
    } else if (ch == ' ') {
      out << '+';
    } else {
      out << '%' << std::setw(2) << std::setfill('0') << static_cast<int>(ch);
    }
  }
  return out.str();
}

std::string StripHtmlTags(const std::string& html) {
  return std::regex_replace(html, std::regex("<[^>]+>"), " ");
}

std::string HtmlEntityDecode(std::string text) {
  const std::vector<std::pair<std::string, std::string>> replacements = {
      {"&amp;", "&"},
      {"&lt;", "<"},
      {"&gt;", ">"},
      {"&quot;", "\""},
      {"&apos;", "'"},
      {"&#39;", "'"},
      {"&nbsp;", " "},
  };
  for (const auto& [from, to] : replacements) {
    std::size_t pos = 0;
    while ((pos = text.find(from, pos)) != std::string::npos) {
      text.replace(pos, from.size(), to);
      pos += to.size();
    }
  }
  return text;
}

std::string NormalizeWhitespace(std::string text) {
  text = std::regex_replace(text, std::regex("[\\t\\r\\f\\v]+"), " ");
  text = std::regex_replace(text, std::regex(" *\\n+ *"), "\n");
  text = std::regex_replace(text, std::regex(" {2,}"), " ");
  return TrimCopy(text);
}

std::optional<std::string> ExtractHost(const std::string& url) {
  static const std::regex kUrlPattern(R"(^(https?)://([^/?#]+))", std::regex::icase);
  std::smatch match;
  if (!std::regex_search(url, match, kUrlPattern)) {
    return std::nullopt;
  }
  std::string host = match[2].str();
  const auto at = host.rfind('@');
  if (at != std::string::npos) {
    host = host.substr(at + 1);
  }
  if (!host.empty() && host.front() == '[') {
    return std::nullopt;
  }
  const auto colon = host.rfind(':');
  if (colon != std::string::npos) {
    host = host.substr(0, colon);
  }
  host = LowercaseCopy(TrimCopy(host));
  if (host.empty()) {
    return std::nullopt;
  }
  return host;
}

bool EndsWithDomain(const std::string& host, const std::string& domain) {
  if (host == domain) {
    return true;
  }
  if (host.size() <= domain.size()) {
    return false;
  }
  return host.compare(host.size() - domain.size(), domain.size(), domain) == 0 &&
         host[host.size() - domain.size() - 1] == '.';
}

bool IsIpv4Literal(const std::string& host) {
  static const std::regex kIpv4(R"(^\d{1,3}(\.\d{1,3}){3}$)");
  return std::regex_match(host, kIpv4);
}

bool IsPrivateIpv4(const std::string& host) {
  if (!IsIpv4Literal(host)) {
    return false;
  }
  std::array<int, 4> octets{};
  std::size_t start = 0;
  for (int index = 0; index < 4; ++index) {
    const auto end = host.find('.', start);
    octets[index] = std::stoi(host.substr(start, end == std::string::npos ? std::string::npos : end - start));
    start = end == std::string::npos ? host.size() : end + 1;
  }
  const int a = octets[0];
  const int b = octets[1];
  return a == 10 || a == 127 || a == 0 || (a == 169 && b == 254) ||
         (a == 172 && b >= 16 && b <= 31) || (a == 192 && b == 168);
}

bool DomainAllowed(
    const std::string& host,
    const BrowsingPolicy& policy,
    const std::vector<std::string>& requested_domains = {}) {
  for (const auto& blocked : policy.blocked_domains) {
    if (!blocked.empty() && EndsWithDomain(host, LowercaseCopy(blocked))) {
      return false;
    }
  }
  if (!requested_domains.empty()) {
    for (const auto& requested : requested_domains) {
      if (!requested.empty() && EndsWithDomain(host, LowercaseCopy(requested))) {
        return true;
      }
    }
    return false;
  }
  if (policy.allowed_domains.empty()) {
    return true;
  }
  for (const auto& allowed : policy.allowed_domains) {
    if (!allowed.empty() && EndsWithDomain(host, LowercaseCopy(allowed))) {
      return true;
    }
  }
  return false;
}

bool IsJsHeavyDomain(const std::string& host) {
  return EndsWithDomain(host, "x.com") ||
         EndsWithDomain(host, "twitter.com") ||
         EndsWithDomain(host, "reddit.com") ||
         EndsWithDomain(host, "old.reddit.com");
}

std::vector<std::string> ExpandDiscoveryDomains(const std::vector<std::string>& requested_domains) {
  std::vector<std::string> expanded;
  std::unordered_set<std::string> seen;
  const auto append_domain = [&expanded, &seen](const std::string& domain) {
    if (!domain.empty() && seen.insert(domain).second) {
      expanded.push_back(domain);
    }
  };

  for (const auto& domain : requested_domains) {
    const std::string normalized = LowercaseCopy(TrimCopy(domain));
    append_domain(normalized);
    if (EndsWithDomain(normalized, "x.com") || EndsWithDomain(normalized, "twitter.com")) {
      append_domain("x.com");
      append_domain("twitter.com");
    }
    if (EndsWithDomain(normalized, "reddit.com") || EndsWithDomain(normalized, "old.reddit.com")) {
      append_domain("reddit.com");
      append_domain("old.reddit.com");
    }
  }

  return expanded;
}

std::optional<std::string> ExtractXmlElement(
    const std::string& xml,
    const std::string& tag) {
  const std::regex pattern(
      "<" + tag + R"([^>]*>([\s\S]*?)</)" + tag + ">",
      std::regex::icase);
  std::smatch match;
  if (!std::regex_search(xml, match, pattern)) {
    return std::nullopt;
  }
  return match[1].str();
}

std::string ExtractTitle(const std::string& html) {
  static const std::regex kTitle(R"(<title[^>]*>(.*?)</title>)", std::regex::icase);
  std::smatch match;
  if (!std::regex_search(html, match, kTitle)) {
    return {};
  }
  return NormalizeWhitespace(HtmlEntityDecode(StripHtmlTags(match[1].str())));
}

std::string StripActiveHtml(const std::string& html) {
  std::string sanitized = html;
  const std::vector<std::regex> block_patterns = {
      std::regex(R"(<!--.*?-->)", std::regex::icase | std::regex::extended),
      std::regex(R"(<script[\s\S]*?</script>)", std::regex::icase),
      std::regex(R"(<style[\s\S]*?</style>)", std::regex::icase),
      std::regex(R"(<iframe[\s\S]*?</iframe>)", std::regex::icase),
      std::regex(R"(<noscript[\s\S]*?</noscript>)", std::regex::icase),
      std::regex(R"(<svg[\s\S]*?</svg>)", std::regex::icase),
      std::regex(R"(<form[\s\S]*?</form>)", std::regex::icase),
      std::regex(R"(<head[\s\S]*?</head>)", std::regex::icase),
  };
  for (const auto& pattern : block_patterns) {
    sanitized = std::regex_replace(sanitized, pattern, " ");
  }
  sanitized = std::regex_replace(
      sanitized,
      std::regex(R"(<[^>]*(display\s*:\s*none|visibility\s*:\s*hidden|hidden)[^>]*>[\s\S]*?</[^>]+>)",
                 std::regex::icase),
      " ");
  sanitized = std::regex_replace(
      sanitized,
      std::regex(R"((</(p|div|li|tr|h1|h2|h3|h4|h5|h6|section|article)>|<br\s*/?>))",
                 std::regex::icase),
      "\n");
  sanitized = std::regex_replace(sanitized, std::regex(R"(<[^>]+>)"), " ");
  return NormalizeWhitespace(HtmlEntityDecode(sanitized));
}

std::vector<std::string> RequestedDomainsFromPayload(const nlohmann::json& payload) {
  std::vector<std::string> domains;
  if (!payload.contains("domains") || !payload.at("domains").is_array()) {
    return domains;
  }
  for (const auto& item : payload.at("domains")) {
    if (item.is_string()) {
      const std::string domain = LowercaseCopy(TrimCopy(item.get<std::string>()));
      if (!domain.empty()) {
        domains.push_back(domain);
      }
    }
  }
  return domains;
}

std::string ComposeSearchQuery(
    const std::string& query,
    const std::vector<std::string>& requested_domains) {
  const std::string trimmed_query = TrimCopy(query);
  if (requested_domains.empty()) {
    return trimmed_query;
  }

  std::ostringstream out;
  if (requested_domains.size() > 1) {
    out << "(";
  }
  for (std::size_t index = 0; index < requested_domains.size(); ++index) {
    if (index > 0) {
      out << " OR ";
    }
    out << "site:" << requested_domains[index];
  }
  if (requested_domains.size() > 1) {
    out << ")";
  }
  if (!trimmed_query.empty()) {
    out << " " << trimmed_query;
  }
  return out.str();
}

std::optional<std::string> BuildSiteDiscoveryUrl(
    const std::string& domain,
    const std::string& query) {
  if (EndsWithDomain(domain, "reddit.com") || EndsWithDomain(domain, "old.reddit.com")) {
    return "https://www.reddit.com/search/?q=" + UrlEncode(query) + "&sort=relevance&t=all";
  }
  if (EndsWithDomain(domain, "x.com") || EndsWithDomain(domain, "twitter.com")) {
    return "https://x.com/search?q=" + UrlEncode(query) + "&src=typed_query&f=live";
  }
  return std::nullopt;
}

std::string SearchSnippetFromHtml(const std::string& item_html) {
  static const std::regex kParagraphPattern(R"(<p[^>]*>([\s\S]*?)</p>)", std::regex::icase);
  std::smatch paragraph_match;
  if (std::regex_search(item_html, paragraph_match, kParagraphPattern)) {
    const std::string snippet =
        NormalizeWhitespace(HtmlEntityDecode(StripHtmlTags(paragraph_match[1].str())));
    if (!snippet.empty()) {
      return snippet;
    }
  }

  std::string flattened = std::regex_replace(
      item_html,
      std::regex(R"(<h2[^>]*>[\s\S]*?</h2>)", std::regex::icase),
      " ");
  return NormalizeWhitespace(HtmlEntityDecode(StripHtmlTags(flattened)));
}

bool SearchResultsLookThin(
    const std::vector<SearchResult>& results,
    int limit) {
  if (results.empty()) {
    return true;
  }
  const int minimum_expected = std::min(2, limit);
  if (static_cast<int>(results.size()) < minimum_expected) {
    return true;
  }
  int low_signal_results = 0;
  for (const auto& result : results) {
    const std::string snippet = TrimCopy(result.snippet);
    const std::string summary = LowercaseCopy(
        TrimCopy(result.title) + "\n" + snippet + "\n" + LowercaseCopy(result.url));
    const bool looks_blocked =
        summary.find("you've been blocked by network security") != std::string::npos ||
        summary.find("try reloading") != std::string::npos ||
        summary.find("something went wrong") != std::string::npos ||
        summary.find("/i/flow/login") != std::string::npos ||
        summary.find("js_challenge") != std::string::npos;
    if (snippet.empty() || snippet == TrimCopy(result.title) || looks_blocked) {
      ++low_signal_results;
    }
  }
  return low_signal_results == static_cast<int>(results.size());
}

std::string TruncateText(const std::string& value, std::size_t limit) {
  if (value.size() <= limit) {
    return value;
  }
  return value.substr(0, limit);
}

bool EvidenceLooksThin(const FetchResult& fetched) {
  const std::string text = TrimCopy(fetched.visible_text);
  if (text.size() < 160) {
    return true;
  }
  const std::string lowered = LowercaseCopy(text);
  return lowered.find("enable javascript") != std::string::npos ||
         lowered.find("javascript is required") != std::string::npos ||
         lowered.find("please wait while your request is being verified") != std::string::npos;
}

FetchResult BuildRenderedFetchResult(
    const std::string& requested_url,
    const CefRenderedDocument& rendered,
    const BrowsingPolicy& policy,
    const std::string& fetched_at) {
  FetchResult result;
  const std::string rendered_text = TrimCopy(rendered.visible_text);
  if (!rendered_text.empty()) {
    result.url = requested_url;
    result.final_url = rendered.final_url.empty() ? requested_url : rendered.final_url;
    result.content_type = rendered.content_type;
    result.title = rendered.title;
    result.visible_text =
        TruncateText(rendered_text, static_cast<std::size_t>(policy.max_fetch_bytes));
    result.response_hash =
        HashText(rendered.html_source.empty() ? rendered.visible_text : rendered.html_source);
    result.injection_flags = BrowsingServer::DetectInjectionFlags(result.visible_text);
    if (!result.visible_text.empty()) {
      result.citations.push_back(
          nlohmann::json{{"start", 0},
                         {"end", std::min<int>(120, static_cast<int>(result.visible_text.size()))},
                         {"url", result.final_url}});
    }
  } else if (!rendered.html_source.empty()) {
    result = BrowsingServer::SanitizeFetchedDocument(
        requested_url,
        rendered.final_url,
        rendered.content_type,
        rendered.html_source,
        policy);
  } else {
    result.url = requested_url;
    result.final_url = rendered.final_url.empty() ? requested_url : rendered.final_url;
    result.content_type = rendered.content_type;
    result.title = rendered.title;
    result.visible_text =
        TruncateText(rendered.visible_text, static_cast<std::size_t>(policy.max_fetch_bytes));
    result.response_hash = HashText(rendered.visible_text);
    result.injection_flags = BrowsingServer::DetectInjectionFlags(result.visible_text);
    if (!result.visible_text.empty()) {
      result.citations.push_back(
          nlohmann::json{{"start", 0},
                         {"end", std::min<int>(120, static_cast<int>(result.visible_text.size()))},
                         {"url", result.final_url}});
    }
  }
  if (!result.title.has_value() && rendered.title.has_value()) {
    result.title = rendered.title;
  }
  result.fetched_at = fetched_at;
  result.backend = "browser_render";
  result.rendered = true;
  if (rendered.screenshot_path.has_value()) {
    result.screenshot_path = rendered.screenshot_path->string();
  }
  return result;
}

}  // namespace

BrowsingServer::BrowsingServer(BrowsingRuntimeConfig config) : config_(std::move(config)) {
  if (CefRuntimeEnabled()) {
    cef_backend_ = std::make_unique<CefBrowserBackend>(config_.state_root);
  }
}

BrowsingServer::~BrowsingServer() {
  RequestStop();
}

BrowsingPolicy BrowsingServer::ParsePolicyJson(const std::string& json_text) {
  BrowsingPolicy policy;
  if (json_text.empty()) {
    return policy;
  }
  const auto payload = nlohmann::json::parse(json_text, nullptr, false);
  if (!payload.is_object()) {
    return policy;
  }
  policy.browser_session_enabled =
      payload.value("browser_session_enabled", policy.browser_session_enabled);
  policy.rendered_browser_enabled =
      payload.value("rendered_browser_enabled", policy.rendered_browser_enabled);
  policy.login_enabled =
      payload.value("login_enabled", policy.login_enabled);
  policy.max_search_results = payload.value("max_search_results", policy.max_search_results);
  policy.max_fetch_bytes = payload.value("max_fetch_bytes", policy.max_fetch_bytes);
  if (payload.contains("allowed_domains") && payload.at("allowed_domains").is_array()) {
    for (const auto& item : payload.at("allowed_domains")) {
      if (item.is_string()) {
        policy.allowed_domains.push_back(LowercaseCopy(item.get<std::string>()));
      }
    }
  }
  if (payload.contains("blocked_domains") && payload.at("blocked_domains").is_array()) {
    for (const auto& item : payload.at("blocked_domains")) {
      if (item.is_string()) {
        policy.blocked_domains.push_back(LowercaseCopy(item.get<std::string>()));
      }
    }
  }
  return policy;
}

bool BrowsingServer::IsSafeBrowsingUrl(
    const std::string& url,
    const BrowsingPolicy& policy,
    std::string* reason,
    std::string* normalized_host) {
  if (!(url.rfind("http://", 0) == 0 || url.rfind("https://", 0) == 0)) {
    if (reason != nullptr) {
      *reason = "only http and https URLs are supported";
    }
    return false;
  }
  const auto host = ExtractHost(url);
  if (!host.has_value()) {
    if (reason != nullptr) {
      *reason = "URL host is missing or unsupported";
    }
    return false;
  }
  if (normalized_host != nullptr) {
    *normalized_host = *host;
  }
  if (host == "localhost" || EndsWithDomain(*host, "local") ||
      EndsWithDomain(*host, "internal") || EndsWithDomain(*host, "lan") ||
      *host == "host.docker.internal" || *host == "controller.internal") {
    if (reason != nullptr) {
      *reason = "private or controller-local hosts are denied";
    }
    return false;
  }
  if (IsPrivateIpv4(*host)) {
    if (reason != nullptr) {
      *reason = "private IP targets are denied";
    }
    return false;
  }
  if (!DomainAllowed(*host, policy)) {
    if (reason != nullptr) {
      *reason = "domain is blocked by browsing policy";
    }
    return false;
  }
  return true;
}

std::vector<SearchResult> BrowsingServer::ParseBingRssResults(
    const std::string& rss_xml,
    const BrowsingPolicy& policy,
    const std::vector<std::string>& requested_domains,
    int limit) {
  std::vector<SearchResult> results;
  std::regex item_pattern(R"(<item\b[^>]*>([\s\S]*?)</item>)", std::regex::icase);
  auto begin = std::sregex_iterator(rss_xml.begin(), rss_xml.end(), item_pattern);
  auto end = std::sregex_iterator();
  int index = 0;
  for (auto it = begin; it != end && static_cast<int>(results.size()) < limit; ++it) {
    const std::string item_xml = (*it)[1].str();
    const auto link = ExtractXmlElement(item_xml, "link");
    const auto title = ExtractXmlElement(item_xml, "title");
    if (!link.has_value() || !title.has_value()) {
      continue;
    }
    const std::string href = TrimCopy(HtmlEntityDecode(*link));
    std::string host;
    std::string reason;
    if (!IsSafeBrowsingUrl(href, policy, &reason, &host) ||
        !DomainAllowed(host, policy, requested_domains)) {
      continue;
    }
    SearchResult item;
    item.url = href;
    item.domain = host;
    item.title = NormalizeWhitespace(HtmlEntityDecode(StripHtmlTags(*title)));
    if (const auto description = ExtractXmlElement(item_xml, "description");
        description.has_value()) {
      item.snippet = NormalizeWhitespace(HtmlEntityDecode(StripHtmlTags(*description)));
    }
    if (const auto pub_date = ExtractXmlElement(item_xml, "pubDate");
        pub_date.has_value()) {
      const std::string normalized_pub_date =
          NormalizeWhitespace(HtmlEntityDecode(StripHtmlTags(*pub_date)));
      if (!normalized_pub_date.empty()) {
        item.published_at = normalized_pub_date;
      }
    }
    if (item.snippet.empty()) {
      item.snippet = item.title;
    }
    item.score = std::max(0.0, 1.0 - static_cast<double>(index) * 0.05);
    if (!item.title.empty()) {
      results.push_back(std::move(item));
      ++index;
    }
  }
  return results;
}

std::vector<SearchResult> BrowsingServer::ParseBingHtmlResults(
    const std::string& html,
    const BrowsingPolicy& policy,
    const std::vector<std::string>& requested_domains,
    int limit) {
  std::vector<SearchResult> results;
  std::unordered_set<std::string> seen_urls;
  std::regex item_pattern(
      R"(<li[^>]*class\s*=\s*["'][^"']*\bb_algo\b[^"']*["'][^>]*>([\s\S]*?)</li>)",
      std::regex::icase);
  std::regex link_pattern(
      R"(<h2[^>]*>[\s\S]*?<a[^>]+href\s*=\s*["']([^"']+)["'][^>]*>([\s\S]*?)</a>)",
      std::regex::icase);
  auto begin = std::sregex_iterator(html.begin(), html.end(), item_pattern);
  auto end = std::sregex_iterator();
  int index = 0;
  for (auto it = begin; it != end && static_cast<int>(results.size()) < limit; ++it) {
    const std::string item_html = (*it)[1].str();
    std::smatch link_match;
    if (!std::regex_search(item_html, link_match, link_pattern)) {
      continue;
    }

    const std::string href = TrimCopy(HtmlEntityDecode(link_match[1].str()));
    std::string host;
    std::string reason;
    if (!IsSafeBrowsingUrl(href, policy, &reason, &host) ||
        !DomainAllowed(host, policy, requested_domains)) {
      continue;
    }

    SearchResult item;
    item.url = href;
    item.domain = host;
    item.title = NormalizeWhitespace(HtmlEntityDecode(StripHtmlTags(link_match[2].str())));
    item.snippet = SearchSnippetFromHtml(item_html);
    if (item.snippet.empty()) {
      item.snippet = item.title;
    }
    item.backend = "browser_render";
    item.rendered = true;
    item.score = std::max(0.0, 1.0 - static_cast<double>(index) * 0.05);
    if (!item.title.empty() && seen_urls.insert(item.url).second) {
      results.push_back(std::move(item));
      ++index;
    }
  }

  if (!results.empty()) {
    return results;
  }

  std::regex anchor_pattern(
      R"(<a[^>]+href\s*=\s*["']([^"']+)["'][^>]*>([\s\S]*?)</a>)",
      std::regex::icase);
  begin = std::sregex_iterator(html.begin(), html.end(), anchor_pattern);
  end = std::sregex_iterator();
  index = 0;
  for (auto it = begin; it != end && static_cast<int>(results.size()) < limit; ++it) {
    const std::string href = TrimCopy(HtmlEntityDecode((*it)[1].str()));
    std::string host;
    std::string reason;
    if (!IsSafeBrowsingUrl(href, policy, &reason, &host) ||
        !DomainAllowed(host, policy, requested_domains)) {
      continue;
    }

    SearchResult item;
    item.url = href;
    item.domain = host;
    item.title = NormalizeWhitespace(HtmlEntityDecode(StripHtmlTags((*it)[2].str())));
    const std::size_t match_offset = static_cast<std::size_t>((*it).position());
    const std::size_t context_start = match_offset > 240 ? match_offset - 240 : 0;
    const std::size_t context_size = std::min<std::size_t>(html.size() - context_start, 480);
    item.snippet = NormalizeWhitespace(
        HtmlEntityDecode(StripHtmlTags(html.substr(context_start, context_size))));
    if (item.snippet.empty()) {
      item.snippet = item.title;
    }
    item.backend = "browser_render";
    item.rendered = true;
    item.score = std::max(0.0, 1.0 - static_cast<double>(index) * 0.05);
    if (!item.title.empty() && seen_urls.insert(item.url).second) {
      results.push_back(std::move(item));
      ++index;
    }
  }
  return results;
}

std::vector<std::string> BrowsingServer::DetectInjectionFlags(const std::string& text) {
  const std::string lowered = LowercaseCopy(text);
  std::vector<std::string> flags;
  const std::vector<std::pair<std::string, std::string>> heuristics = {
      {"ignore previous instructions", "instruction_override_attempt"},
      {"reveal your system prompt", "system_prompt_exfiltration_attempt"},
      {"upload", "upload_request"},
      {"ssh", "remote_tool_reference"},
      {"secret", "secret_access_request"},
      {"browser session", "browser_action_request"},
      {"copy repository", "repo_exfiltration_attempt"},
  };
  for (const auto& [needle, flag] : heuristics) {
    if (lowered.find(needle) != std::string::npos) {
      flags.push_back(flag);
    }
  }
  return flags;
}

FetchResult BrowsingServer::SanitizeFetchedDocument(
    const std::string& requested_url,
    const std::string& final_url,
    const std::string& content_type,
    const std::string& body,
    const BrowsingPolicy& policy) {
  FetchResult result;
  result.url = requested_url;
  result.final_url = final_url.empty() ? requested_url : final_url;
  result.content_type = content_type;
  result.fetched_at = "";
  result.title = ExtractTitle(body);

  std::string visible_text;
  if (LowercaseCopy(content_type).find("text/plain") != std::string::npos) {
    visible_text = NormalizeWhitespace(body);
  } else if (LowercaseCopy(content_type).find("text/html") != std::string::npos) {
    visible_text = StripActiveHtml(body);
  } else {
    throw ApiError(
        415,
        "unsupported_content_type",
        "only text/html and text/plain fetches are supported");
  }

  visible_text = TruncateText(visible_text, static_cast<std::size_t>(policy.max_fetch_bytes));
  result.visible_text = visible_text;
  result.response_hash = HashText(body);
  result.injection_flags = DetectInjectionFlags(visible_text);
  if (!visible_text.empty()) {
    result.citations.push_back(
        nlohmann::json{{"start", 0},
                       {"end", std::min<int>(120, static_cast<int>(visible_text.size()))},
                       {"url", result.final_url}});
  }
  return result;
}

int BrowsingServer::Run() {
  std::filesystem::create_directories(config_.status_path.parent_path());
  std::filesystem::create_directories(config_.state_root);

  WriteRuntimeStatus("starting", false);
  SetReadyFile(false);

  g_stop_requested = &stop_requested_;
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  listen_fd_ = comet::controller::ControllerNetworkManager::CreateListenSocket(
      config_.listen_host,
      config_.port);
  WriteRuntimeStatus("running", true);
  SetReadyFile(true);

  try {
    AcceptLoop();
  } catch (...) {
    WriteRuntimeStatus("stopped", false);
    SetReadyFile(false);
    comet::controller::ControllerNetworkManager::ShutdownAndCloseSocket(listen_fd_);
    listen_fd_ = comet::platform::kInvalidSocket;
    throw;
  }

  WriteRuntimeStatus("stopped", false);
  SetReadyFile(false);
  comet::controller::ControllerNetworkManager::ShutdownAndCloseSocket(listen_fd_);
  listen_fd_ = comet::platform::kInvalidSocket;
  return 0;
}

void BrowsingServer::RequestStop() {
  const bool was_requested = stop_requested_.exchange(true);
  if (!was_requested && comet::platform::IsSocketValid(listen_fd_)) {
    WriteRuntimeStatus("stopping", false);
    SetReadyFile(false);
    comet::controller::ControllerNetworkManager::ShutdownAndCloseSocket(listen_fd_);
  }
}

void BrowsingServer::AcceptLoop() {
  while (!stop_requested_.load()) {
    const auto client_fd = accept(listen_fd_, nullptr, nullptr);
    if (!comet::platform::IsSocketValid(client_fd)) {
      if (stop_requested_.load() || comet::platform::LastSocketErrorWasInterrupted()) {
        continue;
      }
      throw std::runtime_error(
          "accept failed: " + comet::controller::ControllerNetworkManager::SocketErrorMessage());
    }
    std::thread(&BrowsingServer::HandleClient, this, client_fd).detach();
  }
}

void BrowsingServer::HandleClient(comet::platform::SocketHandle client_fd) {
  std::string request_data;
  std::array<char, 8192> buffer{};
  std::size_t expected_request_bytes = 0;
  while (true) {
    const ssize_t read_count = recv(client_fd, buffer.data(), buffer.size(), 0);
    if (read_count <= 0) {
      break;
    }
    request_data.append(buffer.data(), static_cast<std::size_t>(read_count));
    if (expected_request_bytes == 0) {
      expected_request_bytes =
          comet::controller::ControllerHttpServerSupport::ExpectedRequestBytes(request_data);
    }
    if (expected_request_bytes != 0 && request_data.size() >= expected_request_bytes) {
      break;
    }
  }

  if (!request_data.empty()) {
    try {
      const HttpRequest request =
          comet::controller::ControllerHttpServerSupport::ParseHttpRequest(request_data);
      comet::controller::ControllerNetworkManager::SendHttpResponse(
          client_fd,
          HandleRequest(request));
    } catch (const ApiError& error) {
      comet::controller::ControllerNetworkManager::SendHttpResponse(
          client_fd,
          BuildJsonResponse(
              error.status(),
              nlohmann::json{{"status", "error"},
                             {"error", {{"code", error.code()}, {"message", error.message()}}}}));
    } catch (const std::exception& error) {
      comet::controller::ControllerNetworkManager::SendHttpResponse(
          client_fd,
          BuildJsonResponse(
              500,
              nlohmann::json{{"status", "error"},
                             {"error", {{"code", "internal_error"}, {"message", error.what()}}}}));
    }
  }
  comet::controller::ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
}

HttpResponse BrowsingServer::HandleRequest(const HttpRequest& request) {
  if (request.method == "GET") {
    return HandleGet(request);
  }
  if (request.method == "POST") {
    return HandlePost(request);
  }
  if (request.method == "DELETE") {
    return HandleDelete(request);
  }
  throw ApiError(405, "method_not_allowed", "method not allowed");
}

HttpResponse BrowsingServer::HandleGet(const HttpRequest& request) {
  const auto parts = SplitPath(request.path);
  if (parts.size() == 1 && parts[0] == "health") {
    return BuildJsonResponse(
        200,
        nlohmann::json{{"status", "ok"}, {"service", "comet-browsing"}, {"ready", true}});
  }
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "browsing" && parts[2] == "status") {
    return BuildJsonResponse(200, BuildStatusPayload());
  }
  if (parts.size() == 4 && parts[0] == "v1" && parts[1] == "browsing" && parts[2] == "sessions") {
    return BuildJsonResponse(200, ReadSession(parts[3]));
  }
  throw ApiError(404, "not_found", "route not found");
}

HttpResponse BrowsingServer::HandlePost(const HttpRequest& request) {
  const auto parts = SplitPath(request.path);
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "browsing" && parts[2] == "search") {
    return BuildJsonResponse(200, HandleSearchPayload(ParseJsonBody(request)));
  }
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "browsing" && parts[2] == "fetch") {
    return BuildJsonResponse(200, HandleFetchPayload(ParseJsonBody(request)));
  }
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "browsing" && parts[2] == "sessions") {
    return BuildJsonResponse(201, CreateSession(ParseJsonBody(request)));
  }
  if (parts.size() == 5 && parts[0] == "v1" && parts[1] == "browsing" && parts[2] == "sessions" &&
      parts[4] == "actions") {
    return BuildJsonResponse(200, ApplySessionAction(parts[3], ParseJsonBody(request)));
  }
  throw ApiError(404, "not_found", "route not found");
}

HttpResponse BrowsingServer::HandleDelete(const HttpRequest& request) {
  const auto parts = SplitPath(request.path);
  if (parts.size() == 4 && parts[0] == "v1" && parts[1] == "browsing" && parts[2] == "sessions") {
    return BuildJsonResponse(200, DeleteSession(parts[3]));
  }
  throw ApiError(404, "not_found", "route not found");
}

std::vector<std::string> BrowsingServer::SplitPath(const std::string& path) const {
  std::vector<std::string> parts;
  std::size_t start = 0;
  while (start < path.size()) {
    while (start < path.size() && path[start] == '/') {
      ++start;
    }
    if (start >= path.size()) {
      break;
    }
    const std::size_t end = path.find('/', start);
    parts.push_back(path.substr(start, end == std::string::npos ? std::string::npos : end - start));
    if (end == std::string::npos) {
      break;
    }
    start = end + 1;
  }
  return parts;
}

nlohmann::json BrowsingServer::ParseJsonBody(const HttpRequest& request) {
  if (request.body.empty()) {
    return nlohmann::json::object();
  }
  const auto payload = nlohmann::json::parse(request.body, nullptr, false);
  if (payload.is_discarded()) {
    throw ApiError(400, "invalid_json", "invalid JSON body");
  }
  return payload;
}

HttpResponse BrowsingServer::BuildJsonResponse(
    int status_code,
    const nlohmann::json& payload) const {
  HttpResponse response;
  response.status_code = status_code;
  response.content_type = "application/json";
  response.body = payload.dump();
  return response;
}

void BrowsingServer::WriteRuntimeStatus(const std::string& phase, bool ready) const {
  comet::RuntimeStatus status;
  status.plane_name = config_.plane_name;
  status.control_root = config_.control_root;
  status.controller_url = config_.controller_url;
  status.instance_name = config_.instance_name;
  status.instance_role = config_.instance_role;
  status.node_name = config_.node_name;
  status.runtime_backend =
      config_.policy.rendered_browser_enabled && cef_backend_ != nullptr && cef_backend_->IsAvailable()
          ? "browsing-cef"
          : "browsing-broker";
  status.runtime_phase = phase;
  status.started_at = UtcNow();
  status.last_activity_at = status.started_at;
  status.ready = ready;
  status.active_model_ready = true;
  status.inference_ready = ready;
  status.launch_ready = ready;
  SaveRuntimeStatusJson(status, config_.status_path.string());
}

void BrowsingServer::SetReadyFile(bool ready) const {
  if (ready) {
    std::filesystem::create_directories(config_.ready_path.parent_path());
    std::ofstream output(config_.ready_path);
    output << "ready\n";
    return;
  }
  std::error_code error;
  std::filesystem::remove(config_.ready_path, error);
}

std::string BrowsingServer::UtcNow() const {
  const auto now = std::chrono::system_clock::now();
  const std::time_t time = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
  gmtime_r(&time, &tm);
  char buffer[32];
  std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", &tm);
  return buffer;
}

void BrowsingServer::AppendAuditLog(const nlohmann::json& payload) const {
  std::filesystem::create_directories(config_.state_root);
  std::ofstream output(config_.state_root / "audit.log", std::ios::app);
  output << payload.dump() << "\n";
}

nlohmann::json BrowsingServer::BuildStatusPayload() const {
  std::lock_guard<std::mutex> lock(sessions_mutex_);
  const bool rendered_browser_ready =
      config_.policy.rendered_browser_enabled && cef_backend_ != nullptr && cef_backend_->IsAvailable();
  return nlohmann::json{
      {"status", "ok"},
      {"service", "comet-browsing"},
      {"plane_name", config_.plane_name},
      {"instance_name", config_.instance_name},
      {"ready", true},
      {"cef_build_enabled", CefBuildEnabled()},
      {"cef_runtime_enabled", CefRuntimeEnabled()},
      {"cef_build_summary", CefBuildSummary()},
      {"search_enabled", true},
      {"fetch_enabled", true},
      {"rendered_browser_enabled", config_.policy.rendered_browser_enabled},
      {"rendered_browser_ready", rendered_browser_ready},
      {"rendered_fetch_enabled", rendered_browser_ready},
      {"login_enabled", config_.policy.login_enabled},
      {"session_backend", rendered_browser_ready ? "cef" : "broker_fallback"},
      {"browser_session_enabled", config_.policy.browser_session_enabled},
      {"active_session_count", sessions_.size()},
      {"policy",
       {{"browser_session_enabled", config_.policy.browser_session_enabled},
        {"rendered_browser_enabled", config_.policy.rendered_browser_enabled},
        {"login_enabled", config_.policy.login_enabled},
        {"allowed_domains", config_.policy.allowed_domains},
        {"blocked_domains", config_.policy.blocked_domains},
        {"max_search_results", config_.policy.max_search_results},
        {"max_fetch_bytes", config_.policy.max_fetch_bytes}}},
  };
}

nlohmann::json BrowsingServer::HandleSearchPayload(const nlohmann::json& payload) {
  const std::string query = TrimCopy(payload.value("query", std::string{}));
  if (query.empty()) {
    throw ApiError(400, "invalid_query", "query is required");
  }
  const int requested_limit = payload.value("limit", config_.policy.max_search_results);
  const int limit = std::max(1, std::min(config_.policy.max_search_results, requested_limit));
  const auto requested_domains = ExpandDiscoveryDomains(RequestedDomainsFromPayload(payload));
  const std::string effective_query = ComposeSearchQuery(query, requested_domains);
  const bool rendered_browser_ready =
      config_.policy.rendered_browser_enabled && cef_backend_ != nullptr && cef_backend_->IsAvailable();

  const std::string search_url =
      "https://www.bing.com/search?format=rss&q=" + UrlEncode(effective_query);
  const auto command = RunCommandCapture(CommandRequest{
      .args =
          {"/usr/bin/curl",
           "-A",
           "Mozilla/5.0",
           "-fsSL",
           "--proto",
           "=https,http",
           "--connect-timeout",
           "5",
           "--max-time",
           "20",
           "--max-redirs",
           "5",
           search_url},
      .environment = {},
      .working_directory = std::nullopt,
      .clear_environment = false,
      .merge_stderr_into_stdout = true,
  });
  if (command.exit_code != 0) {
    throw ApiError(502, "search_upstream_failed", TrimCopy(command.output));
  }

  std::vector<SearchResult> results =
      ParseBingRssResults(command.output, config_.policy, requested_domains, limit);
  bool rendered_discovery_used = false;
  if (!requested_domains.empty() &&
      SearchResultsLookThin(results, limit) &&
      rendered_browser_ready) {
    std::unordered_set<std::string> seen_urls;
    std::vector<SearchResult> rendered_results;
    std::vector<SearchResult> fallback_rendered_results;
    int discovery_index = 0;
    for (const auto& domain : requested_domains) {
      const auto discovery_url = BuildSiteDiscoveryUrl(domain, query);
      if (!discovery_url.has_value()) {
        continue;
      }
      std::string discovery_error_code;
      std::string discovery_error_message;
      const auto fetched = FetchUrl(*discovery_url, &discovery_error_code, &discovery_error_message);
      if (!fetched.has_value()) {
        if (!discovery_error_message.empty()) {
          AppendAuditLog(
              nlohmann::json{{"ts", UtcNow()},
                             {"kind", "search_discovery_fallback_failed"},
                             {"query", query},
                             {"requested_domain", domain},
                             {"url", *discovery_url},
                             {"message", discovery_error_message}});
        }
        continue;
      }

      SearchResult item;
      item.url = fetched->final_url.empty() ? *discovery_url : fetched->final_url;
      item.domain = ExtractHost(item.url).value_or(domain);
      item.title = fetched->title.value_or("Rendered discovery for " + domain);
      item.snippet = fetched->visible_text.empty()
                         ? item.title
                         : TruncateText(fetched->visible_text, static_cast<std::size_t>(640));
      item.backend = fetched->backend;
      item.rendered = fetched->rendered;
      item.score = std::max(0.0, 0.9 - static_cast<double>(discovery_index) * 0.1);
      if (seen_urls.insert(item.url).second) {
        rendered_results.push_back(std::move(item));
        ++discovery_index;
      }
      if (static_cast<int>(rendered_results.size()) >= limit) {
        break;
      }
    }

    if (!rendered_results.empty()) {
      if (SearchResultsLookThin(rendered_results, limit)) {
        fallback_rendered_results = rendered_results;
      } else {
        results = std::move(rendered_results);
        rendered_discovery_used = true;
      }
    }

    if (!rendered_discovery_used) {
      std::string rendered_error;
      const std::string rendered_search_url =
          "https://www.bing.com/search?q=" + UrlEncode(effective_query);
      const auto rendered = cef_backend_->FetchPage(
          rendered_search_url,
          config_.state_root / ("rendered-search-" + ShellSafeToken(comet::RandomTokenBase64(8))),
          &rendered_error,
          true);
      if (rendered.has_value() && !rendered->html_source.empty()) {
        rendered_results =
            ParseBingHtmlResults(rendered->html_source, config_.policy, requested_domains, limit);
        if (!rendered_results.empty()) {
          results = std::move(rendered_results);
          rendered_discovery_used = true;
        }
      } else if (!rendered_error.empty()) {
        AppendAuditLog(
            nlohmann::json{{"ts", UtcNow()},
                           {"kind", "search_discovery_fallback_failed"},
                           {"query", query},
                           {"requested_domains", requested_domains},
                           {"message", rendered_error}});
      }

      if (!rendered_discovery_used && !fallback_rendered_results.empty()) {
        results = std::move(fallback_rendered_results);
        rendered_discovery_used = true;
      }
    }
  }
  AppendAuditLog(
      nlohmann::json{{"ts", UtcNow()},
                     {"kind", "search"},
                     {"query", query},
                     {"requested_domains", requested_domains},
                     {"backend", rendered_discovery_used ? "browser_render" : "broker_search"},
                     {"result_count", results.size()}});

  nlohmann::json items = nlohmann::json::array();
  for (const auto& result : results) {
    items.push_back(
        nlohmann::json{{"url", result.url},
                       {"domain", result.domain},
                       {"title", result.title},
                       {"snippet", result.snippet},
                       {"published_at", result.published_at.has_value()
                                            ? nlohmann::json(*result.published_at)
                                            : nlohmann::json(nullptr)},
                       {"backend", result.backend},
                       {"rendered", result.rendered},
                       {"score", result.score}});
  }
  return nlohmann::json{
      {"query", query},
      {"backend", rendered_discovery_used ? "browser_render" : "broker_search"},
      {"results", std::move(items)}};
}

nlohmann::json BrowsingServer::HandleFetchPayload(const nlohmann::json& payload) {
  const std::string url = TrimCopy(payload.value("url", std::string{}));
  if (url.empty()) {
    throw ApiError(400, "invalid_url", "url is required");
  }
  std::string error_code;
  std::string error_message;
  const auto fetched = FetchUrl(url, &error_code, &error_message);
  if (!fetched.has_value()) {
    throw ApiError(502, error_code.empty() ? "fetch_failed" : error_code, error_message);
  }
  AppendAuditLog(
      nlohmann::json{{"ts", UtcNow()}, {"kind", "fetch"}, {"url", url}, {"final_url", fetched->final_url}});
  nlohmann::json payload_json = {
      {"url", fetched->url},
      {"final_url", fetched->final_url},
      {"content_type", fetched->content_type},
      {"fetched_at", fetched->fetched_at},
      {"backend", fetched->backend},
      {"rendered", fetched->rendered},
      {"title", fetched->title.has_value() ? nlohmann::json(*fetched->title) : nlohmann::json(nullptr)},
      {"screenshot_path",
       fetched->screenshot_path.has_value() ? nlohmann::json(*fetched->screenshot_path)
                                            : nlohmann::json(nullptr)},
      {"visible_text", fetched->visible_text},
      {"response_hash", fetched->response_hash},
      {"citations", fetched->citations},
      {"injection_flags", fetched->injection_flags},
  };
  return payload_json;
}

nlohmann::json BrowsingServer::CreateSession(const nlohmann::json& payload) {
  if (!config_.policy.browser_session_enabled) {
    throw ApiError(409, "browser_session_disabled", "browser sessions are disabled for this plane");
  }
  if (!payload.value("confirmed", false)) {
    throw ApiError(409, "approval_required", "opening a browser session requires confirmed=true");
  }
  SessionRecord session;
  session.id = NewSessionId();
  session.root = config_.state_root / session.id;
  std::filesystem::create_directories(session.root);
  session.created_at = UtcNow();
  session.updated_at = session.created_at;

  const std::string url = TrimCopy(payload.value("url", std::string{}));
  nlohmann::json observation = nullptr;
  if (!url.empty()) {
    if (config_.policy.rendered_browser_enabled && cef_backend_ != nullptr && cef_backend_->IsAvailable()) {
      std::string error_message;
      const auto rendered = cef_backend_->OpenSession(session.id, session.root, url, &error_message);
      if (!rendered.has_value()) {
        std::filesystem::remove_all(session.root);
        throw ApiError(502, "rendered_session_open_failed", error_message);
      }
      session.current_url = rendered->final_url;
      if (const auto host = ExtractHost(rendered->final_url); host.has_value()) {
        session.visited_domains.insert(*host);
      }
      FetchResult fetched =
          BuildRenderedFetchResult(url, *rendered, config_.policy, UtcNow());
      observation = nlohmann::json{
          {"session_id", session.id},
          {"url", fetched.final_url},
          {"backend", fetched.backend},
          {"rendered", fetched.rendered},
          {"observation", "session opened in CEF-backed isolated browser"},
          {"screenshot_path", fetched.screenshot_path.has_value() ? nlohmann::json(*fetched.screenshot_path)
                                                                 : nlohmann::json(nullptr)},
          {"dom_excerpt", fetched.visible_text},
          {"injection_flags", fetched.injection_flags},
          {"requires_confirmation", false},
      };
    } else {
      std::string error_code;
      std::string error_message;
      const auto fetched = FetchUrl(url, &error_code, &error_message);
      if (!fetched.has_value()) {
        std::filesystem::remove_all(session.root);
        throw ApiError(502, error_code.empty() ? "fetch_failed" : error_code, error_message);
      }
      session.current_url = fetched->final_url;
      if (const auto host = ExtractHost(fetched->final_url); host.has_value()) {
        session.visited_domains.insert(*host);
      }
      observation = nlohmann::json{
          {"session_id", session.id},
          {"url", fetched->final_url},
          {"backend", fetched->backend},
          {"rendered", fetched->rendered},
          {"observation", "session opened using v1 brokered browser fallback"},
          {"screenshot_path", fetched->screenshot_path.has_value() ? nlohmann::json(*fetched->screenshot_path)
                                                                   : nlohmann::json(nullptr)},
          {"dom_excerpt", fetched->visible_text},
          {"injection_flags", fetched->injection_flags},
          {"requires_confirmation", false},
      };
    }
  }

  {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    sessions_[session.id] = session;
  }
  AppendAuditLog(
      nlohmann::json{{"ts", UtcNow()}, {"kind", "browser_session_open"}, {"session_id", session.id}, {"url", url}});

  return nlohmann::json{
      {"session_id", session.id},
      {"created_at", session.created_at},
      {"current_url", session.current_url.empty() ? nlohmann::json(nullptr) : nlohmann::json(session.current_url)},
      {"browser_session_enabled", true},
      {"observation", observation},
  };
}

nlohmann::json BrowsingServer::ReadSession(const std::string& session_id) const {
  std::lock_guard<std::mutex> lock(sessions_mutex_);
  const auto it = sessions_.find(session_id);
  if (it == sessions_.end()) {
    throw ApiError(404, "session_not_found", "session not found");
  }
  return nlohmann::json{
      {"session_id", it->second.id},
      {"current_url", it->second.current_url.empty() ? nlohmann::json(nullptr)
                                                      : nlohmann::json(it->second.current_url)},
      {"created_at", it->second.created_at},
      {"updated_at", it->second.updated_at},
      {"visited_domains", std::vector<std::string>(it->second.visited_domains.begin(),
                                                    it->second.visited_domains.end())},
  };
}

nlohmann::json BrowsingServer::ApplySessionAction(
    const std::string& session_id,
    const nlohmann::json& payload) {
  if (!config_.policy.browser_session_enabled) {
    throw ApiError(409, "browser_session_disabled", "browser sessions are disabled for this plane");
  }
  const std::string action = TrimCopy(payload.value("action", std::string{}));
  if (action.empty()) {
    throw ApiError(400, "invalid_action", "action is required");
  }

  SessionRecord session;
  {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    const auto it = sessions_.find(session_id);
    if (it == sessions_.end()) {
      throw ApiError(404, "session_not_found", "session not found");
    }
    session = it->second;
  }

  if (action == "snapshot") {
    session.updated_at = UtcNow();
    {
      std::lock_guard<std::mutex> lock(sessions_mutex_);
      sessions_[session_id] = session;
    }
    if (config_.policy.rendered_browser_enabled && cef_backend_ != nullptr && cef_backend_->IsAvailable()) {
      std::string error_message;
      const auto rendered = cef_backend_->SnapshotSession(session_id, &error_message);
      if (!rendered.has_value()) {
        throw ApiError(502, "rendered_snapshot_failed", error_message);
      }
      FetchResult fetched =
          BuildRenderedFetchResult(session.current_url, *rendered, config_.policy, session.updated_at);
      return nlohmann::json{
          {"session_id", session.id},
          {"url", fetched.final_url},
          {"backend", fetched.backend},
          {"rendered", fetched.rendered},
          {"observation", "rendered snapshot from active browser session"},
          {"screenshot_path", fetched.screenshot_path.has_value() ? nlohmann::json(*fetched.screenshot_path)
                                                                 : nlohmann::json(nullptr)},
          {"dom_excerpt", fetched.visible_text},
          {"injection_flags", fetched.injection_flags},
          {"requires_confirmation", false},
      };
    }
    return nlohmann::json{
        {"session_id", session.id},
        {"url", session.current_url.empty() ? nlohmann::json(nullptr) : nlohmann::json(session.current_url)},
        {"backend", "broker_fetch"},
        {"rendered", false},
        {"observation", "snapshot is not available in v1 browser fallback"},
        {"screenshot_path", nullptr},
        {"dom_excerpt", nullptr},
        {"injection_flags", nlohmann::json::array()},
        {"requires_confirmation", false},
    };
  }

  if (action == "extract") {
    if (session.current_url.empty()) {
      throw ApiError(409, "session_url_missing", "session does not have an active URL");
    }
    if (config_.policy.rendered_browser_enabled && cef_backend_ != nullptr && cef_backend_->IsAvailable()) {
      std::string error_message;
      const auto rendered = cef_backend_->ExtractSession(session_id, &error_message);
      if (!rendered.has_value()) {
        throw ApiError(502, "rendered_extract_failed", error_message);
      }
      session.current_url = rendered->final_url;
      session.updated_at = UtcNow();
      {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        sessions_[session_id] = session;
      }
      FetchResult fetched =
          BuildRenderedFetchResult(session.current_url, *rendered, config_.policy, session.updated_at);
      return nlohmann::json{
          {"session_id", session.id},
          {"url", fetched.final_url},
          {"backend", fetched.backend},
          {"rendered", fetched.rendered},
          {"observation", "sanitized extract from active rendered browser session"},
          {"screenshot_path", fetched.screenshot_path.has_value() ? nlohmann::json(*fetched.screenshot_path)
                                                                 : nlohmann::json(nullptr)},
          {"dom_excerpt", fetched.visible_text},
          {"injection_flags", fetched.injection_flags},
          {"requires_confirmation", false},
      };
    }
    std::string error_code;
    std::string error_message;
    const auto fetched = FetchUrl(session.current_url, &error_code, &error_message);
    if (!fetched.has_value()) {
      throw ApiError(502, error_code.empty() ? "fetch_failed" : error_code, error_message);
    }
    session.updated_at = UtcNow();
    {
      std::lock_guard<std::mutex> lock(sessions_mutex_);
      sessions_[session_id] = session;
    }
    return nlohmann::json{
        {"session_id", session.id},
        {"url", fetched->final_url},
        {"backend", fetched->backend},
        {"rendered", fetched->rendered},
        {"observation", "sanitized extract from current session URL"},
        {"screenshot_path",
         fetched->screenshot_path.has_value() ? nlohmann::json(*fetched->screenshot_path)
                                              : nlohmann::json(nullptr)},
        {"dom_excerpt", fetched->visible_text},
        {"injection_flags", fetched->injection_flags},
        {"requires_confirmation", false},
    };
  }

  if (action == "open") {
    if (!payload.value("confirmed", false)) {
      throw ApiError(409, "approval_required", "opening a new browser target requires confirmed=true");
    }
    const std::string next_url = TrimCopy(payload.value("url", std::string{}));
    if (next_url.empty()) {
      throw ApiError(400, "invalid_url", "url is required for open action");
    }
    if (config_.policy.rendered_browser_enabled && cef_backend_ != nullptr && cef_backend_->IsAvailable()) {
      std::string error_message;
      const auto rendered = cef_backend_->OpenSession(session_id, session.root, next_url, &error_message);
      if (!rendered.has_value()) {
        throw ApiError(502, "rendered_open_failed", error_message);
      }
      session.current_url = rendered->final_url;
      session.updated_at = UtcNow();
      if (const auto host = ExtractHost(rendered->final_url); host.has_value()) {
        session.visited_domains.insert(*host);
      }
      {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        sessions_[session_id] = session;
      }
      FetchResult fetched =
          BuildRenderedFetchResult(next_url, *rendered, config_.policy, session.updated_at);
      return nlohmann::json{
          {"session_id", session.id},
          {"url", fetched.final_url},
          {"backend", fetched.backend},
          {"rendered", fetched.rendered},
          {"observation", "session navigated in CEF-backed isolated browser"},
          {"screenshot_path", fetched.screenshot_path.has_value() ? nlohmann::json(*fetched.screenshot_path)
                                                                 : nlohmann::json(nullptr)},
          {"dom_excerpt", fetched.visible_text},
          {"injection_flags", fetched.injection_flags},
          {"requires_confirmation", false},
      };
    }
    std::string error_code;
    std::string error_message;
    const auto fetched = FetchUrl(next_url, &error_code, &error_message);
    if (!fetched.has_value()) {
      throw ApiError(502, error_code.empty() ? "fetch_failed" : error_code, error_message);
    }
    session.current_url = fetched->final_url;
    session.updated_at = UtcNow();
    if (const auto host = ExtractHost(fetched->final_url); host.has_value()) {
      session.visited_domains.insert(*host);
    }
    {
      std::lock_guard<std::mutex> lock(sessions_mutex_);
      sessions_[session_id] = session;
    }
    return nlohmann::json{
        {"session_id", session.id},
        {"url", fetched->final_url},
        {"backend", fetched->backend},
        {"rendered", fetched->rendered},
        {"observation", "session navigated using v1 brokered browser fallback"},
        {"screenshot_path",
         fetched->screenshot_path.has_value() ? nlohmann::json(*fetched->screenshot_path)
                                              : nlohmann::json(nullptr)},
        {"dom_excerpt", fetched->visible_text},
        {"injection_flags", fetched->injection_flags},
        {"requires_confirmation", false},
    };
  }

  if (action == "click" || action == "type" || action == "submit") {
    if (!payload.value("confirmed", false)) {
      throw ApiError(409, "approval_required", action + " requires confirmed=true");
    }
    throw ApiError(501, "browser_action_not_supported", action + " is not supported in v1");
  }

  throw ApiError(400, "invalid_action", "unsupported browser action");
}

nlohmann::json BrowsingServer::DeleteSession(const std::string& session_id) {
  SessionRecord session;
  {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    const auto it = sessions_.find(session_id);
    if (it == sessions_.end()) {
      throw ApiError(404, "session_not_found", "session not found");
    }
    session = it->second;
    sessions_.erase(it);
  }
  if (config_.policy.rendered_browser_enabled && cef_backend_ != nullptr && cef_backend_->IsAvailable()) {
    cef_backend_->DeleteSession(session_id);
  }
  std::error_code error;
  std::filesystem::remove_all(session.root, error);
  AppendAuditLog(
      nlohmann::json{{"ts", UtcNow()}, {"kind", "browser_session_delete"}, {"session_id", session_id}});
  return nlohmann::json{{"deleted", true}, {"session_id", session_id}};
}

std::string BrowsingServer::NewSessionId() const {
  return ShellSafeToken(comet::RandomTokenBase64(12));
}

std::optional<FetchResult> BrowsingServer::FetchUrlViaBroker(
    const std::string& url,
    std::string* error_code,
    std::string* error_message) const {
  std::string host;
  std::string reason;
  if (!IsSafeBrowsingUrl(url, config_.policy, &reason, &host)) {
    if (error_code != nullptr) {
      *error_code = "unsafe_url";
    }
    if (error_message != nullptr) {
      *error_message = reason;
    }
    return std::nullopt;
  }

  const auto temp_root = config_.state_root / ("fetch-" + ShellSafeToken(comet::RandomTokenBase64(8)));
  std::filesystem::create_directories(temp_root);
  const auto header_path = temp_root / "headers.txt";
  const auto body_path = temp_root / "body.txt";

  std::optional<FetchResult> result;
  try {
    const auto command = RunCommandCapture(CommandRequest{
        .args =
            {"/usr/bin/curl",
             "-fsSL",
             "--proto",
             "=https,http",
             "--connect-timeout",
             "5",
             "--max-time",
             "20",
             "--max-redirs",
             "5",
             "-D",
             header_path.string(),
             "-o",
             body_path.string(),
             "-w",
             "%{url_effective}\n%{content_type}",
             url},
        .environment = {},
        .working_directory = std::nullopt,
        .clear_environment = false,
        .merge_stderr_into_stdout = true,
    });
    if (command.exit_code != 0) {
      if (error_code != nullptr) {
        *error_code = "fetch_upstream_failed";
      }
      if (error_message != nullptr) {
        *error_message = TrimCopy(command.output);
      }
      std::filesystem::remove_all(temp_root);
      return std::nullopt;
    }

    std::istringstream meta(command.output);
    std::string final_url;
    std::string content_type;
    std::getline(meta, final_url);
    std::getline(meta, content_type);
    final_url = TrimCopy(final_url);
    content_type = TrimCopy(content_type);

    std::string final_host;
    if (!IsSafeBrowsingUrl(final_url.empty() ? url : final_url, config_.policy, &reason, &final_host)) {
      if (error_code != nullptr) {
        *error_code = "unsafe_redirect_target";
      }
      if (error_message != nullptr) {
        *error_message = reason;
      }
      std::filesystem::remove_all(temp_root);
      return std::nullopt;
    }

    const std::string body = ReadFileOrThrow(body_path);
    FetchResult fetched = SanitizeFetchedDocument(url, final_url, content_type, body, config_.policy);
    fetched.fetched_at = UtcNow();
    result = std::move(fetched);
  } catch (const ApiError&) {
    std::filesystem::remove_all(temp_root);
    throw;
  } catch (const std::exception& error) {
    if (error_code != nullptr) {
      *error_code = "fetch_failed";
    }
    if (error_message != nullptr) {
      *error_message = error.what();
    }
    std::filesystem::remove_all(temp_root);
    return std::nullopt;
  }

  std::filesystem::remove_all(temp_root);
  return result;
}

std::optional<FetchResult> BrowsingServer::FetchUrl(
    const std::string& url,
    std::string* error_code,
    std::string* error_message) const {
  std::string host;
  std::string reason;
  if (!IsSafeBrowsingUrl(url, config_.policy, &reason, &host)) {
    if (error_code != nullptr) {
      *error_code = "unsafe_url";
    }
    if (error_message != nullptr) {
      *error_message = reason;
    }
    return std::nullopt;
  }

  const bool rendered_backend_ready =
      config_.policy.rendered_browser_enabled && cef_backend_ != nullptr && cef_backend_->IsAvailable();
  const bool prefer_rendered = rendered_backend_ready && IsJsHeavyDomain(host);

  if (prefer_rendered) {
    std::string rendered_error;
    const auto rendered = cef_backend_->FetchPage(
        url,
        config_.state_root / ("rendered-fetch-" + ShellSafeToken(comet::RandomTokenBase64(8))),
        &rendered_error,
        false);
    if (rendered.has_value()) {
      return BuildRenderedFetchResult(url, *rendered, config_.policy, UtcNow());
    }
    if (error_message != nullptr && !rendered_error.empty()) {
      *error_message = rendered_error;
    }
  }

  auto brokered = FetchUrlViaBroker(url, error_code, error_message);
  if (!brokered.has_value()) {
    return std::nullopt;
  }

  if (rendered_backend_ready && EvidenceLooksThin(*brokered)) {
    std::string rendered_error;
    const auto rendered = cef_backend_->FetchPage(
        url,
        config_.state_root / ("rendered-fetch-" + ShellSafeToken(comet::RandomTokenBase64(8))),
        &rendered_error,
        false);
    if (rendered.has_value()) {
      return BuildRenderedFetchResult(url, *rendered, config_.policy, UtcNow());
    }
  }

  return brokered;
}

}  // namespace comet::browsing
