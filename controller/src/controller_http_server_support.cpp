#include "../include/controller_http_server_support.h"

#include <cctype>
#include <cstdlib>
#include <iomanip>
#include <sstream>

namespace comet::controller {

std::string ControllerHttpServerSupport::TrimCopy(const std::string& value) {
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

std::string ControllerHttpServerSupport::LowercaseCopy(
    const std::string& value) {
  std::string lowered;
  lowered.reserve(value.size());
  for (unsigned char ch : value) {
    lowered.push_back(static_cast<char>(std::tolower(ch)));
  }
  return lowered;
}

std::map<std::string, std::string>
ControllerHttpServerSupport::ParseQueryParams(const std::string& query_text) {
  const auto decode_component = [](const std::string& value) {
    std::string decoded;
    decoded.reserve(value.size());
    for (std::size_t i = 0; i < value.size(); ++i) {
      if (value[i] == '+') {
        decoded.push_back(' ');
        continue;
      }
      if (value[i] == '%' && i + 2 < value.size()) {
        const std::string hex = value.substr(i + 1, 2);
        char* end = nullptr;
        const long code = std::strtol(hex.c_str(), &end, 16);
        if (end != nullptr && *end == '\0') {
          decoded.push_back(static_cast<char>(code));
          i += 2;
          continue;
        }
      }
      decoded.push_back(value[i]);
    }
    return decoded;
  };

  std::map<std::string, std::string> params;
  std::size_t offset = 0;
  while (offset <= query_text.size()) {
    const std::size_t next = query_text.find('&', offset);
    const std::string pair = query_text.substr(
        offset,
        next == std::string::npos ? std::string::npos : next - offset);
    if (!pair.empty()) {
      const std::size_t equals = pair.find('=');
      if (equals == std::string::npos) {
        params.emplace(decode_component(pair), "");
      } else {
        params.emplace(
            decode_component(pair.substr(0, equals)),
            decode_component(pair.substr(equals + 1)));
      }
    }
    if (next == std::string::npos) {
      break;
    }
    offset = next + 1;
  }
  return params;
}

std::string ControllerHttpServerSupport::UrlEncode(std::string_view value) {
  std::ostringstream encoded;
  for (const unsigned char ch : value) {
    if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
        (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' ||
        ch == '~' || ch == '/' || ch == ':') {
      encoded << static_cast<char>(ch);
    } else if (ch == ' ') {
      encoded << '+';
    } else {
      encoded << '%' << std::uppercase << std::hex << std::setw(2)
              << std::setfill('0') << static_cast<int>(ch)
              << std::nouppercase << std::dec;
    }
  }
  return encoded.str();
}

HttpRequest ControllerHttpServerSupport::ParseHttpRequest(
    const std::string& request_text) {
  HttpRequest request;
  const std::size_t headers_end = request_text.find("\r\n\r\n");
  const std::string header_text = headers_end == std::string::npos
                                      ? request_text
                                      : request_text.substr(0, headers_end);
  request.body = headers_end == std::string::npos
                     ? std::string{}
                     : request_text.substr(headers_end + 4);

  const std::size_t line_end = header_text.find("\r\n");
  const std::string first_line = line_end == std::string::npos
                                     ? header_text
                                     : header_text.substr(0, line_end);
  std::stringstream stream(first_line);
  stream >> request.method >> request.path;
  if (request.path.empty()) {
    request.path = "/";
  }
  const std::size_t query = request.path.find('?');
  if (query != std::string::npos) {
    request.query_params = ParseQueryParams(request.path.substr(query + 1));
    request.path = request.path.substr(0, query);
  }

  std::size_t offset =
      line_end == std::string::npos ? header_text.size() : line_end + 2;
  while (offset < header_text.size()) {
    const std::size_t next = header_text.find("\r\n", offset);
    const std::string line = header_text.substr(
        offset,
        next == std::string::npos ? std::string::npos : next - offset);
    const std::size_t colon = line.find(':');
    if (colon != std::string::npos) {
      request.headers.emplace(
          LowercaseCopy(TrimCopy(line.substr(0, colon))),
          TrimCopy(line.substr(colon + 1)));
    }
    if (next == std::string::npos) {
      break;
    }
    offset = next + 2;
  }
  return request;
}

std::size_t ControllerHttpServerSupport::ExpectedRequestBytes(
    const std::string& request_text) {
  const std::size_t headers_end = request_text.find("\r\n\r\n");
  if (headers_end == std::string::npos) {
    return 0;
  }
  const std::string header_text = request_text.substr(0, headers_end);
  std::size_t offset = 0;
  std::size_t content_length = 0;
  while (offset < header_text.size()) {
    const std::size_t next = header_text.find("\r\n", offset);
    const std::string line = header_text.substr(
        offset,
        next == std::string::npos ? std::string::npos : next - offset);
    const std::size_t colon = line.find(':');
    if (colon != std::string::npos) {
      const std::string key = LowercaseCopy(TrimCopy(line.substr(0, colon)));
      const std::string value = TrimCopy(line.substr(colon + 1));
      if (key == "content-length") {
        content_length = static_cast<std::size_t>(std::stoul(value));
        break;
      }
    }
    if (next == std::string::npos) {
      break;
    }
    offset = next + 2;
  }
  return headers_end + 4 + content_length;
}

std::optional<std::string> ControllerHttpServerSupport::FindHeaderString(
    const HttpRequest& request,
    const std::string& key) {
  const auto it = request.headers.find(LowercaseCopy(key));
  if (it == request.headers.end() || it->second.empty()) {
    return std::nullopt;
  }
  return it->second;
}

bool ControllerHttpServerSupport::StartsWithPath(
    const std::string& value,
    const std::string& prefix) {
  return value.size() >= prefix.size() &&
         value.compare(0, prefix.size(), prefix) == 0;
}

}  // namespace comet::controller
