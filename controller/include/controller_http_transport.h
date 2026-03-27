#pragma once

#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "interaction_service.h"

struct HttpResponse {
  int status_code = 200;
  std::string content_type = "application/json";
  std::string body;
  std::map<std::string, std::string> headers;
};

comet::controller::ControllerEndpointTarget ParseControllerEndpointTarget(
    const std::string& raw_target);

HttpResponse ParseHttpResponse(const std::string& response_text);

std::optional<std::string> FindHttpHeaderValue(
    const std::string& header_text,
    const std::string& header_name);

HttpResponse SendControllerHttpRequest(
    const comet::controller::ControllerEndpointTarget& target,
    const std::string& method,
    const std::string& path_and_query,
    const std::string& body = "",
    const std::vector<std::pair<std::string, std::string>>& headers = {});

comet::controller::InteractionStreamingUpstreamConnection
OpenInteractionStreamRequest(
    const comet::controller::ControllerEndpointTarget& target,
    const std::string& request_id,
    const std::string& body);
