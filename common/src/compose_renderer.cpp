#include "comet/compose_renderer.h"

#include <sstream>
#include <string_view>

namespace comet {

namespace {

void RenderKeyValueMap(
    std::ostringstream& out,
    const std::string& indent,
    const std::map<std::string, std::string>& values) {
  for (const auto& [key, value] : values) {
    out << indent << key << ": \"" << value << "\"\n";
  }
}

void RenderHealthcheckTest(
    std::ostringstream& out,
    const std::string& indent,
    const std::string& healthcheck) {
  constexpr std::string_view kCmdShellPrefix = "CMD-SHELL ";
  constexpr std::string_view kCmdPrefix = "CMD ";
  constexpr std::string_view kNone = "NONE";

  if (healthcheck == kNone) {
    out << indent << "test: [\"NONE\"]\n";
    return;
  }

  if (healthcheck.rfind(std::string(kCmdShellPrefix), 0) == 0) {
    out << indent << "test: [\"CMD-SHELL\", \""
        << healthcheck.substr(kCmdShellPrefix.size()) << "\"]\n";
    return;
  }

  if (healthcheck.rfind(std::string(kCmdPrefix), 0) == 0) {
    out << indent << "test: [\"CMD\", \"" << healthcheck.substr(kCmdPrefix.size())
        << "\"]\n";
    return;
  }

  out << indent << "test: [\"CMD-SHELL\", \"" << healthcheck << "\"]\n";
}

}  // namespace

std::string RenderComposeYaml(const NodeComposePlan& plan) {
  std::ostringstream out;

  out << "name: comet-" << plan.plane_name << "-" << plan.node_name << "\n";
  out << "services:\n";

  for (const auto& service : plan.services) {
    out << "  " << service.name << ":\n";
    out << "    image: " << service.image << "\n";
    out << "    command: [\"" << service.command << "\"]\n";
    out << "    restart: unless-stopped\n";

    if (!service.depends_on.empty()) {
      out << "    depends_on:\n";
      for (const auto& dependency : service.depends_on) {
        out << "      " << dependency << ":\n";
        out << "        condition: service_healthy\n";
      }
    }

    if (!service.environment.empty()) {
      out << "    environment:\n";
      RenderKeyValueMap(out, "      ", service.environment);
    }

    if (!service.labels.empty()) {
      out << "    labels:\n";
      RenderKeyValueMap(out, "      ", service.labels);
    }

    if (!service.security_opts.empty()) {
      out << "    security_opt:\n";
      for (const auto& security_opt : service.security_opts) {
        out << "      - " << security_opt << "\n";
      }
    }

    if (!service.extra_hosts.empty()) {
      out << "    extra_hosts:\n";
      for (const auto& extra_host : service.extra_hosts) {
        out << "      - \"" << extra_host << "\"\n";
      }
    }

    if (!service.volumes.empty()) {
      out << "    volumes:\n";
      for (const auto& volume : service.volumes) {
        out << "      - type: bind\n";
        out << "        source: " << volume.source << "\n";
        out << "        target: " << volume.target << "\n";
        if (volume.read_only) {
          out << "        read_only: true\n";
        }
      }
    }

    if (!service.published_ports.empty()) {
      out << "    ports:\n";
      for (const auto& port : service.published_ports) {
        out << "      - \"" << port.host_ip << ":" << port.host_port << ":"
            << port.container_port << "\"\n";
      }
    }

    if (service.gpu_device.has_value()) {
      out << "    gpus:\n";
      out << "      - driver: nvidia\n";
      out << "        device_ids: [\"" << *service.gpu_device << "\"]\n";
      out << "    deploy:\n";
      out << "      resources:\n";
      out << "        reservations:\n";
      out << "          devices:\n";
      out << "            - driver: nvidia\n";
      out << "              device_ids: [\"" << *service.gpu_device << "\"]\n";
      out << "              capabilities: [gpu]\n";
    }

    out << "    healthcheck:\n";
    RenderHealthcheckTest(out, "      ", service.healthcheck);
    out << "      interval: 15s\n";
    out << "      timeout: 5s\n";
    out << "      retries: 10\n";
  }

  return out.str();
}

}  // namespace comet
