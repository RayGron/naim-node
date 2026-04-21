#include "naim/planning/compose_renderer.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <string_view>
#include <vector>

namespace naim {

namespace {

std::string EscapeYamlDoubleQuoted(const std::string& value) {
  std::string escaped;
  escaped.reserve(value.size());
  for (const char ch : value) {
    switch (ch) {
      case '\\':
        escaped += "\\\\";
        break;
      case '"':
        escaped += "\\\"";
        break;
      case '\n':
        escaped += "\\n";
        break;
      case '\r':
        escaped += "\\r";
        break;
      case '\t':
        escaped += "\\t";
        break;
      default:
        escaped.push_back(ch);
        break;
    }
  }
  return escaped;
}

void RenderKeyValueMap(
    std::ostringstream& out,
    const std::string& indent,
    const std::map<std::string, std::string>& values) {
  for (const auto& [key, value] : values) {
    out << indent << key << ": \"" << EscapeYamlDoubleQuoted(value) << "\"\n";
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
        << EscapeYamlDoubleQuoted(healthcheck.substr(kCmdShellPrefix.size())) << "\"]\n";
    return;
  }

  if (healthcheck.rfind(std::string(kCmdPrefix), 0) == 0) {
    out << indent << "test: [\"CMD\", \"" << EscapeYamlDoubleQuoted(healthcheck.substr(kCmdPrefix.size()))
        << "\"]\n";
    return;
  }

  out << indent << "test: [\"CMD-SHELL\", \"" << EscapeYamlDoubleQuoted(healthcheck) << "\"]\n";
}

std::vector<std::string> UniqueGpuDevices(
    const std::vector<std::string>& gpu_devices) {
  std::vector<std::string> result;
  result.reserve(gpu_devices.size());
  for (const auto& gpu_device : gpu_devices) {
    if (std::find(result.begin(), result.end(), gpu_device) != result.end()) {
      continue;
    }
    result.push_back(gpu_device);
  }
  return result;
}

std::vector<std::string> SplitCommandTokens(const std::string& command) {
  std::vector<std::string> tokens;
  std::string current;
  bool in_single_quotes = false;
  bool in_double_quotes = false;
  bool escape_next = false;

  for (const char ch : command) {
    if (escape_next) {
      current.push_back(ch);
      escape_next = false;
      continue;
    }

    if (ch == '\\' && !in_single_quotes) {
      escape_next = true;
      continue;
    }

    if (ch == '\'' && !in_double_quotes) {
      in_single_quotes = !in_single_quotes;
      continue;
    }

    if (ch == '"' && !in_single_quotes) {
      in_double_quotes = !in_double_quotes;
      continue;
    }

    if (!in_single_quotes && !in_double_quotes &&
        std::isspace(static_cast<unsigned char>(ch)) != 0) {
      if (!current.empty()) {
        tokens.push_back(current);
        current.clear();
      }
      continue;
    }

    current.push_back(ch);
  }

  if (escape_next) {
    current.push_back('\\');
  }
  if (!current.empty()) {
    tokens.push_back(std::move(current));
  }
  if (tokens.empty() && !command.empty()) {
    tokens.push_back(command);
  }
  return tokens;
}

void RenderCommand(
    std::ostringstream& out,
    const std::string& indent,
    const std::string& command) {
  const auto tokens = SplitCommandTokens(command);
  out << indent << "command: [";
  for (std::size_t index = 0; index < tokens.size(); ++index) {
    if (index > 0) {
      out << ", ";
    }
    out << "\"" << EscapeYamlDoubleQuoted(tokens[index]) << "\"";
  }
  out << "]\n";
}

}  // namespace

std::string RenderComposeYaml(const NodeComposePlan& plan) {
  std::ostringstream out;
  const std::string mesh_network_key = "plane-mesh";
  const std::string mesh_network_name = "naim-" + plan.plane_name + "-mesh";

  out << "name: naim-" << plan.plane_name << "-" << plan.node_name << "\n";
  out << "services:\n";

  for (const auto& service : plan.services) {
    out << "  " << service.name << ":\n";
    out << "    image: " << service.image << "\n";
    RenderCommand(out, "    ", service.command);
    out << "    restart: unless-stopped\n";

    if (!service.depends_on.empty()) {
      out << "    depends_on:\n";
      for (const auto& dependency : service.depends_on) {
        out << "      - " << dependency << "\n";
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

    if (service.privileged) {
      out << "    privileged: true\n";
    }

    if (!service.extra_hosts.empty()) {
      out << "    extra_hosts:\n";
      for (const auto& extra_host : service.extra_hosts) {
        out << "      - \"" << extra_host << "\"\n";
      }
    }

    if (service.use_nvidia_runtime) {
      out << "    runtime: nvidia\n";
    }

    if (service.shm_size.has_value() && !service.shm_size->empty()) {
      out << "    shm_size: " << *service.shm_size << "\n";
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

    out << "    networks:\n";
    out << "      " << mesh_network_key << ":\n";
    out << "        aliases:\n";
    out << "          - " << service.name << "\n";

    std::vector<std::string> gpu_devices = UniqueGpuDevices(service.gpu_devices);
    if (gpu_devices.empty() && service.gpu_device.has_value()) {
      gpu_devices.push_back(*service.gpu_device);
    }
    if (!gpu_devices.empty()) {
      std::ostringstream device_ids;
      for (std::size_t index = 0; index < gpu_devices.size(); ++index) {
        if (index > 0) {
          device_ids << ", ";
        }
        device_ids << "\"" << gpu_devices[index] << "\"";
      }
      out << "    gpus:\n";
      out << "      - driver: nvidia\n";
      out << "        device_ids: [" << device_ids.str() << "]\n";
      out << "    deploy:\n";
      out << "      resources:\n";
      out << "        reservations:\n";
      out << "          devices:\n";
      out << "            - driver: nvidia\n";
      out << "              device_ids: [" << device_ids.str() << "]\n";
      out << "              capabilities: [gpu]\n";
    }

    out << "    healthcheck:\n";
    RenderHealthcheckTest(out, "      ", service.healthcheck);
      out << "      interval: 15s\n";
      out << "      timeout: 5s\n";
      out << "      retries: 10\n";
  }

  out << "networks:\n";
  out << "  " << mesh_network_key << ":\n";
  out << "    external: true\n";
  out << "    name: " << mesh_network_name << "\n";

  return out.str();
}

}  // namespace naim
