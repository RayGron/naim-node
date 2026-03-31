#include <iostream>
#include <stdexcept>
#include <string>

#include "comet/state/models.h"
#include "skills/plane_skills_service.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

comet::DesiredState BuildDesiredStateWithSkillsPort(const std::string& host_ip) {
  comet::DesiredState desired_state;
  desired_state.plane_name = "maglev";
  desired_state.plane_mode = comet::PlaneMode::Llm;
  comet::SkillsSettings skills_settings;
  skills_settings.enabled = true;
  desired_state.skills = skills_settings;

  comet::InstanceSpec skills;
  skills.name = "skills-maglev";
  skills.plane_name = "maglev";
  skills.node_name = "local-hostd";
  skills.role = comet::InstanceRole::Skills;
  comet::PublishedPort published_port;
  published_port.host_ip = host_ip;
  published_port.host_port = 27978;
  published_port.container_port = 18120;
  skills.published_ports.push_back(published_port);
  desired_state.instances.push_back(skills);
  return desired_state;
}

}  // namespace

int main() {
  try {
    comet::controller::PlaneSkillsService service;

    {
      const auto target =
          service.ResolveTarget(BuildDesiredStateWithSkillsPort("127.0.0.1"));
      Expect(target.has_value(), "skills target should resolve when a published port exists");
      Expect(target->host == "127.0.0.1", "skills target host should use published host_ip");
      Expect(target->port == 27978, "skills target port should use published host_port");
      Expect(
          target->raw == "http://127.0.0.1:27978",
          "skills target raw URL should use normalized published endpoint");
      std::cout << "ok: published-host-ip-target" << '\n';
    }

    {
      const auto target =
          service.ResolveTarget(BuildDesiredStateWithSkillsPort("0.0.0.0"));
      Expect(target.has_value(), "skills target should resolve for wildcard published host_ip");
      Expect(
          target->host == "127.0.0.1",
          "skills target host should normalize wildcard host_ip for controller probes");
      Expect(
          target->raw == "http://127.0.0.1:27978",
          "skills target raw URL should normalize wildcard host_ip");
      std::cout << "ok: wildcard-host-ip-normalization" << '\n';
    }

    return 0;
  } catch (const std::exception& error) {
    std::cerr << "plane_skills_service_tests failed: " << error.what() << '\n';
    return 1;
  }
}
