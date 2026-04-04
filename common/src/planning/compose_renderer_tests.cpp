#include <iostream>
#include <stdexcept>
#include <string>

#include "comet/planning/compose_renderer.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

}  // namespace

int main() {
  try {
    comet::NodeComposePlan plan;
    plan.plane_name = "plane-a";
    plan.node_name = "local-hostd";

    comet::ComposeService service;
    service.name = "worker-a";
    service.image = "example/worker:dev";
    service.command = "/runtime/bin/comet-workerd";
    service.use_nvidia_runtime = true;
    service.gpu_devices = {"0", "2", "3", "0"};
    service.healthcheck = "CMD-SHELL test -f /tmp/comet-ready";
    plan.services.push_back(std::move(service));

    const std::string yaml = comet::RenderComposeYaml(plan);
    Expect(
        yaml.find("device_ids: [\"0\", \"2\", \"3\"]") != std::string::npos,
        "compose renderer should deduplicate repeated gpu device ids");
    Expect(
        yaml.find("device_ids: [\"0\", \"2\", \"3\", \"0\"]") == std::string::npos,
        "compose renderer should not emit duplicate gpu device ids");

    std::cout << "compose renderer tests passed\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << "\n";
    return 1;
  }
}
