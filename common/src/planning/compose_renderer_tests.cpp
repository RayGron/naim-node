#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>

#include "comet/planning/planner.h"
#include "comet/planning/compose_renderer.h"
#include "comet/state/demo_state.h"

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
    service.environment["COMET_WEBGATEWAY_POLICY_JSON"] =
        R"({"blocked_domains":["localhost","127.0.0.1","internal"],"browser_session_enabled":true})";
    plan.services.push_back(std::move(service));

    const std::string yaml = comet::RenderComposeYaml(plan);
    Expect(
        yaml.find("device_ids: [\"0\", \"2\", \"3\"]") != std::string::npos,
        "compose renderer should deduplicate repeated gpu device ids");
    Expect(
        yaml.find("device_ids: [\"0\", \"2\", \"3\", \"0\"]") == std::string::npos,
        "compose renderer should not emit duplicate gpu device ids");
    Expect(
        yaml.find(
            R"(COMET_WEBGATEWAY_POLICY_JSON: "{\"blocked_domains\":[\"localhost\",\"127.0.0.1\",\"internal\"],\"browser_session_enabled\":true}")") !=
            std::string::npos,
        "compose renderer should escape JSON-valued env vars");
    Expect(
        yaml.find(R"(command: ["/runtime/bin/comet-workerd"])") != std::string::npos,
        "compose renderer should keep single-token commands as a single array element");

    comet::ComposeService infer_service;
    infer_service.name = "infer-a";
    infer_service.image = "example/infer:dev";
    infer_service.command = "/runtime/bin/comet-inferctl container-boot --backend llama-rpc-head";
    infer_service.healthcheck = "NONE";
    plan.services.push_back(std::move(infer_service));

    comet::ComposeService browsing_service;
    browsing_service.name = "webgateway-a";
    browsing_service.image = "example/webgateway:dev";
    browsing_service.command = "/runtime/bin/comet-webgatewayd";
    browsing_service.privileged = true;
    browsing_service.healthcheck = "NONE";
    plan.services.push_back(std::move(browsing_service));

    const std::string yaml_with_infer = comet::RenderComposeYaml(plan);
    Expect(
        yaml_with_infer.find(
            R"(command: ["/runtime/bin/comet-inferctl", "container-boot", "--backend", "llama-rpc-head"])") !=
            std::string::npos,
        "compose renderer should split command strings with arguments into compose array tokens");
    Expect(
        yaml_with_infer.find("privileged: true") != std::string::npos,
        "compose renderer should emit privileged mode for services that require it");

#if defined(_WIN32)
    _putenv_s("COMET_CONTROLLER_INTERNAL_HOST", "192.168.88.13");
#else
    setenv("COMET_CONTROLLER_INTERNAL_HOST", "192.168.88.13", 1);
#endif
    const auto plans = comet::BuildNodeComposePlans(comet::BuildDemoState());
    Expect(!plans.empty(), "planner should render at least one compose plan");
    Expect(!plans.front().services.empty(), "planner should render at least one service");
    const auto& extra_hosts = plans.front().services.front().extra_hosts;
    Expect(
        std::find(extra_hosts.begin(), extra_hosts.end(), "controller.internal:192.168.88.13") !=
            extra_hosts.end(),
        "planner should map controller.internal to the configured internal host");

    auto llama_rpc_state = comet::BuildDemoState();
    llama_rpc_state.inference.runtime_engine = "llama.cpp";
    llama_rpc_state.inference.distributed_backend = "llama_rpc";
    const auto llama_rpc_plans = comet::BuildNodeComposePlans(llama_rpc_state);
    Expect(!llama_rpc_plans.empty(),
           "planner should render a compose plan for llama-rpc demo state");
    const auto infer_it = std::find_if(
        llama_rpc_plans.front().services.begin(),
        llama_rpc_plans.front().services.end(),
        [](const comet::ComposeService& compose_service) {
          return compose_service.name == "infer-main";
        });
    Expect(infer_it != llama_rpc_plans.front().services.end(),
           "planner should include infer service in llama-rpc compose plan");
    Expect(infer_it->gpu_devices.size() == 1 && infer_it->gpu_devices.front() == "0",
           "planner should inherit local worker gpu devices for llama-rpc infer");
    const auto visible_devices = infer_it->environment.find("NVIDIA_VISIBLE_DEVICES");
    Expect(
        visible_devices != infer_it->environment.end() && visible_devices->second == "0",
        "planner should expose inherited gpu devices through NVIDIA_VISIBLE_DEVICES");

#if defined(_WIN32)
    _putenv_s("COMET_CONTROLLER_INTERNAL_HOST", "");
#else
    unsetenv("COMET_CONTROLLER_INTERNAL_HOST");
#endif

    std::cout << "compose renderer tests passed\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << "\n";
    return 1;
  }
}
