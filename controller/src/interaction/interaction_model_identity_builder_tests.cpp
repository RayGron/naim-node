#include <iostream>
#include <stdexcept>
#include <string>

#include "interaction/interaction_model_identity_builder.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestBuildRuntimePreferredUsesRuntimeIdentity() {
  const naim::controller::InteractionModelIdentityBuilder builder;
  naim::controller::PlaneInteractionResolution resolution;
  resolution.status_payload = {
      {"served_model_name", "status-served"},
      {"active_model_id", "status-active"},
  };
  naim::RuntimeStatus runtime_status;
  runtime_status.active_served_model_name = "runtime-served";
  runtime_status.active_model_id = "runtime-active";
  runtime_status.cached_local_model_path = "/models/local.gguf";
  runtime_status.model_path = "/runtime/model.gguf";
  runtime_status.active_runtime_profile = "cuda";
  resolution.runtime_status = runtime_status;

  const auto identity = builder.BuildRuntimePreferred(resolution);
  Expect(identity.served_model_name == "runtime-served",
         "runtime-preferred identity should prefer runtime served model");
  Expect(identity.model_id == "runtime-active",
         "runtime-preferred identity should prefer runtime model id");
  Expect(identity.cached_local_model_path == "/models/local.gguf",
         "runtime-preferred identity should keep cached local model path");
  std::cout << "ok: interaction-model-identity-runtime-preferred" << '\n';
}

void TestBuildStatusPreferredPreservesStatusIdentity() {
  const naim::controller::InteractionModelIdentityBuilder builder;
  naim::controller::PlaneInteractionResolution resolution;
  resolution.status_payload = {
      {"served_model_name", "status-served"},
      {"active_model_id", "status-active"},
  };
  naim::RuntimeStatus runtime_status;
  runtime_status.active_served_model_name = "runtime-served";
  runtime_status.active_model_id = "runtime-active";
  resolution.runtime_status = runtime_status;

  const auto identity = builder.BuildStatusPreferred(resolution);
  Expect(identity.served_model_name == "status-served",
         "status-preferred identity should preserve status served model");
  Expect(identity.model_id == "status-active",
         "status-preferred identity should preserve status model id");
  std::cout << "ok: interaction-model-identity-status-preferred" << '\n';
}

void TestBuildStatusPreferredFallsBackToRuntimeWhenMissing() {
  const naim::controller::InteractionModelIdentityBuilder builder;
  naim::controller::PlaneInteractionResolution resolution;
  resolution.status_payload = nlohmann::json::object();
  naim::RuntimeStatus runtime_status;
  runtime_status.active_served_model_name = "runtime-served";
  runtime_status.active_model_id = "runtime-active";
  resolution.runtime_status = runtime_status;

  const auto identity = builder.BuildStatusPreferred(resolution);
  Expect(identity.served_model_name == "runtime-served",
         "status-preferred identity should fall back to runtime served model");
  Expect(identity.model_id == "runtime-active",
         "status-preferred identity should fall back to runtime model id");
  std::cout << "ok: interaction-model-identity-status-fallback" << '\n';
}

}  // namespace

int main() {
  try {
    TestBuildRuntimePreferredUsesRuntimeIdentity();
    TestBuildStatusPreferredPreservesStatusIdentity();
    TestBuildStatusPreferredFallsBackToRuntimeWhenMissing();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_model_identity_builder_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
