#include "interaction/interaction_model_identity_builder.h"

#include "interaction/interaction_request_contract_support.h"

namespace naim::controller {

naim::runtime::ModelIdentity
InteractionModelIdentityBuilder::BuildRuntimePreferred(
    const PlaneInteractionResolution& resolution) const {
  const InteractionRequestContractSupport request_contract_support;
  naim::runtime::ModelIdentity identity;
  identity.model_id =
      request_contract_support.ResolveInteractionActiveModelId(resolution);
  identity.served_model_name =
      request_contract_support.ResolveInteractionServedModelName(resolution);
  if (resolution.runtime_status.has_value()) {
    identity.cached_local_model_path =
        resolution.runtime_status->cached_local_model_path;
    identity.cached_runtime_model_path =
        resolution.runtime_status->model_path;
    identity.runtime_profile =
        resolution.runtime_status->active_runtime_profile;
  }
  return identity;
}

naim::runtime::ModelIdentity
InteractionModelIdentityBuilder::BuildStatusPreferred(
    const PlaneInteractionResolution& resolution) const {
  naim::runtime::ModelIdentity identity;
  identity.model_id =
      ReadJsonStringOrEmpty(resolution.status_payload, "active_model_id");
  identity.served_model_name =
      ReadJsonStringOrEmpty(resolution.status_payload, "served_model_name");
  if (resolution.runtime_status.has_value()) {
    if (identity.model_id.empty()) {
      identity.model_id = resolution.runtime_status->active_model_id;
    }
    if (identity.served_model_name.empty()) {
      identity.served_model_name =
          resolution.runtime_status->active_served_model_name;
    }
    identity.cached_local_model_path =
        resolution.runtime_status->cached_local_model_path;
    identity.cached_runtime_model_path =
        resolution.runtime_status->model_path;
    identity.runtime_profile =
        resolution.runtime_status->active_runtime_profile;
  }
  return identity;
}

std::string InteractionModelIdentityBuilder::ReadJsonStringOrEmpty(
    const nlohmann::json& payload,
    std::string_view key) const {
  const auto found = payload.find(std::string(key));
  if (found == payload.end() || found->is_null() || !found->is_string()) {
    return {};
  }
  return found->get<std::string>();
}

}  // namespace naim::controller
