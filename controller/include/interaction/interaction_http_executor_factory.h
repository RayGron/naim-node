#pragma once

#include <functional>
#include <optional>

#include "interaction/interaction_http_support.h"
#include "interaction/interaction_service.h"
#include "interaction/interaction_stream_http_request_preparation_service.h"

namespace naim::controller {

class InteractionHttpExecutorFactory final {
 public:
  using ResolveRequestContextFn =
      std::function<std::optional<InteractionValidationError>(
          const PlaneInteractionResolution&,
          InteractionRequestContext*)>;

  explicit InteractionHttpExecutorFactory(const ::InteractionHttpSupport& support);

  InteractionPlaneResolver MakePlaneResolver() const;
  InteractionSessionExecutor MakeSessionExecutor() const;
  InteractionStreamSegmentExecutor MakeStreamSegmentExecutor() const;
  InteractionProxyExecutor MakeProxyExecutor(
      ResolveRequestContextFn resolve_request_context) const;
  InteractionStreamRequestResolver MakeStreamRequestResolver() const;
  InteractionStreamHttpRequestPreparationService MakeStreamRequestPreparationService(
      ResolveRequestContextFn resolve_request_context) const;
  InteractionStreamSessionExecutor MakeStreamSessionExecutor() const;

 private:
  const ::InteractionHttpSupport& support_;
};

}  // namespace naim::controller
