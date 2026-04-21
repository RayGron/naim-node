#pragma once

#include <string>
#include <vector>

#include "runtime/infer_runtime_types.h"

namespace naim::infer::prewarm_support {

struct PrewarmState {
  int ready_upstreams = 0;
  int prewarmed_upstreams = 0;
  std::vector<std::string> ready_base_urls;
  std::vector<std::string> prewarmed_base_urls;
};

std::vector<std::string> ObservedReadyReplicaLeaderBaseUrls(const RuntimeConfig& config);
std::vector<std::string> FilterPrewarmedReplicaBaseUrls(
    const RuntimeConfig& config,
    const std::vector<std::string>& candidate_base_urls);
void ResetPrewarmState(const RuntimeConfig& config);
PrewarmState PrewarmReadyReplicaLeaders(const RuntimeConfig& config);

}  // namespace naim::infer::prewarm_support
