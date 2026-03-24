#include "comet/scheduling_policy.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace comet {

namespace {

constexpr double kFractionEpsilon = 1e-9;

bool NodeSupportsWorkerExecutionMode(const NodeInventory& node) {
  return node.execution_mode != HostExecutionMode::InferOnly;
}

std::string EffectiveWorkerSelectionPolicy(const DesiredState& state) {
  if (!state.worker_group.worker_selection_policy.empty()) {
    return state.worker_group.worker_selection_policy;
  }
  if (!state.inference.worker_selection_policy.empty()) {
    return state.inference.worker_selection_policy;
  }
  return "prefer-free-then-share";
}

std::string JoinStrings(const std::vector<std::string>& values, const std::string& delimiter) {
  std::ostringstream out;
  for (std::size_t index = 0; index < values.size(); ++index) {
    if (index > 0) {
      out << delimiter;
    }
    out << values[index];
  }
  return out.str();
}

struct AllocationAccumulator {
  std::vector<std::string> worker_names;
  std::vector<std::string> exclusive_worker_names;
  std::vector<std::string> shared_worker_names;
  std::vector<std::string> best_effort_worker_names;
  std::vector<std::string> preemptible_worker_names;
  double total_fraction = 0.0;
  double best_effort_fraction = 0.0;
  int highest_priority = std::numeric_limits<int>::min();
  int lowest_priority = std::numeric_limits<int>::max();
  int total_memory_cap_mb = 0;
  int best_effort_memory_cap_mb = 0;
  bool any_memory_cap = false;
  bool missing_memory_cap = false;
  std::optional<int> memory_capacity_mb;
  int highest_guaranteed_priority = std::numeric_limits<int>::min();
  int highest_best_effort_priority = std::numeric_limits<int>::min();
};

struct WorkerAllocationRecord {
  std::string worker_name;
  std::string node_name;
  std::string gpu_device;
  GpuShareMode share_mode = GpuShareMode::Exclusive;
  double gpu_fraction = 0.0;
  int priority = 0;
  bool preemptible = false;
  std::optional<int> memory_cap_mb;
  bool currently_soft_shared = false;
};

struct VictimSelection {
  std::vector<std::string> worker_names;
  double released_fraction = 0.0;
  int released_memory_cap_mb = 0;
  bool satisfies = false;
};

std::string PreemptionReason(const AllocationAccumulator& allocation) {
  if (!allocation.best_effort_worker_names.empty() &&
      (!allocation.exclusive_worker_names.empty() || !allocation.shared_worker_names.empty())) {
    return "protect guaranteed workers before best-effort workers on the same GPU";
  }
  if (allocation.worker_names.size() > 1) {
    return "reduce soft-share pressure by evicting the lowest-priority best-effort workers first";
  }
  return "free capacity for higher-priority or guaranteed work";
}

int ScorePlacementCandidate(
    const WorkerAllocationRecord& worker,
    const GpuCapacitySummary& capacity,
    bool fits_current_fraction,
    bool fits_current_with_preemption,
    bool fits_exclusive_fraction,
    bool fits_exclusive_with_preemption,
    bool fits_memory_cap,
    bool fits_memory_with_preemption,
    bool preemption_required) {
  int score = 0;
  if (capacity.node_name == worker.node_name) {
    score += 40;
  }
  if (capacity.is_idle) {
    score += 60;
  }
  if (fits_current_fraction) {
    score += 20;
  } else if (fits_current_with_preemption) {
    score += 8;
  }
  if (fits_exclusive_fraction) {
    score += 40;
  } else if (fits_exclusive_with_preemption) {
    score += 15;
  }
  if (fits_memory_cap) {
    score += 15;
  } else if (fits_memory_with_preemption) {
    score += 5;
  } else {
    score -= 50;
  }
  if (preemption_required) {
    score -= 20;
  }
  score += static_cast<int>(capacity.free_fraction * 10.0);
  score -= capacity.worker_count * 10;
  return score;
}

int PlacementCandidatePolicyRank(
    const std::string& policy,
    const PlacementCandidate& candidate) {
  if (policy == "prefer-free-then-share") {
    if (candidate.idle && !candidate.preemption_required) {
      return 0;
    }
    if (!candidate.idle && !candidate.preemption_required &&
        (candidate.fits_exclusive_fraction || candidate.fits_current_fraction)) {
      return 1;
    }
    if (candidate.preemption_required) {
      return 2;
    }
    return 3;
  }
  return candidate.preemption_required ? 1 : 0;
}

std::string PlacementAction(
    const WorkerAllocationRecord& worker,
    bool fits_current_with_preemption,
    bool fits_exclusive_fraction,
    bool fits_exclusive_with_preemption,
    bool fits_current_fraction,
    bool fits_memory_with_preemption) {
  if (!fits_memory_with_preemption) {
    return "insufficient-memory";
  }
  if (fits_exclusive_fraction) {
    return worker.currently_soft_shared || worker.gpu_fraction < 1.0 - kFractionEpsilon ||
                   worker.share_mode != GpuShareMode::Exclusive
               ? "upgrade-to-exclusive"
               : "move-equivalent";
  }
  if (fits_exclusive_with_preemption) {
    return "preempt-best-effort-to-exclusive";
  }
  if (fits_current_fraction) {
    return "move-with-current-fraction";
  }
  if (fits_current_with_preemption) {
    return "preempt-best-effort-and-move";
  }
  return "insufficient-fraction";
}

const AllocationAccumulator* FindAllocationAccumulator(
    const std::map<std::pair<std::string, std::string>, AllocationAccumulator>& allocations,
    const std::string& node_name,
    const std::string& gpu_device) {
  const auto it = allocations.find({node_name, gpu_device});
  if (it == allocations.end()) {
    return nullptr;
  }
  return &it->second;
}

std::vector<const WorkerAllocationRecord*> BuildBestEffortVictimOrder(
    const std::vector<WorkerAllocationRecord>& worker_records,
    const std::string& node_name,
    const std::string& gpu_device) {
  std::vector<const WorkerAllocationRecord*> victims;
  for (const auto& worker_record : worker_records) {
    if (worker_record.node_name == node_name &&
        worker_record.gpu_device == gpu_device &&
        worker_record.share_mode == GpuShareMode::BestEffort &&
        worker_record.preemptible) {
      victims.push_back(&worker_record);
    }
  }

  std::sort(
      victims.begin(),
      victims.end(),
      [](const WorkerAllocationRecord* left, const WorkerAllocationRecord* right) {
        if (left->priority != right->priority) {
          return left->priority < right->priority;
        }
        if (std::abs(left->gpu_fraction - right->gpu_fraction) > kFractionEpsilon) {
          return left->gpu_fraction > right->gpu_fraction;
        }
        if (left->memory_cap_mb.value_or(0) != right->memory_cap_mb.value_or(0)) {
          return left->memory_cap_mb.value_or(0) > right->memory_cap_mb.value_or(0);
        }
        return left->worker_name < right->worker_name;
      });
  return victims;
}

VictimSelection SelectMinimalVictims(
    const WorkerAllocationRecord& worker,
    const GpuCapacitySummary& capacity,
    const std::vector<const WorkerAllocationRecord*>& victim_order,
    double required_fraction) {
  VictimSelection selection;
  const double needed_fraction =
      std::max(0.0, required_fraction - capacity.free_fraction);
  int needed_memory_mb = 0;
  if (worker.memory_cap_mb.has_value() && capacity.free_memory_mb.has_value()) {
    needed_memory_mb = std::max(0, *worker.memory_cap_mb - *capacity.free_memory_mb);
  }

  if (needed_fraction <= kFractionEpsilon && needed_memory_mb <= 0) {
    selection.satisfies = true;
    return selection;
  }

  for (const auto* victim : victim_order) {
    selection.worker_names.push_back(victim->worker_name);
    selection.released_fraction += victim->gpu_fraction;
    selection.released_memory_cap_mb += victim->memory_cap_mb.value_or(0);
    if (selection.released_fraction + kFractionEpsilon >= needed_fraction &&
        selection.released_memory_cap_mb >= needed_memory_mb) {
      selection.satisfies = true;
      break;
    }
  }

  return selection;
}

}  // namespace

SchedulingPolicyReport EvaluateSchedulingPolicy(const DesiredState& state) {
  SchedulingPolicyReport report;

  std::map<std::string, NodeInventory> nodes_by_name;
  for (const auto& node : state.nodes) {
    nodes_by_name.emplace(node.name, node);
  }

  std::map<std::pair<std::string, std::string>, AllocationAccumulator> allocations;
  std::vector<WorkerAllocationRecord> worker_records;

  for (const auto& instance : state.instances) {
    if (instance.role != InstanceRole::Worker) {
      continue;
    }

    if (!instance.gpu_device.has_value() || instance.gpu_device->empty()) {
      report.errors.push_back(
          "worker '" + instance.name +
          "' must pin gpu_device explicitly; implicit scheduler defaults are not allowed");
      continue;
    }

    if (instance.priority < 0) {
      report.errors.push_back(
          "worker '" + instance.name + "' has invalid priority=" +
          std::to_string(instance.priority) + "; allowed range is >= 0");
      continue;
    }

    if (instance.gpu_fraction <= 0.0 || instance.gpu_fraction > 1.0 + kFractionEpsilon) {
      std::ostringstream message;
      message << "worker '" << instance.name << "' has invalid gpu_fraction=" << instance.gpu_fraction
              << "; allowed range is (0, 1]";
      report.errors.push_back(message.str());
      continue;
    }

    if (instance.memory_cap_mb.has_value() && *instance.memory_cap_mb <= 0) {
      report.errors.push_back(
          "worker '" + instance.name +
          "' has invalid memory_cap_mb; allowed range is > 0");
      continue;
    }

    if (instance.share_mode == GpuShareMode::Exclusive &&
        std::abs(instance.gpu_fraction - 1.0) > kFractionEpsilon) {
      std::ostringstream message;
      message << "worker '" << instance.name
              << "' uses share_mode=exclusive but gpu_fraction=" << instance.gpu_fraction
              << "; exclusive workers must reserve the whole GPU";
      report.errors.push_back(message.str());
      continue;
    }

    if (instance.share_mode == GpuShareMode::BestEffort && !instance.preemptible) {
      report.errors.push_back(
          "worker '" + instance.name +
          "' uses share_mode=best-effort but preemptible=false; best-effort workers must be preemptible");
      continue;
    }

    const auto node_it = nodes_by_name.find(instance.node_name);
    if (node_it == nodes_by_name.end()) {
      report.errors.push_back(
          "worker '" + instance.name + "' references unknown node '" + instance.node_name + "'");
      continue;
    }

    const auto& node = node_it->second;
    if (!NodeSupportsWorkerExecutionMode(node)) {
      report.errors.push_back(
          "worker '" + instance.name + "' targets node '" + instance.node_name +
          "' which is not worker-capable");
      continue;
    }
    const auto gpu_it = std::find(
        node.gpu_devices.begin(), node.gpu_devices.end(), *instance.gpu_device);
    if (gpu_it == node.gpu_devices.end()) {
      report.errors.push_back(
          "worker '" + instance.name + "' pins missing gpu '" + *instance.gpu_device +
          "' on node '" + instance.node_name + "'");
      continue;
    }

    auto& allocation = allocations[{instance.node_name, *instance.gpu_device}];
    allocation.worker_names.push_back(instance.name);
    switch (instance.share_mode) {
      case GpuShareMode::Exclusive:
        allocation.exclusive_worker_names.push_back(instance.name);
        allocation.highest_guaranteed_priority =
            std::max(allocation.highest_guaranteed_priority, instance.priority);
        break;
      case GpuShareMode::Shared:
        allocation.shared_worker_names.push_back(instance.name);
        allocation.highest_guaranteed_priority =
            std::max(allocation.highest_guaranteed_priority, instance.priority);
        break;
      case GpuShareMode::BestEffort:
        allocation.best_effort_worker_names.push_back(instance.name);
        allocation.best_effort_fraction += instance.gpu_fraction;
        allocation.best_effort_memory_cap_mb += instance.memory_cap_mb.value_or(0);
        allocation.highest_best_effort_priority =
            std::max(allocation.highest_best_effort_priority, instance.priority);
        break;
    }
    if (instance.preemptible) {
      allocation.preemptible_worker_names.push_back(instance.name);
    }
    allocation.total_fraction += instance.gpu_fraction;
    allocation.highest_priority = std::max(allocation.highest_priority, instance.priority);
    allocation.lowest_priority = std::min(allocation.lowest_priority, instance.priority);
    if (instance.memory_cap_mb.has_value()) {
      allocation.any_memory_cap = true;
      allocation.total_memory_cap_mb += *instance.memory_cap_mb;
    } else {
      allocation.missing_memory_cap = true;
    }
    const auto memory_it = node.gpu_memory_mb.find(*instance.gpu_device);
    if (memory_it != node.gpu_memory_mb.end()) {
      allocation.memory_capacity_mb = memory_it->second;
    }

    worker_records.push_back(
        WorkerAllocationRecord{
            instance.name,
            instance.node_name,
            *instance.gpu_device,
            instance.share_mode,
            instance.gpu_fraction,
            instance.priority,
            instance.preemptible,
            instance.memory_cap_mb,
            false,
        });
  }

  for (auto& [key, allocation] : allocations) {
    std::sort(allocation.worker_names.begin(), allocation.worker_names.end());
    std::sort(allocation.exclusive_worker_names.begin(), allocation.exclusive_worker_names.end());
    std::sort(allocation.shared_worker_names.begin(), allocation.shared_worker_names.end());
    std::sort(allocation.best_effort_worker_names.begin(), allocation.best_effort_worker_names.end());
    std::sort(allocation.preemptible_worker_names.begin(), allocation.preemptible_worker_names.end());

    GpuAllocationGroup group;
    group.node_name = key.first;
    group.gpu_device = key.second;
    group.worker_names = allocation.worker_names;
    group.exclusive_worker_names = allocation.exclusive_worker_names;
    group.shared_worker_names = allocation.shared_worker_names;
    group.best_effort_worker_names = allocation.best_effort_worker_names;
    group.preemptible_worker_names = allocation.preemptible_worker_names;
    group.total_fraction = allocation.total_fraction;
    group.remaining_fraction = std::max(0.0, 1.0 - allocation.total_fraction);
    group.highest_priority =
        allocation.highest_priority == std::numeric_limits<int>::min() ? 0 : allocation.highest_priority;
    group.lowest_priority =
        allocation.lowest_priority == std::numeric_limits<int>::max() ? 0 : allocation.lowest_priority;
    group.total_memory_cap_mb = allocation.total_memory_cap_mb;
    group.memory_capacity_mb = allocation.memory_capacity_mb;
    if (allocation.memory_capacity_mb.has_value()) {
      group.remaining_memory_mb =
          std::max(0, *allocation.memory_capacity_mb - allocation.total_memory_cap_mb);
    }
    report.allocations.push_back(group);

    if (!allocation.exclusive_worker_names.empty() && allocation.worker_names.size() > 1) {
      report.errors.push_back(
          "gpu '" + key.second + "' on node '" + key.first +
          "' mixes share_mode=exclusive with other workers: " +
          JoinStrings(allocation.worker_names, ","));
    }

    if (allocation.total_fraction > 1.0 + kFractionEpsilon) {
      std::ostringstream message;
      message << "gpu oversubscription on node '" << key.first << "' gpu '" << key.second
              << "': total requested fraction=" << allocation.total_fraction
              << " by workers " << JoinStrings(allocation.worker_names, ",");
      report.errors.push_back(message.str());
    }

    if (allocation.memory_capacity_mb.has_value() &&
        allocation.total_memory_cap_mb > *allocation.memory_capacity_mb) {
      std::ostringstream message;
      message << "gpu memory oversubscription on node '" << key.first << "' gpu '" << key.second
              << "': total requested memory_cap_mb=" << allocation.total_memory_cap_mb
              << " exceeds capacity_mb=" << *allocation.memory_capacity_mb
              << " by workers " << JoinStrings(allocation.worker_names, ",");
      report.errors.push_back(message.str());
    }

    if (!allocation.memory_capacity_mb.has_value() && allocation.any_memory_cap) {
      report.warnings.push_back(
          "gpu '" + key.second + "' on node '" + key.first +
          "' has memory_cap_mb requests but node inventory does not expose gpu_memory_mb");
    }

    if (allocation.worker_names.size() > 1) {
      std::ostringstream message;
      message << "soft-share group on node '" << key.first << "' gpu '" << key.second
              << "': workers=" << JoinStrings(allocation.worker_names, ",")
              << " total_fraction=" << allocation.total_fraction
              << " shared=" << allocation.shared_worker_names.size()
              << " best_effort=" << allocation.best_effort_worker_names.size()
              << " preemptible=" << allocation.preemptible_worker_names.size();
      if (allocation.memory_capacity_mb.has_value()) {
        message << " memory_cap_mb=" << allocation.total_memory_cap_mb << "/" << *allocation.memory_capacity_mb;
      }
      report.warnings.push_back(message.str());
    }

    if (allocation.missing_memory_cap && allocation.worker_names.size() > 1) {
      report.warnings.push_back(
          "soft-share group on node '" + key.first + "' gpu '" + key.second +
          "' is missing memory_cap_mb on one or more workers; memory-aware admission will be incomplete");
    }

    if (!allocation.best_effort_worker_names.empty() &&
        allocation.highest_guaranteed_priority != std::numeric_limits<int>::min() &&
        allocation.highest_best_effort_priority >= allocation.highest_guaranteed_priority) {
      report.warnings.push_back(
          "best-effort workers on node '" + key.first + "' gpu '" + key.second +
          "' do not have lower priority than the rest of the group");
    }

    if (!allocation.best_effort_worker_names.empty()) {
      const auto best_effort_records =
          BuildBestEffortVictimOrder(worker_records, key.first, key.second);

      PreemptionHint hint;
      hint.node_name = key.first;
      hint.gpu_device = key.second;
      for (const auto* worker_record : best_effort_records) {
        hint.victim_worker_names.push_back(worker_record->worker_name);
      }
      hint.reason = PreemptionReason(allocation);
      report.preemption_hints.push_back(std::move(hint));
    }

    if (allocation.worker_names.size() > 1) {
      for (auto& worker_record : worker_records) {
        if (worker_record.node_name == key.first && worker_record.gpu_device == key.second) {
          worker_record.currently_soft_shared = true;
        }
      }
    }
  }

  bool has_soft_share_groups = false;
  std::vector<std::string> idle_gpus;
  for (const auto& node : state.nodes) {
    if (!NodeSupportsWorkerExecutionMode(node)) {
      continue;
    }
    for (const auto& gpu_device : node.gpu_devices) {
      GpuCapacitySummary summary;
      summary.node_name = node.name;
      summary.gpu_device = gpu_device;
      const auto* allocation = FindAllocationAccumulator(allocations, node.name, gpu_device);
      if (allocation != nullptr) {
        summary.worker_count = static_cast<int>(allocation->worker_names.size());
        summary.guaranteed_worker_count =
            static_cast<int>(
                allocation->exclusive_worker_names.size() + allocation->shared_worker_names.size());
        summary.best_effort_worker_count =
            static_cast<int>(allocation->best_effort_worker_names.size());
        summary.allocated_fraction = allocation->total_fraction;
        summary.free_fraction = std::max(0.0, 1.0 - allocation->total_fraction);
        summary.releasable_best_effort_fraction = allocation->best_effort_fraction;
        summary.allocated_memory_cap_mb = allocation->total_memory_cap_mb;
        summary.releasable_best_effort_memory_cap_mb = allocation->best_effort_memory_cap_mb;
        summary.preemptible_best_effort_workers = allocation->best_effort_worker_names;
        summary.has_soft_share = allocation->worker_names.size() > 1;
        summary.is_idle = allocation->worker_names.empty();
        has_soft_share_groups = has_soft_share_groups || summary.has_soft_share;
      }
      const auto memory_it = node.gpu_memory_mb.find(gpu_device);
      if (memory_it != node.gpu_memory_mb.end()) {
        summary.memory_capacity_mb = memory_it->second;
        summary.free_memory_mb =
            std::max(0, memory_it->second - summary.allocated_memory_cap_mb);
      }
      if (summary.is_idle) {
        idle_gpus.push_back(node.name + ":" + gpu_device);
      }
      report.capacities.push_back(std::move(summary));
    }
  }

  std::sort(
      report.capacities.begin(),
      report.capacities.end(),
      [](const GpuCapacitySummary& left, const GpuCapacitySummary& right) {
        if (left.node_name != right.node_name) {
          return left.node_name < right.node_name;
        }
        return left.gpu_device < right.gpu_device;
      });

  if (has_soft_share_groups && !idle_gpus.empty()) {
    report.warnings.push_back(
        "idle gpu capacity exists while soft-share groups are active; consider rebalancing to " +
        JoinStrings(idle_gpus, ","));
  }

  for (const auto& worker : worker_records) {
    const bool needs_rebalance_hint =
        worker.currently_soft_shared ||
        worker.gpu_fraction < 1.0 - kFractionEpsilon ||
        worker.share_mode != GpuShareMode::Exclusive;
    if (!needs_rebalance_hint) {
      continue;
    }

    std::optional<GpuCapacitySummary> preferred_candidate;
    std::optional<GpuCapacitySummary> fallback_candidate;
    std::optional<GpuCapacitySummary> preferred_preemption_candidate;
    std::optional<GpuCapacitySummary> fallback_preemption_candidate;
    for (const auto& capacity : report.capacities) {
      if (capacity.node_name == worker.node_name && capacity.gpu_device == worker.gpu_device) {
        continue;
      }
      const bool fits_memory_after_preemption =
          !worker.memory_cap_mb.has_value() ||
          !capacity.memory_capacity_mb.has_value() ||
          !capacity.free_memory_mb.has_value() ||
          *worker.memory_cap_mb <=
              (*capacity.free_memory_mb + capacity.releasable_best_effort_memory_cap_mb);
      if (!fits_memory_after_preemption) {
        continue;
      }

      if (capacity.is_idle) {
        if (worker.node_name == capacity.node_name) {
          preferred_candidate = capacity;
          break;
        }
        if (!fallback_candidate.has_value()) {
          fallback_candidate = capacity;
        }
        continue;
      }

      if (capacity.best_effort_worker_count == 0 ||
          capacity.guaranteed_worker_count != 0 ||
          capacity.preemptible_best_effort_workers.empty()) {
        continue;
      }

      const bool fits_exclusive_after_preemption =
          capacity.free_fraction + capacity.releasable_best_effort_fraction + kFractionEpsilon >= 1.0;
      const bool fits_current_after_preemption =
          capacity.free_fraction + capacity.releasable_best_effort_fraction + kFractionEpsilon >=
          worker.gpu_fraction;
      if (!fits_exclusive_after_preemption && !fits_current_after_preemption) {
        continue;
      }

      if (worker.node_name == capacity.node_name) {
        preferred_preemption_candidate = capacity;
      } else if (!fallback_preemption_candidate.has_value()) {
        fallback_preemption_candidate = capacity;
      }
    }

    const auto candidate =
        preferred_candidate.has_value()
            ? preferred_candidate
            : (fallback_candidate.has_value()
                   ? fallback_candidate
                   : (preferred_preemption_candidate.has_value()
                          ? preferred_preemption_candidate
                          : fallback_preemption_candidate));
    if (!candidate.has_value()) {
      continue;
    }

    std::ostringstream hint;
    hint << "worker '" << worker.worker_name << "' can be rebalanced from "
         << worker.node_name << ":" << worker.gpu_device;
    if (candidate->is_idle) {
      hint << " to idle gpu " << candidate->node_name << ":" << candidate->gpu_device
           << " and upgraded to share_mode=exclusive fraction=1";
      if (worker.currently_soft_shared) {
        hint << " to dissolve a soft-share group";
      } else if (worker.gpu_fraction < 1.0 - kFractionEpsilon) {
        hint << " to avoid partial GPU reservation";
      }
    } else {
      hint << " to gpu " << candidate->node_name << ":" << candidate->gpu_device
           << " after preempting best-effort workers "
           << JoinStrings(candidate->preemptible_best_effort_workers, ",");
    }
    if (worker.memory_cap_mb.has_value() && candidate->memory_capacity_mb.has_value()) {
      hint << " (memory_cap_mb=" << *worker.memory_cap_mb
           << ", target_capacity_mb=" << *candidate->memory_capacity_mb << ")";
    }
    report.rebalance_hints.push_back(hint.str());
  }

  for (const auto& worker : worker_records) {
    const bool needs_recommendation =
        worker.currently_soft_shared ||
        worker.gpu_fraction < 1.0 - kFractionEpsilon ||
        worker.share_mode != GpuShareMode::Exclusive;
    if (!needs_recommendation) {
      continue;
    }

    WorkerPlacementRecommendation recommendation;
    recommendation.worker_name = worker.worker_name;
    recommendation.current_node_name = worker.node_name;
    recommendation.current_gpu_device = worker.gpu_device;
    recommendation.current_share_mode = worker.share_mode;
    recommendation.current_fraction = worker.gpu_fraction;
    recommendation.current_memory_cap_mb = worker.memory_cap_mb;

    const std::string selection_policy = EffectiveWorkerSelectionPolicy(state);
    for (const auto& capacity : report.capacities) {
      if (capacity.node_name == worker.node_name &&
          capacity.gpu_device == worker.gpu_device) {
        continue;
      }

      const auto victim_order =
          BuildBestEffortVictimOrder(worker_records, capacity.node_name, capacity.gpu_device);
      const bool fits_current_fraction =
          capacity.free_fraction + kFractionEpsilon >= worker.gpu_fraction;
      const auto current_selection =
          SelectMinimalVictims(worker, capacity, victim_order, worker.gpu_fraction);
      const bool fits_current_with_preemption = current_selection.satisfies;
      const bool fits_exclusive_fraction =
          capacity.is_idle && capacity.free_fraction + kFractionEpsilon >= 1.0;
      const auto exclusive_selection =
          SelectMinimalVictims(worker, capacity, victim_order, 1.0);
      const bool fits_exclusive_with_preemption =
          capacity.guaranteed_worker_count == 0 &&
          exclusive_selection.satisfies;
      const bool fits_memory_cap =
          !worker.memory_cap_mb.has_value() || !capacity.free_memory_mb.has_value() ||
          *capacity.free_memory_mb >= *worker.memory_cap_mb;
      const bool fits_memory_with_preemption =
          fits_current_with_preemption || fits_exclusive_with_preemption;
      const bool preemption_required =
          !capacity.is_idle &&
          ((fits_exclusive_with_preemption && !fits_exclusive_fraction) ||
           (fits_current_with_preemption && !fits_current_fraction));

      PlacementCandidate candidate;
      candidate.node_name = capacity.node_name;
      candidate.gpu_device = capacity.gpu_device;
      candidate.same_node = capacity.node_name == worker.node_name;
      candidate.idle = capacity.is_idle;
      candidate.fits_current_fraction = fits_current_fraction;
      candidate.fits_current_with_preemption = fits_current_with_preemption;
      candidate.fits_exclusive_fraction = fits_exclusive_fraction;
      candidate.fits_exclusive_with_preemption = fits_exclusive_with_preemption;
      candidate.fits_memory_cap = fits_memory_cap;
      candidate.fits_memory_with_preemption = fits_memory_with_preemption;
      candidate.preemption_required = preemption_required;
      candidate.free_fraction = capacity.free_fraction;
      candidate.free_memory_mb = capacity.free_memory_mb;
      candidate.preemption_victims =
          fits_exclusive_with_preemption && !fits_exclusive_fraction
              ? exclusive_selection.worker_names
              : current_selection.worker_names;
      candidate.action =
          PlacementAction(
              worker,
              fits_current_with_preemption,
              fits_exclusive_fraction,
              fits_exclusive_with_preemption,
              fits_current_fraction,
              fits_memory_with_preemption);
      candidate.score =
          ScorePlacementCandidate(
              worker,
              capacity,
              fits_current_fraction,
              fits_current_with_preemption,
              fits_exclusive_fraction,
              fits_exclusive_with_preemption,
              fits_memory_cap,
              fits_memory_with_preemption,
              preemption_required);
      candidate.score -= static_cast<int>(candidate.preemption_victims.size()) * 5;
      recommendation.candidates.push_back(std::move(candidate));
    }

    std::sort(
        recommendation.candidates.begin(),
        recommendation.candidates.end(),
        [&](const PlacementCandidate& left, const PlacementCandidate& right) {
          const int left_rank = PlacementCandidatePolicyRank(selection_policy, left);
          const int right_rank = PlacementCandidatePolicyRank(selection_policy, right);
          if (left_rank != right_rank) {
            return left_rank < right_rank;
          }
          if (left.score != right.score) {
            return left.score > right.score;
          }
          if (left.same_node != right.same_node) {
            return left.same_node;
          }
          if (left.preemption_required != right.preemption_required) {
            return !left.preemption_required;
          }
          if (left.idle != right.idle) {
            return left.idle;
          }
          if (std::abs(left.free_fraction - right.free_fraction) > kFractionEpsilon) {
            return left.free_fraction > right.free_fraction;
          }
          if (left.node_name != right.node_name) {
            return left.node_name < right.node_name;
          }
          return left.gpu_device < right.gpu_device;
        });

    if (!recommendation.candidates.empty()) {
      report.placement_recommendations.push_back(std::move(recommendation));
    }
  }

  std::sort(
      report.placement_recommendations.begin(),
      report.placement_recommendations.end(),
      [](const WorkerPlacementRecommendation& left, const WorkerPlacementRecommendation& right) {
        return left.worker_name < right.worker_name;
      });

  int rollout_step = 1;
  for (const auto& recommendation : report.placement_recommendations) {
    if (recommendation.candidates.empty()) {
      continue;
    }
    const auto& candidate = recommendation.candidates.front();
    if (!candidate.preemption_required || candidate.preemption_victims.empty()) {
      continue;
    }

    SchedulerRolloutAction evict_action;
    evict_action.step = rollout_step++;
    evict_action.worker_name = recommendation.worker_name;
    evict_action.action = "evict-best-effort";
    evict_action.target_node_name = candidate.node_name;
    evict_action.target_gpu_device = candidate.gpu_device;
    evict_action.victim_worker_names = candidate.preemption_victims;
    evict_action.reason =
        "free target capacity before retrying placement for worker '" + recommendation.worker_name + "'";
    report.rollout_actions.push_back(std::move(evict_action));

    SchedulerRolloutAction retry_action;
    retry_action.step = rollout_step++;
    retry_action.worker_name = recommendation.worker_name;
    retry_action.action = "retry-placement";
    retry_action.target_node_name = candidate.node_name;
    retry_action.target_gpu_device = candidate.gpu_device;
    retry_action.reason = "apply deferred scheduler decision after eviction completes";
    report.rollout_actions.push_back(std::move(retry_action));
  }

  std::sort(
      report.preemption_hints.begin(),
      report.preemption_hints.end(),
      [](const PreemptionHint& left, const PreemptionHint& right) {
        if (left.node_name != right.node_name) {
          return left.node_name < right.node_name;
        }
        return left.gpu_device < right.gpu_device;
      });

  std::sort(
      report.allocations.begin(),
      report.allocations.end(),
      [](const GpuAllocationGroup& left, const GpuAllocationGroup& right) {
        if (left.node_name != right.node_name) {
          return left.node_name < right.node_name;
        }
        return left.gpu_device < right.gpu_device;
      });

  return report;
}

std::string RenderSchedulingPolicyReport(const SchedulingPolicyReport& report) {
  std::ostringstream out;
  out << "scheduling-policy:\n";
  if (report.allocations.empty()) {
    out << "  allocations=(empty)\n";
  } else {
    for (const auto& allocation : report.allocations) {
      out << "  - node=" << allocation.node_name
          << " gpu=" << allocation.gpu_device
          << " workers=" << JoinStrings(allocation.worker_names, ",")
          << " exclusive=" << JoinStrings(allocation.exclusive_worker_names, ",")
          << " shared=" << JoinStrings(allocation.shared_worker_names, ",")
          << " best_effort=" << JoinStrings(allocation.best_effort_worker_names, ",")
          << " total_fraction=" << allocation.total_fraction
          << " remaining_fraction=" << allocation.remaining_fraction
          << " priority_range=" << allocation.lowest_priority << ".." << allocation.highest_priority;
      if (allocation.memory_capacity_mb.has_value()) {
        out << " memory_cap_mb=" << allocation.total_memory_cap_mb << "/" << *allocation.memory_capacity_mb;
      } else if (allocation.total_memory_cap_mb > 0) {
        out << " memory_cap_mb=" << allocation.total_memory_cap_mb << "/unknown";
      }
      out << "\n";
    }
  }

  out << "  capacity:\n";
  if (report.capacities.empty()) {
    out << "    (empty)\n";
  } else {
    for (const auto& capacity : report.capacities) {
      out << "    - node=" << capacity.node_name
          << " gpu=" << capacity.gpu_device
          << " workers=" << capacity.worker_count
          << " guaranteed_workers=" << capacity.guaranteed_worker_count
          << " best_effort_workers=" << capacity.best_effort_worker_count
          << " allocated_fraction=" << capacity.allocated_fraction
          << " free_fraction=" << capacity.free_fraction
          << " releasable_best_effort_fraction=" << capacity.releasable_best_effort_fraction
          << " idle=" << (capacity.is_idle ? "yes" : "no")
          << " soft_share=" << (capacity.has_soft_share ? "yes" : "no");
      if (capacity.memory_capacity_mb.has_value()) {
        out << " memory_cap_mb=" << capacity.allocated_memory_cap_mb
            << "/" << *capacity.memory_capacity_mb;
        if (capacity.releasable_best_effort_memory_cap_mb > 0) {
          out << " releasable_best_effort_memory_cap_mb="
              << capacity.releasable_best_effort_memory_cap_mb;
        }
      }
      if (!capacity.preemptible_best_effort_workers.empty()) {
        out << " preemptible_best_effort="
            << JoinStrings(capacity.preemptible_best_effort_workers, ",");
      }
      out << "\n";
    }
  }

  if (!report.warnings.empty()) {
    out << "  warnings:\n";
    for (const auto& warning : report.warnings) {
      out << "    - " << warning << "\n";
    }
  }

  if (!report.rebalance_hints.empty()) {
    out << "  rebalance-hints:\n";
    for (const auto& hint : report.rebalance_hints) {
      out << "    - " << hint << "\n";
    }
  }

  if (!report.preemption_hints.empty()) {
    out << "  preemption-hints:\n";
    for (const auto& hint : report.preemption_hints) {
      out << "    - node=" << hint.node_name
          << " gpu=" << hint.gpu_device
          << " victims=" << JoinStrings(hint.victim_worker_names, ",")
          << " reason=" << hint.reason << "\n";
    }
  }

  if (!report.rollout_actions.empty()) {
    out << "  rollout-actions:\n";
    for (const auto& action : report.rollout_actions) {
      out << "    - step=" << action.step
          << " worker=" << action.worker_name
          << " action=" << action.action
          << " target=" << action.target_node_name << ":" << action.target_gpu_device;
      if (!action.victim_worker_names.empty()) {
        out << " victims=" << JoinStrings(action.victim_worker_names, ",");
      }
      if (!action.reason.empty()) {
        out << " reason=" << action.reason;
      }
      out << "\n";
    }
  }

  if (!report.placement_recommendations.empty()) {
    out << "  placement-recommendations:\n";
    for (const auto& recommendation : report.placement_recommendations) {
      out << "    - worker=" << recommendation.worker_name
          << " current=" << recommendation.current_node_name << ":" << recommendation.current_gpu_device
          << " share_mode=" << ToString(recommendation.current_share_mode)
          << " fraction=" << recommendation.current_fraction;
      if (recommendation.current_memory_cap_mb.has_value()) {
        out << " memory_cap_mb=" << *recommendation.current_memory_cap_mb;
      }
      out << "\n";
      for (const auto& candidate : recommendation.candidates) {
        out << "      candidate node=" << candidate.node_name
            << " gpu=" << candidate.gpu_device
            << " score=" << candidate.score
            << " action=" << candidate.action
            << " same_node=" << (candidate.same_node ? "yes" : "no")
            << " idle=" << (candidate.idle ? "yes" : "no")
            << " fits_current=" << (candidate.fits_current_fraction ? "yes" : "no")
            << " fits_current_after_preemption="
            << (candidate.fits_current_with_preemption ? "yes" : "no")
            << " fits_exclusive=" << (candidate.fits_exclusive_fraction ? "yes" : "no")
            << " fits_exclusive_after_preemption="
            << (candidate.fits_exclusive_with_preemption ? "yes" : "no")
            << " fits_memory=" << (candidate.fits_memory_cap ? "yes" : "no")
            << " fits_memory_after_preemption="
            << (candidate.fits_memory_with_preemption ? "yes" : "no")
            << " preemption_required=" << (candidate.preemption_required ? "yes" : "no")
            << " free_fraction=" << candidate.free_fraction;
        if (candidate.free_memory_mb.has_value()) {
          out << " free_memory_mb=" << *candidate.free_memory_mb;
        }
        if (!candidate.preemption_victims.empty()) {
          out << " victims=" << JoinStrings(candidate.preemption_victims, ",");
        }
        out << "\n";
      }
    }
  }

  if (!report.errors.empty()) {
    out << "  errors:\n";
    for (const auto& error : report.errors) {
      out << "    - " << error << "\n";
    }
  }

  return out.str();
}

void RequireSchedulingPolicy(const DesiredState& state) {
  const SchedulingPolicyReport report = EvaluateSchedulingPolicy(state);
  if (report.errors.empty()) {
    return;
  }

  std::ostringstream message;
  message << "desired state violates scheduling policy:";
  for (const auto& error : report.errors) {
    message << "\n- " << error;
  }
  throw std::runtime_error(message.str());
}

}  // namespace comet
