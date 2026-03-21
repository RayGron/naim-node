#include "comet/reconcile.h"

#include <algorithm>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <utility>

namespace comet {

namespace {

std::string JoinStrings(const std::vector<std::string>& values, const std::string& separator) {
  std::ostringstream out;
  for (std::size_t index = 0; index < values.size(); ++index) {
    if (index > 0) {
      out << separator;
    }
    out << values[index];
  }
  return out.str();
}

std::string OptionalValue(const std::optional<std::string>& value) {
  return value.value_or("-");
}

std::string NodeSignature(const NodeInventory& node) {
  std::vector<std::string> gpu_memory_entries;
  for (const auto& gpu_device : node.gpu_devices) {
    const auto it = node.gpu_memory_mb.find(gpu_device);
    gpu_memory_entries.push_back(
        gpu_device + ":" + std::to_string(it == node.gpu_memory_mb.end() ? 0 : it->second));
  }
  return node.platform + "|" + JoinStrings(node.gpu_devices, ",") + "|" +
         JoinStrings(gpu_memory_entries, ",");
}

std::string DiskKey(const DiskSpec& disk) {
  return disk.name + "@" + disk.node_name;
}

std::string DiskSignature(const DiskSpec& disk) {
  return disk.plane_name + "|" + disk.owner_name + "|" + ToString(disk.kind) + "|" +
         disk.host_path + "|" + disk.container_path + "|" + std::to_string(disk.size_gb);
}

std::vector<std::string> SortedKeys(const std::map<std::string, std::string>& values) {
  std::vector<std::string> keys;
  keys.reserve(values.size());
  for (const auto& [key, _] : values) {
    keys.push_back(key);
  }
  std::sort(keys.begin(), keys.end());
  return keys;
}

std::string MapSignature(const std::map<std::string, std::string>& values) {
  std::vector<std::string> entries;
  for (const auto& key : SortedKeys(values)) {
    entries.push_back(key + "=" + values.at(key));
  }
  return JoinStrings(entries, ";");
}

std::string InstanceSignature(const InstanceSpec& instance) {
  std::vector<std::string> dependencies = instance.depends_on;
  std::sort(dependencies.begin(), dependencies.end());

  return ToString(instance.role) + "|" + instance.plane_name + "|" + instance.node_name + "|" +
         instance.image + "|" + instance.command + "|" + instance.private_disk_name + "|" +
         instance.shared_disk_name + "|" + OptionalValue(instance.gpu_device) + "|" +
         ToString(instance.placement_mode) + "|" +
         ToString(instance.share_mode) + "|" + std::to_string(instance.priority) + "|" +
         std::to_string(instance.preemptible ? 1 : 0) + "|" +
         std::to_string(instance.memory_cap_mb.value_or(0)) + "|" +
         std::to_string(instance.gpu_fraction) + "|" +
         std::to_string(instance.private_disk_size_gb) + "|" +
         JoinStrings(dependencies, ",") + "|" + MapSignature(instance.environment) + "|" +
         MapSignature(instance.labels);
}

std::string PlaneSignature(const DesiredState& state) {
  return state.plane_name + "|" + state.plane_shared_disk_name;
}

template <typename T, typename KeyFn, typename SignatureFn>
void AppendResourceChanges(
    const std::vector<T>& current_items,
    const std::vector<T>& desired_items,
    const std::string& resource_type,
    KeyFn key_fn,
    SignatureFn signature_fn,
    std::vector<ResourceChange>* changes) {
  std::map<std::string, std::string> current_by_key;
  for (const auto& item : current_items) {
    current_by_key[key_fn(item)] = signature_fn(item);
  }

  std::map<std::string, std::string> desired_by_key;
  for (const auto& item : desired_items) {
    desired_by_key[key_fn(item)] = signature_fn(item);
  }

  std::set<std::string> all_keys;
  for (const auto& [key, _] : current_by_key) {
    all_keys.insert(key);
  }
  for (const auto& [key, _] : desired_by_key) {
    all_keys.insert(key);
  }

  for (const auto& key : all_keys) {
    const auto current_it = current_by_key.find(key);
    const auto desired_it = desired_by_key.find(key);

    if (current_it == current_by_key.end()) {
      changes->push_back(ResourceChange{ChangeAction::Create, resource_type, key, "new resource"});
      continue;
    }

    if (desired_it == desired_by_key.end()) {
      changes->push_back(
          ResourceChange{ChangeAction::Delete, resource_type, key, "missing from desired bundle"});
      continue;
    }

    if (current_it->second != desired_it->second) {
      changes->push_back(
          ResourceChange{ChangeAction::Update, resource_type, key, "resource configuration changed"});
    }
  }
}

void AppendDesiredStateWarnings(const DesiredState& state, ReconcilePlan* plan) {
  for (const auto& instance : state.instances) {
    if (instance.role == InstanceRole::Worker && instance.gpu_fraction < 1.0) {
      plan->warnings.push_back(
          "worker '" + instance.name + "' requests gpu_fraction=" +
          std::to_string(instance.gpu_fraction) + ", so soft GPU sharing will be required");
    }
    if (instance.role == InstanceRole::Worker && !instance.memory_cap_mb.has_value()) {
      plan->warnings.push_back(
          "worker '" + instance.name +
          "' does not set memory_cap_mb, so memory-aware GPU admission cannot be enforced");
    }
  }
}

}  // namespace

ReconcilePlan BuildReconcilePlan(
    const std::optional<DesiredState>& current_state,
    const DesiredState& desired_state) {
  ReconcilePlan plan;

  if (!current_state.has_value()) {
    plan.notes.push_back("controller database is empty; all desired resources will be created");
  } else if (current_state->plane_name != desired_state.plane_name) {
    plan.warnings.push_back(
        "bundle plane '" + desired_state.plane_name + "' differs from current plane '" +
        current_state->plane_name + "'");
  }

  AppendDesiredStateWarnings(desired_state, &plan);

  if (!current_state.has_value()) {
    plan.changes.push_back(
        ResourceChange{ChangeAction::Create, "plane", desired_state.plane_name, "new plane"});
    AppendResourceChanges(
        {},
        desired_state.nodes,
        "node",
        [](const NodeInventory& node) { return node.name; },
        [](const NodeInventory& node) { return NodeSignature(node); },
        &plan.changes);
    AppendResourceChanges(
        {},
        desired_state.disks,
        "disk",
        [](const DiskSpec& disk) { return DiskKey(disk); },
        [](const DiskSpec& disk) { return DiskSignature(disk); },
        &plan.changes);
    AppendResourceChanges(
        {},
        desired_state.instances,
        "instance",
        [](const InstanceSpec& instance) { return instance.name; },
        [](const InstanceSpec& instance) { return InstanceSignature(instance); },
        &plan.changes);
    return plan;
  }

  if (PlaneSignature(*current_state) != PlaneSignature(desired_state)) {
    plan.changes.push_back(
        ResourceChange{ChangeAction::Update, "plane", desired_state.plane_name, "plane metadata changed"});
  }

  AppendResourceChanges(
      current_state->nodes,
      desired_state.nodes,
      "node",
      [](const NodeInventory& node) { return node.name; },
      [](const NodeInventory& node) { return NodeSignature(node); },
      &plan.changes);
  AppendResourceChanges(
      current_state->disks,
      desired_state.disks,
      "disk",
      [](const DiskSpec& disk) { return DiskKey(disk); },
      [](const DiskSpec& disk) { return DiskSignature(disk); },
      &plan.changes);
  AppendResourceChanges(
      current_state->instances,
      desired_state.instances,
      "instance",
      [](const InstanceSpec& instance) { return instance.name; },
      [](const InstanceSpec& instance) { return InstanceSignature(instance); },
      &plan.changes);

  if (plan.changes.empty()) {
    plan.notes.push_back("current state already matches desired bundle");
  }

  return plan;
}

std::string RenderReconcilePlan(const ReconcilePlan& plan) {
  std::ostringstream out;

  if (!plan.notes.empty()) {
    out << "notes:\n";
    for (const auto& note : plan.notes) {
      out << "  - " << note << "\n";
    }
  }

  if (!plan.warnings.empty()) {
    out << "warnings:\n";
    for (const auto& warning : plan.warnings) {
      out << "  - " << warning << "\n";
    }
  }

  std::size_t creates = 0;
  std::size_t updates = 0;
  std::size_t deletes = 0;
  for (const auto& change : plan.changes) {
    switch (change.action) {
      case ChangeAction::Create:
        ++creates;
        break;
      case ChangeAction::Update:
        ++updates;
        break;
      case ChangeAction::Delete:
        ++deletes;
        break;
    }
  }

  out << "changes:\n";
  out << "  create=" << creates << "\n";
  out << "  update=" << updates << "\n";
  out << "  delete=" << deletes << "\n";

  for (const auto& change : plan.changes) {
    out << "  - [" << ToString(change.action) << "] " << change.resource_type
        << " " << change.resource_id;
    if (!change.details.empty()) {
      out << " :: " << change.details;
    }
    out << "\n";
  }

  return out.str();
}

std::string ToString(ChangeAction action) {
  switch (action) {
    case ChangeAction::Create:
      return "create";
    case ChangeAction::Update:
      return "update";
    case ChangeAction::Delete:
      return "delete";
  }
  return "unknown";
}

}  // namespace comet
