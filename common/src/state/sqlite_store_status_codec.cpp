#include "comet/state/sqlite_store.h"

#include <stdexcept>
#include <string>

namespace comet {

std::string ToString(HostAssignmentStatus status) {
  switch (status) {
    case HostAssignmentStatus::Pending:
      return "pending";
    case HostAssignmentStatus::Claimed:
      return "claimed";
    case HostAssignmentStatus::Applied:
      return "applied";
    case HostAssignmentStatus::Failed:
      return "failed";
    case HostAssignmentStatus::Superseded:
      return "superseded";
  }
  return "unknown";
}

HostAssignmentStatus ParseHostAssignmentStatus(const std::string& value) {
  if (value == "pending") {
    return HostAssignmentStatus::Pending;
  }
  if (value == "claimed") {
    return HostAssignmentStatus::Claimed;
  }
  if (value == "applied") {
    return HostAssignmentStatus::Applied;
  }
  if (value == "failed") {
    return HostAssignmentStatus::Failed;
  }
  if (value == "superseded") {
    return HostAssignmentStatus::Superseded;
  }
  throw std::runtime_error("unknown host assignment status '" + value + "'");
}

std::string ToString(HostObservationStatus status) {
  switch (status) {
    case HostObservationStatus::Idle:
      return "idle";
    case HostObservationStatus::Applying:
      return "applying";
    case HostObservationStatus::Applied:
      return "applied";
    case HostObservationStatus::Failed:
      return "failed";
  }
  return "unknown";
}

HostObservationStatus ParseHostObservationStatus(const std::string& value) {
  if (value == "idle") {
    return HostObservationStatus::Idle;
  }
  if (value == "applying") {
    return HostObservationStatus::Applying;
  }
  if (value == "applied") {
    return HostObservationStatus::Applied;
  }
  if (value == "failed") {
    return HostObservationStatus::Failed;
  }
  throw std::runtime_error("unknown host observation status '" + value + "'");
}

std::string ToString(NodeAvailability availability) {
  switch (availability) {
    case NodeAvailability::Active:
      return "active";
    case NodeAvailability::Draining:
      return "draining";
    case NodeAvailability::Unavailable:
      return "unavailable";
  }
  return "unknown";
}

NodeAvailability ParseNodeAvailability(const std::string& value) {
  if (value == "active") {
    return NodeAvailability::Active;
  }
  if (value == "draining") {
    return NodeAvailability::Draining;
  }
  if (value == "unavailable") {
    return NodeAvailability::Unavailable;
  }
  throw std::runtime_error("unknown node availability '" + value + "'");
}

std::string ToString(RolloutActionStatus status) {
  switch (status) {
    case RolloutActionStatus::Pending:
      return "pending";
    case RolloutActionStatus::Acknowledged:
      return "acknowledged";
    case RolloutActionStatus::ReadyToRetry:
      return "ready-to-retry";
  }
  throw std::runtime_error("unknown rollout action status");
}

RolloutActionStatus ParseRolloutActionStatus(const std::string& value) {
  if (value == "pending") {
    return RolloutActionStatus::Pending;
  }
  if (value == "acknowledged") {
    return RolloutActionStatus::Acknowledged;
  }
  if (value == "ready-to-retry") {
    return RolloutActionStatus::ReadyToRetry;
  }
  throw std::runtime_error("unknown rollout action status '" + value + "'");
}

}  // namespace comet
