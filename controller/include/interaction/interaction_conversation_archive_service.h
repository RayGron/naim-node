#pragma once

#include <atomic>
#include <filesystem>
#include <optional>
#include <string>

#include "interaction/interaction_service.h"

namespace naim {
class ControllerStore;
}

namespace naim::controller {

class InteractionConversationArchiveService final {
 public:
  std::optional<InteractionValidationError> RestoreArchivedSession(
      naim::ControllerStore& store,
      const std::string& plane_name,
      const std::string& session_id,
      const std::string& owner_kind,
      const std::optional<int>& owner_user_id) const;

  void MaybeArchiveInactiveSessions(const std::string& db_path) const;

 private:
  static std::atomic<long long> last_archive_sweep_epoch_ms_;

  std::string UtcNowSqlTimestamp() const;

  std::string SqlTimestampBeforeSeconds(int seconds) const;

  std::filesystem::path ArchiveRootPath(const std::string& db_path) const;

  std::string CompressStringZstd(const std::string& input) const;

  std::string DecompressStringZstd(const std::string& input) const;

  std::string ReadBinaryFile(const std::filesystem::path& path) const;

  void WriteBinaryFile(
      const std::filesystem::path& path,
      const std::string& contents) const;
};

}  // namespace naim::controller
