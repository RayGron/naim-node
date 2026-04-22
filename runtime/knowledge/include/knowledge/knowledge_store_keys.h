#pragma once

#include <string>
#include <string_view>

namespace naim::knowledge_runtime {

class KnowledgeStoreKeys final {
 public:
  static constexpr std::string_view kDefaultShardId = "kv_default";

  static std::string EventKey(int sequence);
};

}  // namespace naim::knowledge_runtime
