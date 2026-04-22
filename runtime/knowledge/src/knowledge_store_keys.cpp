#include "knowledge/knowledge_store_keys.h"

#include <iomanip>
#include <sstream>

namespace naim::knowledge_runtime {

std::string KnowledgeStoreKeys::EventKey(int sequence) {
  std::ostringstream stream;
  stream << "events:" << std::setw(20) << std::setfill('0') << sequence;
  return stream.str();
}

}  // namespace naim::knowledge_runtime
