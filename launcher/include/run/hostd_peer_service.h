#pragma once

#include <atomic>
#include <ctime>
#include <filesystem>
#include <map>
#include <mutex>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "config/launcher_options.h"

namespace naim::hostd {
class HttpHostdBackend;
}

namespace naim::launcher {

class HostdPeerService final {
 public:
  explicit HostdPeerService(HostdRunOptions options);
  ~HostdPeerService();

  HostdPeerService(const HostdPeerService&) = delete;
  HostdPeerService& operator=(const HostdPeerService&) = delete;

  bool enabled() const { return enabled_; }
  void Start();
  void Stop();

 private:
  struct PeerRecord {
    std::string peer_node_name;
    std::string peer_endpoint;
    std::string local_interface;
    std::string remote_address;
    bool seen_udp = false;
    bool tcp_reachable = false;
    int rtt_ms = 0;
    std::string last_seen_at;
    std::string last_probe_at;
  };
  struct HttpPeerRequest {
    std::string method;
    std::string path;
    std::map<std::string, std::string> headers;
    std::string body;
  };
  struct CachedTransferTicket {
    std::vector<std::string> source_paths;
    std::uintmax_t max_chunk_bytes = 0;
    std::time_t expires_at_epoch = 0;
  };

  void BeaconLoop();
  void ListenLoop();
  void HttpLoop();
  void RecordPeer(PeerRecord peer);
  void WritePeerState();
  bool ProbePeerHealth(const std::string& endpoint, int* rtt_ms) const;
  std::string BuildBeaconPayload() const;
  std::string BuildAdvertisedEndpoint() const;
  std::string BestLanAddress() const;
  std::vector<std::pair<std::string, std::string>> LocalInterfaceAddresses() const;
  std::string CurrentTimestamp() const;

  void HandleHttpClient(int client_fd) const;
  HttpPeerRequest ParseHttpPeerRequest(const std::string& request) const;
  std::string HandlePeerJsonRequest(
      const std::string& path,
      const std::string& body,
      int* status_code,
      std::string* content_type) const;
  std::string HandlePeerChunkRequest(
      const std::string& body,
      int* status_code,
      std::string* content_type) const;
  std::string HandlePeerUploadStartRequest(
      const std::string& body,
      int* status_code,
      std::string* content_type) const;
  std::string HandlePeerUploadChunkRequest(
      const HttpPeerRequest& request,
      int* status_code,
      std::string* content_type) const;
  std::string HandlePeerUploadCompleteRequest(
      const std::string& body,
      int* status_code,
      std::string* content_type) const;

  bool ValidateTicketForPath(
      const std::string& ticket_id,
      const std::string& source_path,
      std::vector<std::string>* allowed_paths,
      std::uintmax_t* max_chunk_bytes) const;
  bool IsPathAllowed(
      const std::string& source_path,
      const std::vector<std::string>& allowed_paths) const;
  bool IsPathUnderStorageRoot(const std::string& source_path) const;
  bool ValidateUploadTicket(
      const std::string& ticket_id,
      std::string* target_relative_path,
      std::string* sha256,
      std::uintmax_t* size_bytes,
      bool* if_missing,
      std::uintmax_t* max_chunk_bytes) const;
  std::filesystem::path ResolveUploadTargetPath(const std::string& relative_path) const;
  std::string ReadPrivateKey() const;

  HostdRunOptions options_;
  bool enabled_ = true;
  int peer_port_ = 29999;
  int discovery_port_ = 29998;
  std::string discovery_group_ = "239.255.42.42";
  std::atomic<bool> stop_requested_{false};
  std::thread beacon_thread_;
  std::thread listen_thread_;
  std::thread http_thread_;
  mutable std::mutex peers_mutex_;
  std::map<std::string, PeerRecord> peers_;
  mutable std::mutex backend_mutex_;
  mutable std::unique_ptr<naim::hostd::HttpHostdBackend> cached_backend_;
  mutable std::map<std::string, CachedTransferTicket> transfer_ticket_cache_;
};

}  // namespace naim::launcher
