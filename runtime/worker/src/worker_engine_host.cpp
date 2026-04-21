#include "worker_engine_host.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

namespace naim::worker {

namespace fs = std::filesystem;

namespace {

std::string ResolveExecutablePath(const char* env_name, const char* fallback) {
  const char* value = std::getenv(env_name);
  if (value != nullptr && *value != '\0') {
    return value;
  }
  return fallback;
}

std::string ResolveRpcDevice(const naim::worker::WorkerConfig& config) {
  if (!config.rpc_device.empty()) {
    return config.rpc_device;
  }

  const char* visible_devices = std::getenv("NVIDIA_VISIBLE_DEVICES");
  if (visible_devices != nullptr && *visible_devices != '\0') {
    const std::string value(visible_devices);
    if (value != "none" && value != "void") {
      // Worker containers are pinned to a single visible NVIDIA device, so CUDA0
      // is the stable in-container device name regardless of host GPU index.
      return "CUDA0";
    }
  }

  if (!config.gpu_device.empty()) {
    return "CUDA0";
  }

  return "";
}

}  // namespace

WorkerEngineHost::WorkerEngineHost(WorkerConfig config)
    : config_(std::move(config)),
      signal_service_(),
      model_resolver_(),
      status_service_(),
      started_at_(status_service_.CurrentTimestamp()) {}

WorkerEngineHost::~WorkerEngineHost() = default;

int WorkerEngineHost::Run() {
  signal_service_.RegisterHandlers();
  std::cout << "[naim-workerd] booting plane=" << config_.plane_name
            << " instance=" << config_.instance_name
            << " node=" << config_.node_name
            << " gpu=" << (config_.gpu_device.empty() ? "(auto)" : config_.gpu_device)
            << " boot_mode=" << config_.boot_mode
            << "\n";

  if (config_.boot_mode == "llama-rpc") {
    return RunRpcWorker();
  }
  return RunIdleWorker();
}

int WorkerEngineHost::RunIdleWorker() {
  while (!signal_service_.StopRequested()) {
    try {
      const auto resolved_model_path = model_resolver_.ResolveModelPath(config_);
      if (!resolved_model_path.has_value()) {
        status_service_.MarkWaitingForModel(config_, started_at_, loaded_model_path_);
        std::this_thread::sleep_for(std::chrono::seconds(2));
        continue;
      }
      if (!loaded_model_path_.has_value() || *loaded_model_path_ != *resolved_model_path) {
        loaded_model_path_ = *resolved_model_path;
      }

      status_service_.MarkRunning(config_, started_at_, loaded_model_path_.value_or(""));
      std::this_thread::sleep_for(std::chrono::seconds(2));
    } catch (const std::exception& error) {
      status_service_.MarkFailed(config_, started_at_, loaded_model_path_);
      std::cerr << "[naim-workerd] " << error.what() << "\n";
      std::this_thread::sleep_for(std::chrono::seconds(2));
    }
  }

  status_service_.MarkStopped(config_, started_at_, loaded_model_path_);
  return 0;
}

int WorkerEngineHost::RunRpcWorker() {
  status_service_.MarkStarting(config_, started_at_, std::nullopt);

  fs::create_directories(fs::path(config_.private_disk_path) / "rpc-cache");
  const std::string rpc_device = ResolveRpcDevice(config_);

  pid_t child = fork();
  if (child < 0) {
    throw std::runtime_error("failed to fork rpc-server process");
  }

  if (child == 0) {
    std::vector<std::string> args = {
        ResolveExecutablePath("NAIM_RPC_SERVER_BIN", "/runtime/bin/rpc-server"),
        "--host",
        config_.rpc_host,
        "--port",
        std::to_string(config_.rpc_port),
        "--threads",
        std::to_string(std::max(1, config_.llama_threads)),
        "--cache",
    };
    if (!rpc_device.empty()) {
      args.push_back("--device");
      args.push_back(rpc_device);
    }
    std::vector<char*> argv;
    argv.reserve(args.size() + 1);
    for (auto& arg : args) {
      argv.push_back(arg.data());
    }
    argv.push_back(nullptr);
    execv(argv.front(), argv.data());
    std::perror("execv rpc-server");
    _exit(127);
  }

  child_pid_ = child;
  std::cout << "[naim-workerd] launching rpc-server host=" << config_.rpc_host
            << " port=" << config_.rpc_port
            << " threads=" << std::max(1, config_.llama_threads)
            << " device=" << (rpc_device.empty() ? "(default)" : rpc_device)
            << "\n";
  if (!EnsureTcpEndpointReady("127.0.0.1", config_.rpc_port, 60)) {
    StopChildProcess();
    status_service_.MarkFailed(config_, started_at_, std::nullopt);
    throw std::runtime_error(
        "timed out waiting for rpc-server to listen on 127.0.0.1:" +
        std::to_string(config_.rpc_port));
  }

  status_service_.MarkRunning(config_, started_at_, "");
  while (!signal_service_.StopRequested()) {
    int child_status = 0;
    const pid_t wait_result = waitpid(*child_pid_, &child_status, WNOHANG);
    if (wait_result == *child_pid_) {
      child_pid_.reset();
      status_service_.MarkFailed(config_, started_at_, std::nullopt);
      std::cerr << "[naim-workerd] rpc-server exited unexpectedly";
      if (WIFEXITED(child_status)) {
        std::cerr << " exit_code=" << WEXITSTATUS(child_status);
      } else if (WIFSIGNALED(child_status)) {
        std::cerr << " signal=" << WTERMSIG(child_status);
      }
      std::cerr << "\n";
      return 1;
    }
    status_service_.MarkRunning(config_, started_at_, "");
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  StopChildProcess();
  status_service_.MarkStopped(config_, started_at_, std::nullopt);
  return 0;
}

bool WorkerEngineHost::EnsureTcpEndpointReady(
    const std::string& host,
    int port,
    int timeout_sec) const {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(timeout_sec);
  while (std::chrono::steady_clock::now() < deadline &&
         !signal_service_.StopRequested()) {
    if (child_pid_.has_value()) {
      int child_status = 0;
      const pid_t wait_result = waitpid(*child_pid_, &child_status, WNOHANG);
      if (wait_result == *child_pid_) {
        return false;
      }
    }

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    addrinfo* results = nullptr;
    const std::string service = std::to_string(port);
    if (getaddrinfo(host.c_str(), service.c_str(), &hints, &results) == 0) {
      for (addrinfo* current = results; current != nullptr; current = current->ai_next) {
        const int fd = socket(current->ai_family, current->ai_socktype, current->ai_protocol);
        if (fd < 0) {
          continue;
        }
        const int connected = connect(fd, current->ai_addr, current->ai_addrlen);
        close(fd);
        if (connected == 0) {
          freeaddrinfo(results);
          return true;
        }
      }
      freeaddrinfo(results);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  return false;
}

void WorkerEngineHost::StopChildProcess() {
  if (!child_pid_.has_value()) {
    return;
  }
  const pid_t pid = *child_pid_;
  kill(pid, SIGTERM);
  const auto deadline =
      std::chrono::steady_clock::now() +
      std::chrono::seconds(std::max(1, config_.graceful_stop_timeout_sec));
  while (std::chrono::steady_clock::now() < deadline) {
    int child_status = 0;
    const pid_t wait_result = waitpid(pid, &child_status, WNOHANG);
    if (wait_result == pid) {
      child_pid_.reset();
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  kill(pid, SIGKILL);
  int child_status = 0;
  waitpid(pid, &child_status, 0);
  child_pid_.reset();
}

}  // namespace naim::worker
