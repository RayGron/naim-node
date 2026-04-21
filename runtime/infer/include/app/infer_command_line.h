#pragma once

#include <string>

namespace naim::infer {

struct InferCommandLineOptions {
  std::string command;
  std::string config_path;
  std::string profiles_path;
  std::string profile = "generic";
  std::string checks = "config,topology,filesystem,tools,gateway";
  std::string alias;
  std::string source_model_id;
  std::string local_model_path;
  std::string runtime_model_path;
  std::string model_id;
  std::string served_model_name;
  std::string backend = "auto";
  int tp = 1;
  int pp = 1;
  double gpu_memory_utilization = 0.9;
  bool apply = false;
  std::string probe_url;
};

class InferCommandLine final {
 public:
  InferCommandLineOptions Parse(int argc, char** argv) const;
  void PrintUsage() const;

 private:
  std::string DefaultConfigPath() const;
  std::string DefaultProfilesPath() const;
};

}  // namespace naim::infer
