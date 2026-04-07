#include "model/model_conversion_service.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <numeric>
#include <stdexcept>
#include <string_view>
#include <thread>

#if !defined(_WIN32)
#include <csignal>
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "comet/core/platform_compat.h"

namespace {

constexpr std::string_view kFormatGguf = "gguf";
constexpr std::string_view kFormatSafetensors = "safetensors";
constexpr std::string_view kFormatUnknown = "unknown";
constexpr std::array<std::string_view, 7> kKnownQuantizations = {
    "FP16",
    "TQ2_0",
    "TQ1_0",
    "Q8_0",
    "Q5_K_M",
    "Q4_K_M",
    "IQ4_NL",
};

bool IsHfAuxiliaryConversionFile(const std::string& lowered_filename) {
  return lowered_filename.ends_with(".json") || lowered_filename.ends_with(".model") ||
         lowered_filename.ends_with(".txt");
}

std::filesystem::path JoinNormalized(
    const std::filesystem::path& left,
    const std::filesystem::path& right) {
  return (left / right).lexically_normal();
}

std::string SanitizeLogLabel(std::string value) {
  std::transform(
      value.begin(),
      value.end(),
      value.begin(),
      [](unsigned char ch) {
        if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')) {
          return static_cast<char>(ch);
        }
        return '-';
      });
  while (!value.empty() && value.back() == '-') {
    value.pop_back();
  }
  while (!value.empty() && value.front() == '-') {
    value.erase(value.begin());
  }
  return value.empty() ? std::string("command") : value;
}

std::filesystem::path FindBuildArtifact(
    const std::filesystem::path& build_root,
    const std::filesystem::path& relative_path) {
  if (!std::filesystem::exists(build_root) || !std::filesystem::is_directory(build_root)) {
    return {};
  }

  for (const auto& os_entry : std::filesystem::directory_iterator(build_root)) {
    if (!os_entry.is_directory()) {
      continue;
    }
    for (const auto& arch_entry : std::filesystem::directory_iterator(os_entry.path())) {
      if (!arch_entry.is_directory()) {
        continue;
      }
      const auto candidate = arch_entry.path() / relative_path;
      if (std::filesystem::exists(candidate)) {
        return candidate;
      }
    }
  }
  return {};
}

}  // namespace

ModelConversionService::ModelConversionService(std::filesystem::path repo_root)
    : tool_locator_(repo_root.empty() ? DetectRepoRoot() : std::move(repo_root)) {}

std::string ModelConversionService::DetectSourceFormat(
    const std::vector<std::string>& source_urls) {
  if (source_urls.empty()) {
    return std::string(kFormatUnknown);
  }
  bool saw_safetensors = false;
  bool saw_gguf = false;
  for (const auto& source_url : source_urls) {
    const auto lowered = Lowercase(FilenameFromUrl(source_url));
    if (lowered.ends_with(".gguf")) {
      saw_gguf = true;
      continue;
    }
    if (lowered.ends_with(".safetensors")) {
      saw_safetensors = true;
      continue;
    }
    if (IsHfAuxiliaryConversionFile(lowered)) {
      continue;
    }
    return std::string(kFormatUnknown);
  }
  if (saw_safetensors && !saw_gguf) {
    return std::string(kFormatSafetensors);
  }
  if (saw_gguf && !saw_safetensors) {
    return std::string(kFormatGguf);
  }
  return std::string(kFormatUnknown);
}

std::string ModelConversionService::NormalizeOutputFormat(const std::string& value) {
  const auto normalized = Lowercase(Trim(value));
  if (normalized == kFormatGguf) {
    return std::string(kFormatGguf);
  }
  if (normalized == kFormatSafetensors) {
    return std::string(kFormatSafetensors);
  }
  return normalized.empty() ? std::string(kFormatUnknown) : normalized;
}

std::vector<std::string> ModelConversionService::NormalizeQuantizations(
    const std::vector<std::string>& values) {
  std::vector<std::string> normalized;
  for (const auto& value : values) {
    const auto trimmed = Trim(value);
    if (trimmed.empty()) {
      continue;
    }
    std::string canonical = trimmed;
    std::transform(
        canonical.begin(),
        canonical.end(),
        canonical.begin(),
        [](unsigned char ch) { return static_cast<char>(std::toupper(ch)); });
    normalized.push_back(std::move(canonical));
  }
  std::sort(normalized.begin(), normalized.end());
  normalized.erase(std::unique(normalized.begin(), normalized.end()), normalized.end());
  return normalized;
}

std::string ModelConversionService::DeriveBaseFilename(
    const std::string& model_id,
    const std::filesystem::path& destination_root,
    const std::vector<std::string>& source_urls) {
  if (!Trim(model_id).empty()) {
    const auto slash = model_id.find_last_of('/');
    const auto leaf = slash == std::string::npos ? model_id : model_id.substr(slash + 1);
    if (!Trim(leaf).empty()) {
      return Trim(leaf);
    }
  }
  if (!destination_root.filename().string().empty()) {
    return destination_root.filename().string();
  }
  if (!source_urls.empty()) {
    return std::filesystem::path(FilenameFromUrl(source_urls.front())).stem().string();
  }
  throw std::runtime_error("failed to derive GGUF output filename");
}

ModelConversionService::Plan ModelConversionService::BuildPlan(
    const Request& request) const {
  Plan plan;
  plan.destination_root = request.destination_root.lexically_normal();
  plan.staging_directory = request.staging_directory.lexically_normal();
  plan.quantizations = NormalizeQuantizations(request.quantizations);

  for (const auto& source_url : request.source_urls) {
    plan.downloaded_source_paths.push_back(
        JoinNormalized(plan.staging_directory, FilenameFromUrl(source_url)));
  }

  const std::string base_name =
      DeriveBaseFilename(request.model_id, plan.destination_root, request.source_urls);
  plan.staged_base_output_path =
      JoinNormalized(plan.staging_directory, base_name + ".gguf");

  for (const auto& quantization : plan.quantizations) {
    plan.staged_quantized_output_paths.push_back(
        JoinNormalized(plan.staging_directory, base_name + "-" + quantization + ".gguf"));
  }

  if (plan.quantizations.empty() || request.keep_base_gguf) {
    plan.retained_output_paths.push_back(
        JoinNormalized(plan.destination_root, base_name + ".gguf"));
  }
  for (const auto& quantization : plan.quantizations) {
    plan.retained_output_paths.push_back(
        JoinNormalized(plan.destination_root, base_name + "-" + quantization + ".gguf"));
  }
  return plan;
}

ModelConversionService::QuantizationPlan ModelConversionService::BuildQuantizationPlan(
    const QuantizationRequest& request) const {
  const auto trimmed_quantization = Trim(request.quantization);
  if (trimmed_quantization.empty()) {
    throw std::runtime_error("quantization type is required");
  }
  const auto source_filename = request.source_path.filename().string();
  if (!Lowercase(source_filename).ends_with(".gguf")) {
    throw std::runtime_error("quantization source must be a GGUF file");
  }
  QuantizationPlan plan;
  plan.source_path = request.source_path.lexically_normal();
  plan.staging_directory = request.staging_directory.lexically_normal();
  plan.quantization = trimmed_quantization;
  const auto base_stem = StripKnownQuantizationSuffix(request.source_path.stem().string());
  plan.staged_output_path =
      JoinNormalized(plan.staging_directory, base_stem + "-" + trimmed_quantization + ".gguf");
  plan.retained_output_path = request.retained_output_path.lexically_normal();
  return plan;
}

void ModelConversionService::Execute(
    const Request& request,
    const Plan& plan,
    const JobHooks& hooks) const {
  EnsureDirectory(plan.destination_root);
  EnsureDirectory(plan.staging_directory);

  if (NormalizeOutputFormat(request.detected_source_format) != kFormatSafetensors ||
      NormalizeOutputFormat(request.desired_output_format) != kFormatGguf) {
    return;
  }

  HfToGgufPythonConverter(tool_locator_.Resolve()).Convert(plan, hooks);
  if (!plan.quantizations.empty()) {
    GgufQuantizationService(tool_locator_.Resolve()).Quantize(plan, hooks);
  }

  if (hooks.update_job) {
    hooks.update_job("cleaning", "retaining outputs", std::nullopt, 0);
  }

  if (plan.quantizations.empty() || request.keep_base_gguf) {
    const auto retained_base = plan.retained_output_paths.front();
    EnsureDirectory(retained_base.parent_path());
    if (!FileExistsAndNonEmpty(retained_base)) {
      std::filesystem::rename(plan.staged_base_output_path, retained_base);
    }
  }

  const std::size_t quantized_retained_offset =
      (plan.quantizations.empty() || request.keep_base_gguf) ? 1u : 0u;
  for (std::size_t index = 0; index < plan.staged_quantized_output_paths.size(); ++index) {
    const auto& staged_quantized = plan.staged_quantized_output_paths.at(index);
    const auto& retained_quantized =
        plan.retained_output_paths.at(index + quantized_retained_offset);
    EnsureDirectory(retained_quantized.parent_path());
    if (!FileExistsAndNonEmpty(retained_quantized)) {
      std::filesystem::rename(staged_quantized, retained_quantized);
    }
  }

  for (const auto& source_path : plan.downloaded_source_paths) {
    RemovePathIfExists(source_path);
  }
  if (!request.keep_base_gguf && !plan.quantizations.empty()) {
    RemovePathIfExists(plan.staged_base_output_path);
  }
  std::error_code cleanup_error;
  std::filesystem::remove_all(plan.staging_directory, cleanup_error);
}

void ModelConversionService::ExecuteQuantization(
    const QuantizationRequest& request,
    const QuantizationPlan& plan,
    const JobHooks& hooks) const {
  EnsureDirectory(plan.staging_directory);
  GgufQuantizationService(tool_locator_.Resolve()).Quantize(
      Plan{
          .destination_root = plan.retained_output_path.parent_path(),
          .staging_directory = plan.staging_directory,
          .downloaded_source_paths = {},
          .staged_base_output_path = plan.source_path,
          .quantizations = {plan.quantization},
          .staged_quantized_output_paths = {plan.staged_output_path},
          .retained_output_paths = {plan.retained_output_path},
      },
      hooks);
  if (hooks.update_job) {
    hooks.update_job("cleaning", "retaining outputs", std::nullopt, 0);
  }
  EnsureDirectory(plan.retained_output_path.parent_path());
  if (request.replace_existing) {
    RemovePathIfExists(plan.retained_output_path);
  }
  std::filesystem::rename(plan.staged_output_path, plan.retained_output_path);
  std::error_code cleanup_error;
  std::filesystem::remove_all(plan.staging_directory, cleanup_error);
}

ModelConversionService::LlamaCppToolLocator::LlamaCppToolLocator(
    std::filesystem::path repo_root)
    : repo_root_(repo_root.empty() ? DetectRepoRoot() : std::move(repo_root)) {}

ModelConversionService::ToolPaths
ModelConversionService::LlamaCppToolLocator::Resolve() const {
  ToolPaths paths;
  if (const char* value = std::getenv("COMET_MODEL_LIBRARY_PYTHON");
      value != nullptr && *value != '\0') {
    paths.python_executable = value;
  } else if (const char* value = std::getenv("COMET_LLAMA_CPP_PYTHON");
             value != nullptr && *value != '\0') {
    paths.python_executable = value;
  } else if (std::filesystem::exists("/usr/bin/python3")) {
    paths.python_executable = "/usr/bin/python3";
  } else {
    paths.python_executable = "python3";
  }

  if (const char* value = std::getenv("COMET_MODEL_LIBRARY_CONVERT_SCRIPT");
      value != nullptr && *value != '\0') {
    paths.convert_script_path = value;
  } else if (const char* value = std::getenv("COMET_LLAMA_CPP_CONVERT_SCRIPT");
             value != nullptr && *value != '\0') {
    paths.convert_script_path = value;
  } else {
#if defined(COMET_LLAMA_CPP_SOURCE_TREE)
    const std::filesystem::path compiled_source_tree(COMET_LLAMA_CPP_SOURCE_TREE);
    if (std::filesystem::exists(compiled_source_tree / "convert_hf_to_gguf.py")) {
      paths.convert_script_path = compiled_source_tree / "convert_hf_to_gguf.py";
    }
#endif
    if (paths.convert_script_path.empty()) {
      const auto candidate = FindBuildArtifact(
          repo_root_ / "build", "_deps/llama_cpp-src/convert_hf_to_gguf.py");
      if (!candidate.empty()) {
        paths.convert_script_path = candidate;
      }
    }
  }

  if (const char* value = std::getenv("COMET_MODEL_LIBRARY_QUANTIZE_BIN");
      value != nullptr && *value != '\0') {
    paths.quantize_executable_path = value;
  } else if (const char* value = std::getenv("COMET_LLAMA_CPP_QUANTIZE");
             value != nullptr && *value != '\0') {
    paths.quantize_executable_path = value;
  } else {
#if defined(COMET_LLAMA_CPP_RUNTIME_OUTPUT_DIR)
    const std::filesystem::path runtime_output(COMET_LLAMA_CPP_RUNTIME_OUTPUT_DIR);
    if (std::filesystem::exists(runtime_output / "llama-quantize")) {
      paths.quantize_executable_path = runtime_output / "llama-quantize";
    }
#endif
    if (paths.quantize_executable_path.empty()) {
      const auto candidate = FindBuildArtifact(
          repo_root_ / "build",
#if defined(_WIN32)
          "llama-quantize.exe"
#else
          "llama-quantize"
#endif
      );
      if (!candidate.empty()) {
        paths.quantize_executable_path = candidate;
      }
    }
  }

  if (paths.convert_script_path.empty()) {
    throw std::runtime_error(
        "failed to locate convert_hf_to_gguf.py; set COMET_MODEL_LIBRARY_CONVERT_SCRIPT");
  }
  return paths;
}

ModelConversionService::HfToGgufPythonConverter::HfToGgufPythonConverter(
    ToolPaths tool_paths)
    : tool_paths_(std::move(tool_paths)) {}

void ModelConversionService::HfToGgufPythonConverter::Convert(
    const Plan& plan,
    const JobHooks& hooks) const {
  if (FileExistsAndNonEmpty(plan.staged_base_output_path)) {
    if (hooks.update_job) {
      hooks.update_job("converting", plan.staged_base_output_path.filename().string(), std::nullopt, 0);
    }
    return;
  }
  std::vector<std::string> argv{
      tool_paths_.python_executable,
      tool_paths_.convert_script_path.string(),
      "--outfile",
      plan.staged_base_output_path.string(),
      plan.staging_directory.string(),
  };
  RunCommand(
      argv,
      plan.staging_directory,
      hooks,
      "converting",
      plan.staged_base_output_path.filename().string(),
      plan.staged_base_output_path,
      std::accumulate(
          plan.downloaded_source_paths.begin(),
          plan.downloaded_source_paths.end(),
          std::uintmax_t{0},
          [](std::uintmax_t total, const std::filesystem::path& path) {
            return total + FileSizeOrZero(path);
          }));
}

ModelConversionService::GgufQuantizationService::GgufQuantizationService(
    ToolPaths tool_paths)
    : tool_paths_(std::move(tool_paths)) {}

void ModelConversionService::GgufQuantizationService::Quantize(
    const Plan& plan,
    const JobHooks& hooks) const {
  if (tool_paths_.quantize_executable_path.empty()) {
    throw std::runtime_error(
        "failed to locate llama-quantize; set COMET_MODEL_LIBRARY_QUANTIZE_BIN");
  }
  for (std::size_t index = 0; index < plan.quantizations.size(); ++index) {
    const auto& quantization = plan.quantizations.at(index);
    const auto& output_path = plan.staged_quantized_output_paths.at(index);
    if (FileExistsAndNonEmpty(output_path)) {
      if (hooks.update_job) {
        hooks.update_job("quantizing", quantization, std::nullopt, 0);
      }
      continue;
    }
    std::vector<std::string> argv{
        tool_paths_.quantize_executable_path.string(),
        plan.staged_base_output_path.string(),
        output_path.string(),
        quantization,
    };
    RunCommand(
        argv,
        plan.staging_directory,
        hooks,
        "quantizing",
        quantization,
        output_path,
        FileSizeOrZero(plan.staged_base_output_path));
  }
}

std::string ModelConversionService::FilenameFromUrl(const std::string& source_url) {
  std::string trimmed = source_url;
  if (const auto query = trimmed.find('?'); query != std::string::npos) {
    trimmed = trimmed.substr(0, query);
  }
  if (const auto fragment = trimmed.find('#'); fragment != std::string::npos) {
    trimmed = trimmed.substr(0, fragment);
  }
  const auto slash = trimmed.find_last_of('/');
  const auto filename = slash == std::string::npos ? trimmed : trimmed.substr(slash + 1);
  if (filename.empty()) {
    throw std::runtime_error("failed to infer filename from source URL: " + source_url);
  }
  return filename;
}

std::string ModelConversionService::Lowercase(const std::string& value) {
  std::string normalized = value;
  std::transform(
      normalized.begin(),
      normalized.end(),
      normalized.begin(),
      [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return normalized;
}

std::string ModelConversionService::Trim(const std::string& value) {
  const auto begin = value.find_first_not_of(" \t\r\n");
  if (begin == std::string::npos) {
    return "";
  }
  const auto end = value.find_last_not_of(" \t\r\n");
  return value.substr(begin, end - begin + 1);
}

std::string ModelConversionService::StripKnownQuantizationSuffix(const std::string& stem) {
  for (const auto quantization : kKnownQuantizations) {
    const std::string suffix = "-" + std::string(quantization);
    if (stem.size() > suffix.size() &&
        stem.compare(stem.size() - suffix.size(), suffix.size(), suffix) == 0) {
      return stem.substr(0, stem.size() - suffix.size());
    }
  }
  return stem;
}

std::string ModelConversionService::ShellEscape(const std::string& value) {
  std::string escaped = "'";
  for (const char ch : value) {
    if (ch == '\'') {
      escaped += "'\"'\"'";
    } else {
      escaped.push_back(ch);
    }
  }
  escaped.push_back('\'');
  return escaped;
}

bool ModelConversionService::FileExistsAndNonEmpty(const std::filesystem::path& path) {
  std::error_code error;
  return std::filesystem::exists(path, error) && !error &&
         std::filesystem::is_regular_file(path, error) && !error &&
         std::filesystem::file_size(path, error) > 0 && !error;
}

std::uintmax_t ModelConversionService::FileSizeOrZero(const std::filesystem::path& path) {
  std::error_code error;
  if (!std::filesystem::exists(path, error) || error ||
      !std::filesystem::is_regular_file(path, error) || error) {
    return 0;
  }
  return std::filesystem::file_size(path, error);
}

void ModelConversionService::EnsureDirectory(const std::filesystem::path& path) {
  std::error_code error;
  std::filesystem::create_directories(path, error);
  if (error) {
    throw std::runtime_error("failed to create directory " + path.string() + ": " + error.message());
  }
}

void ModelConversionService::RemovePathIfExists(const std::filesystem::path& path) {
  std::error_code error;
  if (!std::filesystem::exists(path, error) || error) {
    return;
  }
  if (std::filesystem::is_directory(path, error) && !error) {
    std::filesystem::remove_all(path, error);
  } else {
    std::filesystem::remove(path, error);
  }
  if (error) {
    throw std::runtime_error("failed to remove path " + path.string() + ": " + error.message());
  }
}

std::filesystem::path ModelConversionService::DetectRepoRoot() {
  const auto executable_path = std::filesystem::path(comet::platform::ExecutablePath());
  if (!executable_path.empty()) {
    auto current = executable_path.parent_path();
    while (!current.empty()) {
      if (std::filesystem::exists(current / "CMakeLists.txt") &&
          std::filesystem::exists(current / "controller")) {
        return current;
      }
      if (current == current.root_path()) {
        break;
      }
      current = current.parent_path();
    }
  }
  return std::filesystem::current_path();
}

void ModelConversionService::RunCommand(
    const std::vector<std::string>& argv,
    const std::filesystem::path& working_directory,
    const JobHooks& hooks,
    const std::string& phase,
    const std::string& current_item,
    const std::filesystem::path& progress_path,
    const std::optional<std::uintmax_t>& progress_total) {
  if (argv.empty()) {
    throw std::runtime_error("conversion command is empty");
  }
  if (hooks.update_job) {
    hooks.update_job(phase, current_item, progress_total, 0);
  }

#if defined(_WIN32)
  std::string command = "cd /d " + ShellEscape(working_directory.string()) + " &&";
  for (const auto& arg : argv) {
    command.push_back(' ');
    command += ShellEscape(arg);
  }
  const int rc = std::system(command.c_str());
  if (rc != 0) {
    throw std::runtime_error("command failed during " + phase);
  }
#else
  pid_t pid = fork();
  if (pid < 0) {
    throw std::runtime_error("fork failed while running " + phase);
  }
  if (pid == 0) {
    if (!working_directory.empty()) {
      chdir(working_directory.c_str());
      const auto log_path =
          working_directory / (".comet-" + SanitizeLogLabel(phase) + ".log");
      const int log_fd =
          open(log_path.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0664);
      if (log_fd >= 0) {
        dup2(log_fd, STDOUT_FILENO);
        dup2(log_fd, STDERR_FILENO);
        if (log_fd > STDERR_FILENO) {
          close(log_fd);
        }
      } else {
        std::perror(("open " + log_path.string()).c_str());
      }
    }
    std::vector<char*> raw_argv;
    raw_argv.reserve(argv.size() + 1);
    for (const auto& arg : argv) {
      raw_argv.push_back(const_cast<char*>(arg.c_str()));
    }
    raw_argv.push_back(nullptr);
    execv(raw_argv.front(), raw_argv.data());
    execvp(raw_argv.front(), raw_argv.data());
    std::perror("execv");
    _exit(127);
  }
  if (hooks.register_pid) {
    hooks.register_pid(static_cast<int>(pid));
  }
  int wait_status = 0;
  while (true) {
    const auto wait_result = waitpid(pid, &wait_status, WNOHANG);
    if (wait_result == pid) {
      break;
    }
    if (wait_result < 0) {
      if (hooks.clear_pid) {
        hooks.clear_pid();
      }
      throw std::runtime_error("waitpid failed while running " + phase);
    }
    if (hooks.stop_requested && hooks.stop_requested()) {
      kill(pid, SIGTERM);
    }
    if (hooks.update_job) {
      hooks.update_job(
          phase,
          current_item,
          progress_total,
          progress_path.empty() ? 0 : FileSizeOrZero(progress_path));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  if (hooks.clear_pid) {
    hooks.clear_pid();
  }
  if (hooks.stop_requested && hooks.stop_requested()) {
    throw std::runtime_error("conversion stopped");
  }
  if (!WIFEXITED(wait_status) || WEXITSTATUS(wait_status) != 0) {
    throw std::runtime_error("command failed during " + phase);
  }
#endif
}
