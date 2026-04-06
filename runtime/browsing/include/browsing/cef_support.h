#pragma once

#include <filesystem>
#include <string>

namespace comet::browsing {

bool CefBuildEnabled();
std::string CefBuildSummary();
int MaybeRunCefSubprocess(int argc, char** argv);
void InitializeCefOrThrow(
    int argc,
    char** argv,
    const std::filesystem::path& state_root,
    const std::filesystem::path& executable_path);
void ShutdownCef();
bool CefRuntimeEnabled();
std::string CurrentExecutablePath();

}  // namespace comet::browsing
