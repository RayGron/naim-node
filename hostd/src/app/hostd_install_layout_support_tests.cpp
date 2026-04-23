#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

#include "app/hostd_install_layout_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

}  // namespace

int main() {
  try {
    const naim::hostd::HostdInstallLayoutSupport support;
    namespace fs = std::filesystem;

    Expect(
        support.ResolveHostdRootFromPrivateKeyPath(
            "/opt/naim/hostd/install-state/keys/hostd.key.b64") ==
            fs::path("/opt/naim/hostd"),
        "split install-state/keys layout should resolve hostd root");
    Expect(
        support.ResolveHostdRootFromPrivateKeyPath("/opt/naim/hostd/keys/hostd.key.b64") ==
            fs::path("/opt/naim/hostd"),
        "legacy keys layout should resolve hostd root");

    std::cout << "ok: resolve-hostd-root-from-install-state-keys-layout\n";
    std::cout << "ok: resolve-hostd-root-from-legacy-keys-layout\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
