#include <iostream>
#include <stdexcept>
#include <string>

#include "app/hostd_command_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

}  // namespace

int main() {
  try {
    const naim::hostd::HostdCommandSupport support;
    Expect(support.Trim("  hello \n") == "hello", "Trim should strip outer whitespace");
    Expect(
        support.ShellQuote("a'b") == "'a'\"'\"'b'",
        "ShellQuote should escape single quotes");
    std::cout << "ok: hostd-command-support-trim\n";
    std::cout << "ok: hostd-command-support-shell-quote\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
