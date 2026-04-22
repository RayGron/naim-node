#pragma once

#include <initializer_list>
#include <string>
#include <string_view>
#include <vector>

namespace naim::controller::serve_support {

class ControllerRouteSummaryBuilder final {
 public:
  std::string BuildControllerRoutesSummary(bool webgateway_routes_enabled) const;
  std::string BuildSkillsFactoryRoutesSummary() const;

 private:
  using RouteSummary = std::vector<std::string_view>;

  static void AppendRoutes(
      RouteSummary& summary,
      std::initializer_list<std::string_view> routes);
  static std::string JoinRoutes(const RouteSummary& routes);
};

}  // namespace naim::controller::serve_support
