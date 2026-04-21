#pragma once

#include "http/controller_http_types.h"

namespace naim::controller {

class ControllerRequestContext {
 public:
  class Scope {
   public:
    explicit Scope(const HttpRequest& request);
    ~Scope();

    Scope(const Scope&) = delete;
    Scope& operator=(const Scope&) = delete;

   private:
    const HttpRequest* previous_ = nullptr;
  };

  static const HttpRequest* Current();

 private:
  static const HttpRequest*& CurrentSlot();
};

}  // namespace naim::controller
