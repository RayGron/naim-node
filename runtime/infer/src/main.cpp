#include "app/infer_app.h"

int main(int argc, char** argv) {
  naim::infer::InferApp app(argc, argv);
  return app.Run();
}
