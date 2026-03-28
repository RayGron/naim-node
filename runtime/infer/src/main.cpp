#include "app/infer_app.h"

int main(int argc, char** argv) {
  comet::infer::InferApp app(argc, argv);
  return app.Run();
}
