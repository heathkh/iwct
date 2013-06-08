#include "snap/google/glog/logging.h"



int main(int argc, char **argv) {
  google::InstallFailureSignalHandler();

  CHECK(false);

  return 0;
}
