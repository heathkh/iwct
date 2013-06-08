#include "snap/google/glog/logging.h"
#include <google/protobuf/stubs/common.h>

using namespace std;

int main(int argc, char *argv[]) {
  //google::InstallFailureSignalHandler();

  int headerVersion = 2005000;
  int minLibraryVersion = 2005000;

  google::protobuf::internal::VerifyVersion(headerVersion, minLibraryVersion, __FILE__);


  return 0;
}

