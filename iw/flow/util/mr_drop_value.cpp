#include "snap/google/glog/logging.h"
#include "deluge/deluge.h"
#include "iw/iw.pb.h"

using namespace std;
using namespace deluge;

int main(int argc, char *argv[]) {
  google::InstallFailureSignalHandler();
  ReaderWriterJob<DropValueMapper, void, ModKeyPartitioner, IdentityReducer> job;
  return HadoopPipes::runTask(job);
}




