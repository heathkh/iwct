#include "deluge/deluge.h"
#include "snap/google/glog/logging.h"
#include "iw/iw.pb.h"

using namespace deluge;

// This just sorts by key
int main(int argc, char *argv[]) {
  google::InstallFailureSignalHandler();
  ReaderProtoWriterJob<IdentityMapper, void, ModKeyPartitioner, AtMostOnceReducer, iw::GeometricMatchResult> job;
  return HadoopPipes::runTask(job);
}
