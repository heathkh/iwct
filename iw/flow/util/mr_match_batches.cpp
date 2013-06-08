#include "iw/flow/util/match_batches_mapper.h"
#include "iw/flow/util/match_batches_reducer.h"

using namespace deluge;

int main(int argc, char *argv[]) {
  google::InstallFailureSignalHandler();
  ReaderProtoWriterJob<MatchBatchesMapper, void, void, MatchBatchesReducer, GeometricMatchResult> job;
  return HadoopPipes::runTask(job);
}


