#include "iw/matching/cbir/full/full.h"
#include "iw/matching/cbir/full/full.pb.h"
#include "snap/deluge/deluge.h"
#include "snap/google/glog/logging.h"
#include "snap/pert/pert.h"

using namespace std;
using namespace deluge;

class Reducer : public HadoopPipes::Reducer {
public:
  Reducer(HadoopPipes::TaskContext& context) {
  }

  void reduce(HadoopPipes::ReduceContext& context) {
    cbir::ImageFeaturesNeighbors new_neighbors;
    cbir::NearestNeighborsAccumulator accumulator;
    while (context.nextValue()){
      CHECK(new_neighbors.ParseFromString(context.getInputValue()));
      accumulator.Add(new_neighbors);
    }
    cbir::ImageFeaturesNeighbors neighbors;
    CHECK(accumulator.GetResult(&neighbors));
    context.emit(context.getInputKey(), neighbors.SerializeAsString());
  }
};

int main(int argc, char *argv[]) {
  google::InstallFailureSignalHandler();
  ReaderProtoWriterJob<IdentityMapper, void, ModKeyPartitioner, Reducer, cbir::ImageFeaturesNeighbors> job;
  return HadoopPipes::runTask(job);
}

