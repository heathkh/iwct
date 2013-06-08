#include "snap/google/glog/logging.h"
#include "iw/iw.pb.h"
#include "iw/matching/cbir/cbir.pb.h"
#include "deluge/deluge.h"

using namespace std;
using namespace deluge;

class Mapper : public HadoopPipes::Mapper {
public:
  HadoopPipes::TaskContext::Counter* success_counter_;
  HadoopPipes::TaskContext::Counter* fail_counter_;

  Mapper(HadoopPipes::TaskContext& context) {
  }

  void map(HadoopPipes::MapContext& context) {
    const string& key = context.getInputKey();
    iw::ImageFeatures features;
    CHECK(features.ParseFromString(context.getInputValue()));
    CHECK_EQ(features.descriptors_size(), features.keypoints_size());
    cbir::Count count;
    count.set_value(features.descriptors_size());
    context.emit(key, count.SerializeAsString());
  }
};

int main(int argc, char *argv[]) {
  google::InstallFailureSignalHandler();
  ReaderProtoWriterJob<Mapper, void, ModKeyPartitioner, IdentityReducer, cbir::Count> job;
  return HadoopPipes::runTask(job);
}

