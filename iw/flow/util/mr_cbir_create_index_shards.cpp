#include "iw/iw.pb.h"
#include "iw/matching/cbir/full/full.h"
#include "snap/deluge/deluge.h"
#include "snap/google/base/hashutils.h"
#include "snap/google/glog/logging.h"

using namespace std;
using namespace deluge;

/*
class Mapper : public HadoopPipes::Mapper {
public:
  Mapper(HadoopPipes::TaskContext& context) {
    JobInfo conf(context);
  }

  void map(HadoopPipes::MapContext& context) {
    iw::ImageFeatures features;
    CHECK(features.ParseFromString(context.getInputValue()));
    context.emit(context.getInputKey(), features.SerializeAsString());
  }
};
*/

class Reducer : public HadoopPipes::Reducer {
public:
  Reducer(HadoopPipes::TaskContext& context) : counters_(context ,"iw"), profiler_(context)  {
    JobInfo conf(context);
    int partition_num = conf.GetIntOrDie("mapred.task.partition");
    int estimated_features_per_index = conf.GetIntOrDie("estimated_features_per_index");
    flann_params_ = conf.GetProtoOrDie<cbir::FlannParams>("flann_params");
    index.StartPreallocate(estimated_features_per_index);
    std::string work_dir = CleanupMaprPathFormat(conf.GetStringOrDie("mapred.work.output.dir"));
    index_output_path_ = StringPrintf("%s/index_part_%05d/", work_dir.c_str(), partition_num);
  }

  virtual void close() {
    index.Build(flann_params_);
    index.Save(index_output_path_);
  }

  void reduce(HadoopPipes::ReduceContext& context) {
    uint64 image_id = KeyToUint64(context.getInputKey());
    iw::ImageFeatures features;
    CHECK(context.nextValue());
    CHECK(features.ParseFromString(context.getInputValue()));
    index.AddPreallocated(image_id, features);
    CHECK(!context.nextValue()) << "This means your dataset includes duplicate images (hash to same key)";
    //while (context.nextValue()){
    //  counters_.Increment("duplicated_keys");
    //}
  }

private:
  CounterGroup counters_;
  std::string index_output_path_;
  cbir::FeatureIndex index;
  cbir::FlannParams flann_params_;
  Profiler profiler_;
};


int main(int argc, char *argv[]) {
  google::InstallFailureSignalHandler();
  ReaderWriterJob<IdentityMapper, void, ModKeyPartitioner, Reducer> job;
  return HadoopPipes::runTask(job);
}




