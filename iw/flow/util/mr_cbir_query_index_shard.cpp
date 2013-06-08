#include "deluge/deluge.h"
#include "iw/iw.pb.h"
#include "iw/matching/cbir/full/full.h"
#include "snap/google/glog/logging.h"
#include "snap/pert/pert.h"
#include "snap/progress/progress.hpp"

using namespace std;
using namespace deluge;

class Mapper : public HadoopPipes::Mapper {
public:
  Mapper(HadoopPipes::TaskContext& context) : context_(context), counters_(context, "iw"), profiler_(context), num_images_proceessed_(0)  {
    JobInfo conf(context);
    MaprDistributedCache cache(context);
    std::string index_local_uri =  cache.GetResourceCacheUriOrDie("index_shard");
    Timer load_index_timer(counters_, "load_index_timer");
    num_neighbors_per_shard_ = conf.GetIntOrDie("num_neighbors_per_shard");
    CHECK_GE(num_neighbors_per_shard_, 1);
    include_keypoints_ = conf.GetIntOrDie("include_keypoints");
    inner_index_.reset(new cbir::FeatureIndex());
    CHECK(inner_index_->Load(index_local_uri));
    index_.reset(new cbir::ConcurrentFeatureIndex(*inner_index_.get()));
    compute_timer_.reset(new Timer(counters_, "compute_timer"));
  }

  virtual void close() {
    CHECK(index_.get());
    while (index_->NumJobsPending()){
     LOG(INFO) << "Waiting...";
     sleep(2);
    }
    EmitCompletedJobs();
    compute_timer_.reset(NULL);
  }

  void EmitCompletedJobs(){
    while (index_->JobsDone()){
      uint64 image_id;
      cbir::ImageFeaturesNeighbors neighbors;
      index_->GetJob(&image_id, &neighbors);
      if (neighbors.features_size()){
        context_.emit(Uint64ToKey(image_id), neighbors.SerializeAsString());
        counters_.Increment("num_image_queries");
      }
      else{
        counters_.Increment("num_failed_image_queries");
      }
    }
  }


  void map(HadoopPipes::MapContext& context) {
    const string& key = context.getInputKey();
    uint64 image_id = KeyToUint64(key);
    boost::shared_ptr<iw::ImageFeatures> features(new iw::ImageFeatures);
    CHECK(features->ParseFromString(context.getInputValue()));

    index_->AddJob(image_id, num_neighbors_per_shard_, include_keypoints_, features);
    num_images_proceessed_++;
    if (num_images_proceessed_ % 100 == 0){
      LOG(INFO) << "num jobs pending: " << index_->NumJobsPending();
      EmitCompletedJobs();
    }
  }


protected:
  HadoopPipes::TaskContext& context_;
  boost::scoped_ptr<cbir::FeatureIndex> inner_index_;
  boost::scoped_ptr<cbir::ConcurrentFeatureIndex> index_;
  CounterGroup counters_;
  Profiler profiler_;
  int num_neighbors_per_shard_;
  int num_images_proceessed_;
  bool include_keypoints_;
  boost::scoped_ptr<Timer> compute_timer_;
};

int main(int argc, char *argv[]) {
  google::InstallFailureSignalHandler();
  using namespace deluge;
  ReaderProtoWriterJob<Mapper, void, ModKeyPartitioner, IdentityReducer, cbir::ImageFeaturesNeighbors> job;
  return HadoopPipes::runTask(job);
}




