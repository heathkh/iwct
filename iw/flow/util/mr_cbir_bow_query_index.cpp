#include "iw/iw.pb.h"
#include "iw/matching/cbir/cbir.pb.h"
#include "iw/matching/cbir/bow/ctis/index.h"
#include "snap/boost/timer/timer.hpp"
#include "snap/deluge/deluge.h"
#include "snap/google/glog/logging.h"

using namespace std;
using namespace cbir;
using namespace deluge;
using boost::timer::cpu_timer;

class Mapper : public HadoopPipes::Mapper {
public:

  Mapper(HadoopPipes::TaskContext& context) : counters_(context, "iw") {
    JobInfo conf(context);
    MaprDistributedCache cache(context);
    params_ = conf.GetProtoOrDie<BowCbirParams>("cbir_bow_params");

    if (params_.implementation() == "ctis"){
      index_.reset(new CtisIndex());
    }
    else{
      LOG(FATAL) << "unknown implementation type: " << params_.implementation();
    }
    string index_uri = cache.GetResourceCacheUriOrDie("bow_index");
    UriLocalCache index_cache(index_uri);
    bow_index_filebase_ = index_cache.GetLocalPath();

    LOG(INFO) << "Initializing bow index: " << bow_index_filebase_;
    cpu_timer timer;
    timer.start();
    CHECK(index_->Load(bow_index_filebase_)) << "failed to load: " << bow_index_filebase_;
    timer.stop();
    double load_time_ms = timer.elapsed().wall*1.0e-6; // nanoseconds to miliseconds

    LOG(INFO) << "num params: " << params_.DebugString();
    LOG(INFO) << "index loaded in " << load_time_ms << " ms";
    counters_.Increment("cbirdb_init_ms", load_time_ms);
    counters_.Increment("cbirdb_init_counter");
  }


  void map(HadoopPipes::MapContext& context) {
    const string& image_key = context.getInputKey();
    uint64 image_id = KeyToUint64(image_key);
    BagOfWords bag_of_words;
    CHECK(bag_of_words.ParseFromString(context.getInputValue()));
    bag_of_words.CheckInitialized();
    QueryResults results;
    if (!index_->Query(image_id, bag_of_words, params_.num_candidates(), &results) || !results.IsInitialized() ){
      counters_.Increment("fail_counter");
      return;
    }

    if (results.entries_size() < params_.num_candidates()){
      counters_.Increment("partial_fail_counter");
    }
    counters_.Increment("total_results_count", results.entries_size());
    context.emit(image_key, results.SerializeAsString());
  }

protected:
  CounterGroup counters_;
  string bow_index_uri_;
  string bow_index_filebase_;
  //iw_bow::CtisIndex index_;
  boost::scoped_ptr<Index> index_;
  //int num_results_;
  BowCbirParams params_;
};

int main(int argc, char *argv[]) {
  google::InstallFailureSignalHandler();
  ReaderProtoWriterJob<Mapper, void, ModKeyPartitioner, IdentityReducer, BagOfWords> job;
  return HadoopPipes::runTask(job);
}




