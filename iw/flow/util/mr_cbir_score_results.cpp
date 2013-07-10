#include "deluge/deluge.h"
#include "iw/iw.pb.h"
#include "iw/matching/cbir/full/full.h"
#include "iw/util.h"
#include "snap/boost/scoped_ptr.hpp"
#include "snap/google/base/hashutils.h"
#include "snap/google/glog/logging.h"


using namespace std;
using namespace boost;
using namespace cbir;
using namespace iw;
using namespace deluge;

class Mapper : public HadoopPipes::Mapper {
public:

  Mapper(HadoopPipes::TaskContext& context) : profiler_(context), counters_(context, "iw") {
    JobInfo conf(context);
    MaprDistributedCache cache(context);
    string feature_count_uri = cache.GetResourceCacheUriOrDie("feature_counts");
    CHECK(feature_counts_.Load(feature_count_uri));
    scorer_.reset(new QueryScorer(feature_counts_, conf.GetProtoOrDie<QueryScorerParams>("query_scorer_params")));
  }

  void map(HadoopPipes::MapContext& context) {
    //uint64 imageid1 = KeyToUint64(context.getInputKey());
    cbir::ImageFeaturesNeighbors nearest_neighbors;

    if (nearest_neighbors.ParseFromString(context.getInputValue())){
      //nearest_neighbors.CheckInitialized();
      cbir::QueryResults results;
      CHECK(scorer_->Run(nearest_neighbors, &results));
      context.emit(context.getInputKey(), results.SerializeAsString());
      counters_.Increment("num_images_scored");
    }
    else{
      counters_.Increment("num_images_failed");
    }

  }

protected:
  Profiler profiler_;
  scoped_ptr<cbir::QueryScorer> scorer_;
  CounterGroup counters_;
  cbir::ImageFeatureCountTable feature_counts_;

};

int main(int argc, char *argv[]) {
  google::InstallFailureSignalHandler();
  ReaderProtoWriterJob<Mapper, void, ModKeyPartitioner, IdentityReducer, cbir::QueryResults> job;
  return HadoopPipes::runTask(job);
}




