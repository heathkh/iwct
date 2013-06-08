#include "snap/google/glog/logging.h"
#include "snap/deluge/deluge.h"
#include "iw/iw.pb.h"
#include "iw/matching/cbir/cbir.pb.h"
#include "iw/util.h"
#include "snap/google/base/hashutils.h"
#include <string>

using namespace std;
using namespace boost;
using namespace cbir;
using namespace iw;
using namespace deluge;

// This takes a set of cbir query results for each image, instantiates all match candidates, and removes duplicates

enum OrderingMethod {INVALID_ORDERING_METHOD, INCREASING_BY_CBIR_RANK, DECREASING_BY_CBIR_SCORE};

OrderingMethod GetOrderingMethod(const JobInfo& info){
  string ordering_method_str = info.GetStringOrDie("ordering_method");
  OrderingMethod method = INVALID_ORDERING_METHOD;
  if (ordering_method_str == "INCREASING_BY_CBIR_RANK"){
    method = INCREASING_BY_CBIR_RANK;
  }
  else if (ordering_method_str == "DECREASING_BY_CBIR_SCORE"){
    method = DECREASING_BY_CBIR_SCORE;
  }
  else{
    LOG(FATAL) << "invalid ordering method: " << ordering_method_str;
  }
  return method;
}

class Mapper : public HadoopPipes::Mapper {
public:

  Mapper(HadoopPipes::TaskContext& context) : counters_(context, "iw") {
    JobInfo info(context);
    ordering_method_ = GetOrderingMethod(info);
  }

  void map(HadoopPipes::MapContext& context) {
    uint64 imageid1 = KeyToUint64(context.getInputKey());
    cbir::QueryResults results;
    CHECK(results.ParseFromString(context.getInputValue()));
    results.CheckInitialized();
    for (int rank=0; rank < results.entries_size(); ++rank){
      uint64 imageid2 = results.entries(rank).image_id();
      CHECK_NE(imageid2, imageid1); // Check invariant: we should never compute a score for ourselves if prev stages blacklist us
      iw::Scalar s;
      if (ordering_method_ == DECREASING_BY_CBIR_SCORE){
        s.set_value(results.entries(rank).score());
      }
      else if (ordering_method_ == INCREASING_BY_CBIR_RANK){
        s.set_value(rank);
      }
      else{
        LOG(FATAL);
      }
      context.emit(iw::UndirectedEdgeKey(imageid1, imageid2), s.SerializeAsString());
    }
  }

private:
  CounterGroup counters_;
  OrderingMethod ordering_method_;
};


class Reducer : public HadoopPipes::Reducer {
public:
  Reducer(HadoopPipes::TaskContext& context) : counters_(context, "iw") {
    JobInfo info(context);
    ordering_method_ = GetOrderingMethod(info);
  }

  // If there are duplicate candidates, return the one with largest score
  void reduce(HadoopPipes::ReduceContext& context) {
    std::vector<double> scores;
    iw::Scalar s;
    while(context.nextValue()){
      CHECK(s.ParseFromString(context.getInputValue()));
      scores.push_back(s.value());
    }

    // A candidate should appear either once or twice
    CHECK_GE(scores.size(), 1);
    //CHECK_LE(scores.size(), 2);
    if (scores.size() > 2){
      counters_.Increment("unexpected condition");
    }

    if (ordering_method_ == DECREASING_BY_CBIR_SCORE){
      std::sort(scores.rbegin(), scores.rend()); // sort descending by cbir score... pos 0 has max score
    }
    else if (ordering_method_ == INCREASING_BY_CBIR_RANK){
      std::sort(scores.begin(), scores.end()); // sort ascending by cbir rank... pos 0 has min rank
    }
    else{
      LOG(FATAL);
    }

    s.set_value(scores[0]);
    context.emit(context.getInputKey(), s.SerializeAsString());
  }
private:
  OrderingMethod ordering_method_;
  CounterGroup counters_;
};


int main(int argc, char *argv[]) {
  google::InstallFailureSignalHandler();
  using namespace deluge;
  ReaderProtoWriterJob<Mapper, void, ModKeyPartitioner, Reducer, iw::Scalar> job;
  return HadoopPipes::runTask(job);
}




