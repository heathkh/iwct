#include "snap/google/glog/logging.h"
#include "snap/google/base/hashutils.h"
#include "deluge/deluge.h"
#include "iw/iw.pb.h"

using namespace std;
using namespace deluge;

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

  Mapper(HadoopPipes::TaskContext& context) {
    JobInfo info(context);
    ordering_method_ = GetOrderingMethod(info);
  }


  void map(HadoopPipes::MapContext& context) {
    const string& match_candidate_key = context.getInputKey();
    iw::Scalar s;
    CHECK(s.ParseFromString(context.getInputValue()));

    std::string score_key;
    if (ordering_method_ == DECREASING_BY_CBIR_SCORE){
      // need to negate score it to get the desired sort order (i.e. order decreasing by score)
      KeyFromDouble(-s.value(), &score_key);
    }
    else if (ordering_method_ == INCREASING_BY_CBIR_RANK){
      // using rank as the key gives the desired order (increasing by rank)
      KeyFromDouble(s.value(), &score_key);
    }
    else{
      LOG(FATAL);
    }
    context.emit(score_key, match_candidate_key);
  }
private:
  OrderingMethod ordering_method_;
};

int main(int argc, char *argv[]) {
  google::InstallFailureSignalHandler();
  ReaderWriterJob<Mapper, void, ModKeyPartitioner, IdentityReducer> job;
  return HadoopPipes::runTask(job);
}




