
#include "iw/iw.pb.h"
#include "iw/matching/cbir/bow/quantizer.h"
#include "snap/boost/timer/timer.hpp"
#include "snap/deluge/deluge.h"
#include "snap/google/glog/logging.h"

using boost::timer::cpu_timer;
using namespace std;
using namespace deluge;
using namespace iw;
using namespace cbir;


class Mapper : public HadoopPipes::Mapper {
public:
  HadoopPipes::TaskContext::Counter* success_counter_;
  HadoopPipes::TaskContext::Counter* fail_counter_;
  HadoopPipes::TaskContext::Counter* quantizer_init_counter_;
  HadoopPipes::TaskContext::Counter* quantizer_init_time_counter_;

  Mapper(HadoopPipes::TaskContext& context) {
    JobInfo conf(context);
    MaprDistributedCache cache(context);
    success_counter_ = context.getCounter("iw", "success");
    fail_counter_ = context.getCounter("iw", "failure");
    quantizer_init_counter_ = context.getCounter("iw", "quantizer_init");
    quantizer_init_time_counter_ = context.getCounter("iw", "quantizer_init_time_counter");

    std::string uri_key;
    std::string vv_uri = cache.GetResourceCacheUriOrDie("visual_vocab");

    LOG(INFO) << "Initializing quantizer: " << vv_uri;
    cpu_timer timer;
    timer.start();
    CHECK(quantizer_.Init(vv_uri)) << "failed to load quantizer clusters: " << vv_uri;
    timer.stop();
    double quantizer_load_seconds = timer.elapsed().wall*1.0e-9; // convert nanoseconds to seconds
    LOG(INFO) << "Done initializing quantizer.. it took " << quantizer_load_seconds << " sec";
    context.incrementCounter(quantizer_init_time_counter_, quantizer_load_seconds);
    context.incrementCounter(quantizer_init_counter_, 1);
  }

  void map(HadoopPipes::MapContext& context) {
    const string& image_key = context.getInputKey();
    const string& features_value = context.getInputValue();

    ImageFeatures features;
    //CHECK(features.ParseFromString(features_value));

    if (!features.ParseFromString(features_value)){
      context.incrementCounter(fail_counter_, 1);
      return;
    }

    BagOfWords bag_of_words;
    CHECK(quantizer_.Quantize(features, &bag_of_words));

    //TODO: INRIA indexer requires vw to be sorted in increasing order
    // Can remove after inria is deprecated
    std::sort(bag_of_words.mutable_word_id()->begin(), bag_of_words.mutable_word_id()->end());

    context.incrementCounter(success_counter_, 1);

    context.emit(image_key, bag_of_words.SerializeAsString());
  }

protected:
  Quantizer quantizer_;
};

int main(int argc, char *argv[]) {
  google::InstallFailureSignalHandler();
  ReaderProtoWriterJob<Mapper, void, ModKeyPartitioner, IdentityReducer, BagOfWords> job;
  return HadoopPipes::runTask(job);
}

