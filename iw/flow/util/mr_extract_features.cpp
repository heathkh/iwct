#include "deluge/deluge.h"
#include "snap/google/glog/logging.h"
#include "iw/iw.pb.h"
#include "snap/google/base/hashutils.h"
#include "iw/matching/feature_extractor.h"

using namespace std;
using namespace deluge;
using iw::ImageFeatures;
using iw::JpegImage;


class Mapper : public HadoopPipes::Mapper {
public:
  HadoopPipes::TaskContext::Counter* success_counter_;
  HadoopPipes::TaskContext::Counter* fail_counter_;

  Mapper(HadoopPipes::TaskContext& context) {
    JobInfo conf(context);
    success_counter_ = context.getCounter("iw", "success");
    fail_counter_ = context.getCounter("iw", "failure");
    params_ = conf.GetProtoOrDie<iw::FeatureExtractorParams>("feature_extractor_params");
    feature_extractor_ = iw::CreateFeatureExtractorOrDie(params_);
    CHECK(feature_extractor_) << "failed to create feature_extractor: " << params_.DebugString();
  }

  void map(HadoopPipes::MapContext& context) {
    const string& key = context.getInputKey();
    JpegImage image;
    CHECK(image.ParseFromString(context.getInputValue()));

    ImageFeatures features;
    if (feature_extractor_->Run(image.data(), &features)){
      context.emit(key, features.SerializeAsString());
      context.incrementCounter(success_counter_, 1);
    }
    else{
      LOG(WARNING)  << "Error extracting features from image id: " << KeyToUint64(key);
      context.incrementCounter(fail_counter_, 1);
    }
  }

  iw::FeatureExtractorParams params_;
  iw::FeatureExtractor* feature_extractor_;
};


int main(int argc, char *argv[]) {
  google::InstallFailureSignalHandler();
  ReaderProtoWriterJob<Mapper, void, ModKeyPartitioner, IdentityReducer, ImageFeatures> job;
  return HadoopPipes::runTask(job);
}

