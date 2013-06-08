#include "iw/matching/feature_extractor.h"
#include "snap/google/glog/logging.h"

using namespace std;
using namespace iw;

int main(int argc, char *argv[]) {
  FeatureExtractorParams p;
  p.vgg_affine_sift_params(); // creates default params for vgg affine sift
  FeatureExtractor* feature_extractor = CreateFeatureExtractorOrDie(p);
  CHECK(feature_extractor);
  LOG(INFO) << "success";
  return 0;
}


