#include "iw/matching/visutil.h"
#include "iw/matching/feature_extractor.h"
#include "snap/google/glog/logging.h"
#include "snap/google/base/stringprintf.h"
#include "snap/boost/foreach.hpp"
#include <iostream>

using namespace std;
using namespace iw;

void RenderCorrespondenceResults(string filename_a, string filename_b){
  JpegImage image_a, image_b;
  LoadJpegImageOrDie(filename_a, &image_a);
  LoadJpegImageOrDie(filename_b, &image_b);

  ImageFeatures features_a, features_b;
  FeatureExtractor* feature_extractor = CreateFeatureExtractorOrDie("ahess");
  feature_extractor->Run(image_a.data(), &features_a);
  feature_extractor->Run(image_b.data(), &features_b);

  Correspondences correspondences, discarded_correspondences;
  ACRansacIndependenceFilter(features_a, features_b).Run(&correspondences, &discarded_correspondences);

  LOG(INFO) << "correspondences: " << correspondences.size();
  LOG(INFO) << "discarded: " << discarded_correspondences.size();
  SaveOrDie(RenderCorrespondencesSVG(image_a, image_b, correspondences), filename_a+"_correspondences.svg");
  SaveOrDie(RenderCorrespondencesSVG(image_a, image_b, discarded_correspondences), filename_a+"_discarded_correspondences.svg");
}

int main(int argc, char *argv[]) {
  FeatureExtractor* feature_extractor = CreateFeatureExtractorOrDie("ahess");
  for (int i=1; i <= 10; ++i){
    string filename_a = StringPrintf("%02da.jpg", i);
    string filename_b = StringPrintf("%02db.jpg", i);
    RenderCorrespondenceResults(filename_a, filename_b);
  }
  return 0;
}


