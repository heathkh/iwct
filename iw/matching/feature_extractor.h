#pragma once

#include "iw/iw.pb.h"
#include "snap/google/glog/logging.h"
#include "iw/matching/vgg/vgg_feature_extractor.h"
#include "opencv2/nonfree/nonfree.hpp"

namespace iw {

// Abstract base class for all feature extractors
class FeatureExtractor {
 public:
  virtual bool Run(const std::string& image_data, iw::ImageFeatures* features) = 0;
  virtual ~FeatureExtractor() {}
 private:
};

// Factory for constructing a feature extractor from a protobuffer describing all required configuration params
iw::FeatureExtractor* CreateFeatureExtractorOrDie(const iw::FeatureExtractorParams& params);

//
class OcvSiftFeatureExtractor : public FeatureExtractor {
 public:
  OcvSiftFeatureExtractor(const iw::FeatureExtractorParams::OcvSiftParams& params);
    virtual ~OcvSiftFeatureExtractor() {}
  virtual bool Run(const std::string& image_data, iw::ImageFeatures* features);

 private:
  iw::FeatureExtractorParams::OcvSiftParams params_;
  cv::SIFT sift_;
};

class VggAffineSiftFeatureExtractor : public FeatureExtractor {
 public:
  VggAffineSiftFeatureExtractor(const iw::FeatureExtractorParams::VggAffineSiftParams& params);
  virtual ~VggAffineSiftFeatureExtractor() {}
  virtual bool Run(const std::string& image_data, iw::ImageFeatures* features);

 private:
  iw::FeatureExtractorParams::VggAffineSiftParams params_;
  VGGFeatureExtractor vgg_;
};

/*
class MserFeatureExtractor : public FeatureExtractor {
 public:
  MserFeatureExtractor() : sift_(0, 3, 0.01, 30, 1.6, true) {} // default
  virtual ~MserFeatureExtractor() {}
  virtual bool Run(const std::string& image_data, iw::ImageFeatures* features);

 private:
  cv::SIFT sift_;
  cv::MserFeatureDetector mser_detector_;
};
*/
/*
class DenseFeatureExtractor : public FeatureExtractor {
 public:
  DenseFeatureExtractor() : sift_(0, 3, 0.01, 30, 1.6, true),
  dense_detector_( 50, // feature scale = 1
                   3, // featureScaleLevels=1,
                   0.5, //  featureScaleMul=0.1f,
                   10, //int initXyStep=6,
                   10, //int initImgBound=0,
                   false , //         bool varyXyStepWithScale=true,
                   false ) {}  //   bool varyImgBoundWithScale=false );
  virtual ~DenseFeatureExtractor() {}
  virtual bool Run(const std::string& image_data, iw::ImageFeatures* features);

 private:
  cv::SIFT sift_;
  cv::DenseFeatureDetector dense_detector_;
};
*/



} // close namespace iw
