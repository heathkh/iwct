#pragma once
#include "iw/iw.pb.h"

namespace iw {

/** Extracts harris or hessian affine covariant features from images.
 * This is simply a wrapper around the binary available here
 * http://www.robots.ox.ac.uk/~vgg/research/affine/
 */
class VGGFeatureExtractor {
 public:
  enum FeatureType {HESSIAN = 0, HARRIS = 1};
  VGGFeatureExtractor(FeatureType type, double threshold);
  virtual ~VGGFeatureExtractor();
  bool ExtractFromData(const std::string& image_data, iw::ImageFeatures* features);

 private:
  //methods
  bool ParseAsciiFormat(std::string filename, iw::ImageFeatures* features);

  //members
  std::string feature_flag_;
  std::string threshold_flag_;
  std::string binPath_;
  std::string bin_name_;
  std::string temp_dir_;
  int max_feature_count_;
};

} // close namespace iw
