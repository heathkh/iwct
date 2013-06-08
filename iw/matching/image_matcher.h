#pragma once

#include "iw/iw.pb.h"

namespace iw {

// Abstract base class for all image matchers
class ImageMatcher {
public:
  ImageMatcher();
  virtual ~ImageMatcher();

  virtual bool Run(const iw::ImageFeatures& features_a,
                   const iw::ImageFeatures& features_b,
                   GeometricMatches* matches, double* time) = 0;

};

// Factory to construct an ImageMatcher from a configuration proto
ImageMatcher* CreateImageMatcherOrDie(const iw::ImageMatcherConfig& config);

} // close namespace iw
