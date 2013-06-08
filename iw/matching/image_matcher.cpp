#include "iw/matching/image_matcher.h"
#include "iw/matching/acransac_image_matcher.h"
#include "snap/google/glog/logging.h"

namespace iw {

ImageMatcher::ImageMatcher() {}
ImageMatcher::~ImageMatcher() {}


ImageMatcher* CreateImageMatcherOrDie(const ImageMatcherConfig& config) {
  CHECK(config.has_affine_acransac_params()) <<  "missing has_affine_acransac_params: " << config.DebugString(); // only one kind of matcher currently supported
  ImageMatcher* matcher = new AffineACRansacImageMatcher(config.affine_acransac_params());
  return matcher;
}

} // close namespace iw
