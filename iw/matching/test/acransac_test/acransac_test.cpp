#include "iw/matching/visutil.h"
#include "iw/matching/feature_extractor.h"
#include "iw/matching/acransac_image_matcher.h"
#include "snap/google/glog/logging.h"
#include "snap/google/base/stringprintf.h"
#include "snap/boost/foreach.hpp"
#include <iostream>

using namespace std;
using namespace iw;

void RenderMatchingResults(string filename_a, string filename_b){
  JpegImage image_a, image_b;
  LoadJpegImageOrDie(filename_a, &image_a);
  LoadJpegImageOrDie(filename_b, &image_b);

  ImageFeatures features_a, features_b;
  FeatureExtractorParams extractor_params;
  extractor_params.vgg_affine_sift_params(); // creates default params for vgg affine sift
  FeatureExtractor* feature_extractor = CreateFeatureExtractorOrDie(extractor_params);
  CHECK(feature_extractor->Run(image_a.data(), &features_a));
  CHECK(feature_extractor->Run(image_b.data(), &features_b));

  CHECK_GT(features_a.width(), 0);
  CHECK_GT(features_a.height(), 0);
  CHECK_GT(features_b.width(), 0);
  CHECK_GT(features_b.height(), 0);

  GeometricMatches matches;
  double time;

  ACRansacImageMatcherParams matcher_params;
  matcher_params.set_max_iterations(50000);
  matcher_params.set_precision_ratio(0.25);
  matcher_params.set_max_scaling(5.0);
  matcher_params.set_max_correspondence_scale_deviation(0.75);
  matcher_params.set_max_in_plane_rotation(0.785398163);

  {
    SimilarityACRansacImageMatcher matcher(matcher_params);
    matcher.Run(features_a, features_b, &matches, &time);
    SaveOrDie(RenderGeometricMatchesHTML(image_a, image_b, matches), filename_a+filename_b+"_similarity_matches.html");
  }


  {
    AffineACRansacImageMatcher matcher(matcher_params);
    matcher.Run(features_a, features_b, &matches, &time);
    SaveOrDie(RenderGeometricMatchesHTML(image_a, image_b, matches), filename_a+filename_b+"_affine_matches.html");
  }


  {
    HomographyACRansacImageMatcher matcher;
    matcher.Run(features_a, features_b, &matches, &time);
    SaveOrDie(RenderGeometricMatchesHTML(image_a, image_b, matches), filename_a+filename_b+"_homography_matches.html");
  }

  /*
  {
    EpipolarACRansacImageMatcher matcher;
    matcher.Run(features_a, features_b, &matches, &time);
    SaveOrDie(RenderGeometricMatchesHTML(image_a, image_b, matches), filename_a+filename_b+"_epipolar_matches.html");
  }
  */

}

int main(int argc, char *argv[]) {
  for (int i=20; i <= 20; ++i){
    string filename_a = StringPrintf("%02da.jpg", i);
    string filename_b = StringPrintf("%02db.jpg", i);
    RenderMatchingResults(filename_a, filename_b);
  }
  return 0;
}


