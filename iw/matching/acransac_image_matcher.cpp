#include "iw/matching/acransac_image_matcher.h"
#include "iw/matching/acransac_correspondences.h"
#include "iw/matching/acransac_models.h"
#include "snap/google/glog/logging.h"
#include "snap/boost/timer.hpp"
#include "snap/orsa/homography_model.hpp"
#include "snap/orsa/fundamental_model.hpp"
#include "snap/boost/foreach.hpp"
#include <map>
#include <iostream>
#include <math.h>

using namespace std;

namespace iw {

ACRansacImageMatcher::ACRansacImageMatcher(const ACRansacImageMatcherParams& params) :
  params_(params) {
}

ACRansacImageMatcher::~ACRansacImageMatcher() {
}

bool ACRansacImageMatcher::Run(const ImageFeatures& features_a,
                               const ImageFeatures& features_b,
                               GeometricMatches* matches, double* time){
  CHECK(matches);
  CHECK(time);
  matches->Clear();
  *time = 0;
  boost::timer timer;

  FeatureCorrespondences initial_correspondences;
#if 1
  BidirectionalFeatureMatcher feature_matcher;
  feature_matcher.Run(features_a, features_b, &initial_correspondences);
#else
  CHECK(MatchFeaturesMutualBestMatchCemd(features_a, features_b, &initial_correspondences));
#endif

  Correspondences correspondences;
  Correspondences discarded_correspondences;
  ACRansacIndependenceFilter(features_a, features_b, initial_correspondences).Run(&correspondences, &discarded_correspondences);
  LOG(INFO) << "correspondences used: " << correspondences.size() << " discarded: " << discarded_correspondences.size();

  int num_correspondences = correspondences.size();
  bool found_model = false;
  if (num_correspondences > 0){
    std::vector<size_t> inlier_indices;
    Matrix3x3 model;
    double nfa;
    double precision;
    found_model = FitModel(correspondences,
                                features_a.width(),
                                features_a.height(),
                                features_b.width(),
                                features_b.height(),
                                params_,
                                &inlier_indices,
                                &model,
                                &nfa,
                                &precision);
    if (inlier_indices.size()){
      GeometricMatch* match = matches->add_entries();
      BOOST_FOREACH(int inlier_index, inlier_indices){
        match->add_correspondences()->CopyFrom(correspondences.Get(inlier_index));
      }
      match->set_nfa(nfa);
      match->set_precision(precision);
      CopyModelToMatch(model, match);
    }
  }

  *time = timer.elapsed();
  return found_model;
}




////////////


SimilarityACRansacImageMatcher::SimilarityACRansacImageMatcher(const iw::ACRansacImageMatcherParams& params) : ACRansacImageMatcher(params) {}
SimilarityACRansacImageMatcher::~SimilarityACRansacImageMatcher() {}

bool SimilarityACRansacImageMatcher::FitModel(const iw::Correspondences& correspondences,
                          int w1,
                          int h1,
                          int w2,
                          int h2,
                          const ACRansacImageMatcherParams& params,
                          std::vector<size_t>* inliers,
                          Matrix3x3 *model,
                          double* nfa,
                          double* precision){

  return FitModelType<iw::ExtendedOrsaSimilarityModel>(correspondences,
                      w1,
                      h1,
                      w2,
                      h2,
                      params,
                      inliers,
                      model,
                      nfa,
                      precision);
}


void SimilarityACRansacImageMatcher::CopyModelToMatch(const Matrix3x3& model, GeometricMatch* match){
  SimilarityModel* m = match->mutable_similarity_model();
  m->set_m00(model.m00());
  m->set_m01(model.m01());
  m->set_m02(model.m02());
  m->set_m10(model.m10());
  m->set_m11(model.m11());
  m->set_m12(model.m12());
}


////////////


AffineACRansacImageMatcher::AffineACRansacImageMatcher(const ACRansacImageMatcherParams& params) : ACRansacImageMatcher(params) {}
AffineACRansacImageMatcher::~AffineACRansacImageMatcher() {}


bool AffineACRansacImageMatcher::FitModel(const iw::Correspondences& correspondences,
                          int w1,
                          int h1,
                          int w2,
                          int h2,
                          const ACRansacImageMatcherParams& params,
                          std::vector<size_t>* inliers,
                          Matrix3x3 *model,
                          double* nfa,
                          double* precision){

  return FitModelType<iw::ExtendedOrsaAffineModel>(correspondences,
                      w1,
                      h1,
                      w2,
                      h2,
                      params,
                      inliers,
                      model,
                      nfa,
                      precision);
}


void AffineACRansacImageMatcher::CopyModelToMatch(const Matrix3x3& model, GeometricMatch* match){
  AffineModel* m = match->mutable_affine_model();
  m->set_m00(model.m00());
  m->set_m01(model.m01());
  m->set_m02(model.m02());
  m->set_m10(model.m10());
  m->set_m11(model.m11());
  m->set_m12(model.m12());
}

////////////


HomographyACRansacImageMatcher::HomographyACRansacImageMatcher(const ACRansacImageMatcherParams& params) : ACRansacImageMatcher(params) {}
HomographyACRansacImageMatcher::~HomographyACRansacImageMatcher() {}


bool HomographyACRansacImageMatcher::FitModel(const iw::Correspondences& correspondences,
                          int w1,
                          int h1,
                          int w2,
                          int h2,
                          const ACRansacImageMatcherParams& params,
                          std::vector<size_t>* inliers,
                          Matrix3x3 *model,
                          double* nfa,
                          double* precision){

  return FitModelType<iw::ExtendedOrsaHomographyModel>(correspondences,
                      w1,
                      h1,
                      w2,
                      h2,
                      params,
                      inliers,
                      model,
                      nfa,
                      precision);
}


void HomographyACRansacImageMatcher::CopyModelToMatch(const Matrix3x3& model, GeometricMatch* match){
  HomographyModel* m = match->mutable_homography_model();
  m->set_m00(model.m00());
  m->set_m01(model.m01());
  m->set_m02(model.m02());
  m->set_m10(model.m10());
  m->set_m11(model.m11());
  m->set_m12(model.m12());
  m->set_m20(model.m20());
  m->set_m21(model.m21());
  m->set_m22(model.m22());
}



} // close namespace iw
