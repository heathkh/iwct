#pragma once

#include "iw/matching/image_matcher.h"
#include "iw/matching/acransac_correspondences.h"
#include "iw/iw.pb.h"

namespace iw {

// Abstract base class for a-contrario image matchers
class ACRansacImageMatcher : public ImageMatcher {
public:
  ACRansacImageMatcher(const iw::ACRansacImageMatcherParams& params = iw::ACRansacImageMatcherParams());
  virtual ~ACRansacImageMatcher();

  bool Run(const iw::ImageFeatures& features_a,
           const iw::ImageFeatures& features_b,
           iw::GeometricMatches* matches, double* time);

protected:
  virtual bool FitModel(const iw::Correspondences& correspondences,
                        int w1,
                        int h1,
                        int w2,
                        int h2,
                        const ACRansacImageMatcherParams& params,
                        std::vector<size_t>* inliers,
                        Matrix3x3 *model,
                        double* nfa,
                        double* precision) = 0;
  virtual void CopyModelToMatch(const Matrix3x3& model, GeometricMatch* match) = 0;
  ACRansacImageMatcherParams params_;
};


class SimilarityACRansacImageMatcher : public ACRansacImageMatcher {
public:
  SimilarityACRansacImageMatcher(const iw::ACRansacImageMatcherParams& params = iw::ACRansacImageMatcherParams());
  virtual ~SimilarityACRansacImageMatcher();

protected:
  virtual bool FitModel(const iw::Correspondences& correspondences,
                          int w1,
                          int h1,
                          int w2,
                          int h2,
                          const ACRansacImageMatcherParams& params,
                          std::vector<size_t>* inliers,
                          Matrix3x3 *model,
                          double* nfa,
                          double* precision);

  virtual void CopyModelToMatch(const Matrix3x3& model, GeometricMatch* match);
};


class AffineACRansacImageMatcher : public ACRansacImageMatcher {
public:
  AffineACRansacImageMatcher(const iw::ACRansacImageMatcherParams& params = iw::ACRansacImageMatcherParams());
  virtual ~AffineACRansacImageMatcher();

protected:
  bool FitModel(const iw::Correspondences& correspondences,
                          int w1,
                          int h1,
                          int w2,
                          int h2,
                          const ACRansacImageMatcherParams& params,
                          std::vector<size_t>* inliers,
                          Matrix3x3 *model,
                          double* nfa,
                          double* precision);
  void CopyModelToMatch(const Matrix3x3& model, GeometricMatch* match);
};



class HomographyACRansacImageMatcher : public ACRansacImageMatcher {
public:
  HomographyACRansacImageMatcher(const iw::ACRansacImageMatcherParams& params = iw::ACRansacImageMatcherParams());
  virtual ~HomographyACRansacImageMatcher();

protected:
  bool FitModel(const iw::Correspondences& correspondences,
                          int w1,
                          int h1,
                          int w2,
                          int h2,
                          const ACRansacImageMatcherParams& params,
                          std::vector<size_t>* inliers,
                          Matrix3x3 *model,
                          double* nfa,
                          double* precision);
  void CopyModelToMatch(const Matrix3x3& model, GeometricMatch* match);
};


} // close namespace iw
