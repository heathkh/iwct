#pragma once
#include "iw/iw.pb.h"
#include "iw/matching/acransac_correspondences.h"
#include "snap/google/glog/logging.h"
#include "snap/orsa/homography_model.hpp"
#include "snap/orsa/numeric/matrix.h"
#include "snap/orsa/orsa_model.hpp"
#include <cmath>
#include <vector>

namespace iw {

class ExtendedOrsaModel : public orsa::OrsaModel {
public:
  ExtendedOrsaModel(const iw::Correspondences& correspondences,
                    const Mat &x1, int w1, int h1,
                    const Mat &x2, int w2, int h2, const ACRansacImageMatcherParams& params);
  double Error(const Mat &A, size_t index, int* side=0) const;
  void Unnormalize(Model *model) const;

protected:
  double GetCachedGlobalScaling(const Mat &normalized_model) const;
  std::vector<double> correspondence_scaling_;
  mutable Mat cached_model_;
  mutable double cached_global_scaling_;
  const ACRansacImageMatcherParams& params_;
};


class ExtendedOrsaSimilarityModel : public ExtendedOrsaModel {
public:
  ExtendedOrsaSimilarityModel(const iw::Correspondences& correspondences,
                              const Mat &x1, int w1, int h1,
                              const Mat &x2, int w2, int h2, const ACRansacImageMatcherParams& params);
  int SizeSample() const { return 2; }
  int NbModels() const { return 1; }
  virtual bool DistToPoint() const { return true; }  // Distance used to distinguish inlier/outlier is to a point
  void Fit(const std::vector<size_t> &indices, std::vector<Mat> *A) const;

private:
  bool ModelIsDegenerate(double scale, double rotation) const;
};


class ExtendedOrsaAffineModel : public ExtendedOrsaModel {
public:
  ExtendedOrsaAffineModel(const iw::Correspondences& correspondences,
              const Mat &x1, int w1, int h1,
              const Mat &x2, int w2, int h2, const ACRansacImageMatcherParams& params);
  int SizeSample() const { return  3; }
  int NbModels() const { return 1; }
  virtual bool DistToPoint() const { return true; }
  void Fit(const std::vector<size_t> &indices, std::vector<Mat> *A) const;

private:
  bool ModelIsDegenerate(const double affine_data[6]) const;
};


class ExtendedOrsaHomographyModel : public ExtendedOrsaModel {
public:
  ExtendedOrsaHomographyModel(const iw::Correspondences& correspondences,
              const Mat &x1, int w1, int h1,
              const Mat &x2, int w2, int h2, const ACRansacImageMatcherParams& params) :
                ExtendedOrsaModel(correspondences, x1, w1, h2, x2, w2, h2, params),
                m_(x1, w1, h2, x2, w2, h2) { }
  int SizeSample() const { return m_.SizeSample(); }
  int NbModels() const { return m_.NbModels();  }
  virtual bool DistToPoint() const { return m_.DistToPoint(); }
  void Fit(const std::vector<size_t> &indices, std::vector<Mat> *H) const { return m_.Fit(indices, H); }

private:
  bool ModelIsDegenerate(const double affine_data[6]) const { return false; }
  orsa::HomographyModel m_;
};



template <typename ModelType>
bool FitModelType(const iw::Correspondences& correspondences,
                    int w1,
                    int h1,
                    int w2,
                    int h2,
                    const ACRansacImageMatcherParams& params,
                    std::vector<size_t>* inliers,
                    Matrix3x3 *model,
                    double* nfa,
                    double* precision) {
  CHECK(inliers);
  CHECK(model);
  CHECK(nfa);
  CHECK(precision);
  CHECK_GT(params.precision_ratio(), 0);
  CHECK_LE(params.precision_ratio(), 1.0);

  int num_correspondences = correspondences.size();
  orsa::matrix<double> points_a(2, num_correspondences);
  orsa::matrix<double> points_b(2, num_correspondences);
  for (int i=0; i < num_correspondences; ++i)  {
    const Correspondence& c = correspondences.Get(i);
    points_a(0,i) = c.a().pos().x();
    points_a(1,i) = c.a().pos().y();
    points_b(0,i) = c.b().pos().x();
    points_b(1,i) = c.b().pos().y();
  }

  double image2_size = sqrt(double(w2*h2));
  ModelType orsa_model(correspondences, points_a, w1, h1, points_b, h2, h2, params);
  double required_precision = image2_size*params.precision_ratio();
  LOG(INFO) << "required_precision: " << required_precision;
  orsa::OrsaModel::Mat model_mat = orsa::OrsaModel::Mat::zeros(3);
  *nfa = orsa_model.orsa(*inliers, params.max_iterations(), required_precision, precision, &model_mat, true);

  model->set_m00(model_mat(0,0));
  model->set_m01(model_mat(0,1));
  model->set_m02(model_mat(0,2));

  model->set_m10(model_mat(1,0));
  model->set_m11(model_mat(1,1));
  model->set_m12(model_mat(1,2));

  model->set_m20(model_mat(2,0));
  model->set_m21(model_mat(2,1));
  model->set_m22(model_mat(2,2));

  bool found_model = inliers->size() > 0;
  return found_model;
}


} // close namespace iw

