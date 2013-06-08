#include "iw/matching/acransac_models.h"

#include "snap/boost/foreach.hpp"
#include "snap/Eigen/Core"
#include "snap/Eigen/Geometry"
#include "snap/google/base/stringprintf.h"
#include "snap/google/glog/logging.h"
#include "snap/opencv2/core/core.hpp"
#include "snap/orsa/conditioning.hpp"
#include <iostream>
#include <math.h>

//typedef Eigen::Vector2d Point2D
typedef Eigen::Affine2d AffineTransform2D;
typedef Eigen::Rotation2D<double> Rotation2D;
typedef Eigen::Translation2d Translation2D;

namespace iw {

ExtendedOrsaModel::ExtendedOrsaModel(const iw::Correspondences& correspondences,
                    const Mat &x1, int w1, int h1,
                    const Mat &x2, int w2, int h2,
                    const ACRansacImageMatcherParams& params) :
 OrsaModel(x1, w1, h1, x2, w2, h2),
 cached_model_(Mat::zeros(3)),
 cached_global_scaling_(-1),
 params_(params) {
    correspondence_scaling_.resize(correspondences.size());
    for (int i=0; i < correspondences.size(); ++i){
      const iw::Correspondence& c = correspondences.Get(i);
      correspondence_scaling_[i] = c.b().radius()/c.a().radius();
    }
}

/// Unnormalize a given model (from normalized to image space).
void ExtendedOrsaModel::Unnormalize(Model *model) const  {
  orsa::UnnormalizerI::Unnormalize(N1_, N2_, model);
}

inline double sqr(double x) {return x*x;}

double ExtendedOrsaModel::Error(const orsa::OrsaModel::Mat &orsaH, size_t index, int* side) const {
  double err = std::numeric_limits<double>::infinity();
  double global_scaling = GetCachedGlobalScaling(orsaH);
  double delta_scale = correspondence_scaling_[index]/global_scaling;
  double scale_deviation = fabs(delta_scale - 1.0);
  bool has_consistent_scale = scale_deviation < params_.max_correspondence_scale_deviation();
  if (has_consistent_scale){
    if(side) {
      *side=1;
    }
    Eigen::Map<Eigen::Matrix<double,3,3,Eigen::RowMajor> > H((double*)orsaH.data()); //
    Eigen::Vector3d p1(x1_(0,index), x1_(1,index), 1.0);
    Eigen::Vector3d x = H*p1;
    if(x.coeff(2) > 0.0) {
      x /= x.coeff(2);
      err = sqr(x2_(0,index)-x.coeff(0)) + sqr(x2_(1,index)-x.coeff(1));
    }
  }
  return err;
}

double AffineScaling(const orsa::Mat& m){
    AffineTransform2D a2d;
    a2d(0,0) = m(0,0);
    a2d(0,1) = m(0,1);
    a2d(0,2) = m(0,2);
    a2d(1,0) = m(1,0);
    a2d(1,1) = m(1,1);
    a2d(1,2) = m(1,2);

    AffineTransform2D::LinearMatrixType scaling;
    a2d.computeRotationScaling((AffineTransform2D::LinearMatrixType*)NULL, &scaling);
    double scale_x = scaling(0,0);
    double scale_y = scaling(1,1);
    return (scale_x+scale_y)/2.0;
}

double ExtendedOrsaModel::GetCachedGlobalScaling(const Mat &normalized_model) const {
  // Note: must compute the global scaling on the unnormalized model (otherwise there is an additional scaling unaccounted for)
  if (!cached_model_.equal(normalized_model)){
    cached_model_ = normalized_model;
    Mat unormalized_model = normalized_model;
    Unnormalize(&unormalized_model);
    cached_global_scaling_ = AffineScaling(unormalized_model);
  }
  return cached_global_scaling_;
}


/// Constructor, initializing \c logalpha0_
ExtendedOrsaSimilarityModel::ExtendedOrsaSimilarityModel(
                         const iw::Correspondences& correspondences,
                         const Mat &x1, int w1, int h1,
                         const Mat &x2, int w2, int h2,
                         const ACRansacImageMatcherParams& params)
: ExtendedOrsaModel(correspondences, x1, w1, h1, x2, w2, h2, params) {
  // for planar models
  logalpha0_[0] = log10(M_PI/(w1*(double)h1) /(N1_(0,0)*N1_(0,0)));
  logalpha0_[1] = log10(M_PI/(w2*(double)h2) /(N2_(0,0)*N2_(0,0)));
}


/////

bool ExtendedOrsaSimilarityModel::ModelIsDegenerate(double scale, double rotation) const {
  double max_scale = params_.max_scaling();
  double min_scale = 1.0/max_scale;
  if (scale > max_scale || scale < min_scale) {
    return true;
  }

  if (params_.has_max_in_plane_rotation()){
    if (abs(rotation) > params_.max_in_plane_rotation()){
      return true;
    }
  }

  return false;
}

#define RAD2DEG 57.2957795
/// 2D similarity estimation from point correspondences.
// NOTE: we don't allow reflections... only rotations
void ExtendedOrsaSimilarityModel::Fit(const std::vector<size_t> &indices,
                          std::vector<Model> *models) const {
  if(indices.size() < 2)
    return;
  size_t index_1 = indices[0];
  size_t index_2 = indices[1];

  double a1_x = x1_(0, index_1);
  double a1_y = x1_(1, index_1);
  double b1_x = x2_(0, index_1);
  double b1_y = x2_(1, index_1);

  double a2_x = x1_(0, index_2);
  double a2_y = x1_(1, index_2);
  double b2_x = x2_(0, index_2);
  double b2_y = x2_(1, index_2);

  // compute translation of a1 to align points a1 and b1
  double t_x = b1_x - a1_x;
  double t_y = b1_y - a1_y;

  // compute rotation of a1->a2 to align vector a1->a2 to b1->b2
  double a_dx = a2_x - a1_x;
  double a_dy = a2_y - a1_y;
  double r_a = atan2(a_dy, a_dx);

  double b_dx = b2_x - b1_x;
  double b_dy = b2_y - b1_y;
  double r_b = atan2(b_dy, b_dx);

  double r = r_b - r_a;  // rotation in radians

  // compute scaling to scale a1->a2 to have same magnitude as b1->b2
  double a_len = sqrt(a_dx*a_dx + a_dy*a_dy);
  double b_len = sqrt(b_dx*b_dx + b_dy*b_dy);

  double s = b_len/a_len;

  //AffineTransform2D sim =  Scaling2D(s) * Rotation2D(r) * Translation2D(t_x, t_y) ;
  AffineTransform2D sim;
  sim.setIdentity();
  sim.scale(s);
  sim.rotate(r);
  sim.translate(Eigen::Vector2d(t_x, t_y));

  //LOG(INFO) << "t: " << t_x << " " << t_y << " r(deg): " << r*RAD2DEG << " s: " << s;
  //LOG(INFO) << sim.matrix();


  if (!ModelIsDegenerate(s, r)){
    // copy 2x3 affine to a 3x3 matrix for output
    orsa::Mat out_model(3,3);

    out_model(0,0) = sim(0,0);
    out_model(0,1) = sim(0,1);
    out_model(0,2) = sim(0,2);
    out_model(1,0) = sim(1,0);
    out_model(1,1) = sim(1,1);
    out_model(1,2) = sim(1,2);
    out_model(2,0) = 0;
    out_model(2,1) = 0;
    out_model(2,2) = 1;

    models->push_back(out_model);
  }
}



////////////////////////////////


ExtendedOrsaAffineModel::ExtendedOrsaAffineModel(const iw::Correspondences& correspondences,
                         const Mat &x1, int w1, int h1,
                         const Mat &x2, int w2, int h2, const ACRansacImageMatcherParams& params) :
 ExtendedOrsaModel(correspondences, x1, w1, h1, x2, w2, h2, params) {
  // for planar models
  logalpha0_[0] = log10(M_PI/(w1*(double)h1) /(N1_(0,0)*N1_(0,0)));
  logalpha0_[1] = log10(M_PI/(w2*(double)h2) /(N2_(0,0)*N2_(0,0)));
}



/// 2D affine estimation from point correspondences.
void ExtendedOrsaAffineModel::Fit(const std::vector<size_t> &indices,
                          std::vector<Model> *models) const {
  if(indices.size() < 3)
    return;
  const size_t n = indices.size();

  double sa[36], sb[6], mm[6];
  cv::Mat A(6, 6, CV_64F, sa);
  cv::Mat B(6, 1, CV_64F, sb);
  cv::Mat MM(6, 1, CV_64F, mm);
  memset(sa, 0, sizeof(sa));
  memset(sb, 0, sizeof(sb));

  for (size_t i = 0; i < n; ++i) {
    size_t index = indices[i];
    double ax = x1_(0, index);
    double ay = x1_(1, index);
    double bx = x2_(0, index);
    double by = x2_(1, index);

    sa[0] += ax*ax;
    sa[1] += ay*ax;
    sa[2] += ax;

    sa[6] += ax*ay;
    sa[7] += ay*ay;
    sa[8] += ay;

    sa[12] += ax;
    sa[13] += ay;
    sa[14] += 1;

    sb[0] += ax*bx;
    sb[1] += ay*bx;
    sb[2] += bx;
    sb[3] += ax*by;
    sb[4] += ay*by;
    sb[5] += by;
  }

  sa[21] = sa[0];
  sa[22] = sa[1];
  sa[23] = sa[2];
  sa[27] = sa[6];
  sa[28] = sa[7];
  sa[29] = sa[8];
  sa[33] = sa[12];
  sa[34] = sa[13];
  sa[35] = sa[14];

  cv::solve(A, B, MM, cv::DECOMP_SVD);

  if (!ModelIsDegenerate(mm)){
    // copy 2x3 affine to a 3x3 matrix for output
    orsa::Mat out_model(3,3);

    out_model(0,0) = mm[0];
    out_model(0,1) = mm[1];
    out_model(0,2) = mm[2];
    out_model(1,0) = mm[3];
    out_model(1,1) = mm[4];
    out_model(1,2) = mm[5];
    out_model(2,0) = 0;
    out_model(2,1) = 0;
    out_model(2,2) = 1;

    models->push_back(out_model);
  }
}

bool ExtendedOrsaAffineModel::ModelIsDegenerate(const double affine_data[6] ) const {
  AffineTransform2D a2d;
  a2d(0,0) = affine_data[0];
  a2d(0,1) = affine_data[1];
  a2d(0,2) = affine_data[2];
  a2d(1,0) = affine_data[3];
  a2d(1,1) = affine_data[4];
  a2d(1,2) = affine_data[5];

  double max_scale = params_.max_scaling();
  double min_scale = 1.0/max_scale;
  double max_shear = 0.25;
  AffineTransform2D::LinearMatrixType rotation, scaling;
  a2d.computeRotationScaling(&rotation, &scaling);
  double scale_x = scaling(0,0);
  double scale_y = scaling(1,1);
  bool is_reflection = (scale_x*scale_y < 0);  // exactly 1 must be negative for a reflection... both negative is just a rotation
  if (is_reflection){
    return true;
  }

  double scale_x_mag = fabs(scale_x);
  double scale_y_mag = fabs(scale_y);
  if (scale_x_mag > max_scale || scale_y_mag > max_scale) {
    return true;
  }

  if (scale_x_mag < min_scale || scale_y_mag < min_scale) {
    return true;
  }

  double shear_mag = std::max(fabs(scaling(0,1)), fabs(scaling(1,0)));
  if (shear_mag > max_shear){
    return true;
  }

  if (params_.has_max_in_plane_rotation()){
    Rotation2D rot(0);
    rot.fromRotationMatrix(rotation);

    if (fabs(rot.angle()) > params_.max_in_plane_rotation()){
      return true;
    }
  }

  return false;
}






}  // namespace orsa
