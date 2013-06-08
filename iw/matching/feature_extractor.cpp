#include "iw/matching/feature_extractor.h"
#include "snap/opencv2/core/core.hpp"
#include "snap/opencv2/highgui/highgui.hpp"
#include "snap/boost/timer.hpp"
#include "snap/boost/foreach.hpp"
#include "iw/iw.pb.h"

namespace iw {

FeatureExtractor* CreateFeatureExtractorOrDie(const iw::FeatureExtractorParams& params){
  params.CheckInitialized();
  // switch on the optional proto fields to decide which kind of feature extractor to create
  if (params.has_ocv_sift_params()){
    return new OcvSiftFeatureExtractor(params.ocv_sift_params());
  }
  else if (params.has_vgg_affine_sift_params()){
    return new VggAffineSiftFeatureExtractor(params.vgg_affine_sift_params());
  }
  else{
    LOG(ERROR) << "unknown feature type:" << params.DebugString();
  }
  return NULL;
}

#define DEG2RAD 0.0174532925
#define SIFT_DESCR_SCL_FCTR 3.0  // copied from opencv's sift.cpp is there a better way to get at it?
#define OPENCV_SIFT_SIZE_TO_SIFT_RADIUS_SCALING SIFT_DESCR_SCL_FCTR/2.0 // opencv reports size as diameter of detected scale before scaling to keypoint descriptor region size

// This transformation greatly improves sift matching under L2 distance
// See: http://www.robots.ox.ac.uk/~vgg/publications/2012/Arandjelovic12/arandjelovic12.pdf
void ApplyRootSiftTransform(iw::ImageFeatures* features){
  BOOST_FOREACH(iw::FeatureDescriptor& descriptor, *features->mutable_descriptors()){
    std::string& data = *descriptor.mutable_data();
    CHECK_EQ(data.size(), 128);
    float l1_norm = 0;
    for (int i=0; i < 128; ++i) {
      l1_norm += (uchar)data[i];
    }
    float scaling = 1.0/std::max(l1_norm, FLT_EPSILON);
    for (int i=0; i < 128; ++i) {
      data[i] = cv::saturate_cast<uchar>(512.0f*sqrt((uchar)data[i]*scaling));
    }
  }
}

OcvSiftFeatureExtractor::OcvSiftFeatureExtractor(const iw::FeatureExtractorParams::OcvSiftParams& params) :
  params_(params),
  sift_(0, params_.num_octave_layers(), params_.contrast_threshold(),
        params_.edge_threshold(), params_.sigma(), params_.upright())
{
  CHECK(params.IsInitialized()) << params.InitializationErrorString();
}

bool OcvSiftFeatureExtractor::Run(const std::string& image_data, iw::ImageFeatures* features){
  CHECK(image_data.size());
  CHECK(features);
  features->Clear();
  boost::timer timer;
  cv::Mat image_bytes(1, image_data.size(), CV_8U, (void*)image_data.data()); // TODO: is there a better way without casting away constness?
  cv::Mat img = cv::imdecode(image_bytes, 0); // force load as grayscale

  if ( !(img.size().width && img.size().height) ){
    LOG(INFO) << "failed to decode image";
    return false;
  }

  std::vector<cv::KeyPoint> keypoints;
  cv::Mat descriptors;
  sift_(img, cv::noArray(), keypoints, descriptors);
  LOG(INFO) << keypoints.size();

  double prev_keypoint_response = 0;
  for (int i=0; i < keypoints.size();++i){
    // check invariant: keypoints are listed most important keypoint first
    CHECK_GE(keypoints[i].response, prev_keypoint_response);
    prev_keypoint_response = keypoints[i].response;

    iw::FeatureKeypoint* new_keypoint = features->add_keypoints();
    new_keypoint->set_angle(keypoints[i].angle*DEG2RAD); // opencv angle is in degress convert rad
    new_keypoint->set_radius(keypoints[i].size*OPENCV_SIFT_SIZE_TO_SIFT_RADIUS_SCALING);
    new_keypoint->mutable_pos()->set_x(keypoints[i].pt.x);
    new_keypoint->mutable_pos()->set_y(keypoints[i].pt.y);

    //LOG(INFO) << new_keypoint->DebugString();
    iw::FeatureDescriptor* new_desc = features->add_descriptors();
    std::string* desc_data = new_desc->mutable_data();
    desc_data->resize(128);

    for (int j=0; j<128; ++j) {
      desc_data->at(j) = descriptors.at<float>(i,j);
    }
  }
  features->set_width(img.cols);
  features->set_height(img.rows);
  features->set_extraction_time(timer.elapsed());
  if (params_.root_sift_normalization()){
    ApplyRootSiftTransform(features);
  }
  return true;
}


VggAffineSiftFeatureExtractor::VggAffineSiftFeatureExtractor(const iw::FeatureExtractorParams::VggAffineSiftParams& params) :
    params_(params),
    vgg_(VGGFeatureExtractor::FeatureType(params_.type()), params_.threshold()){
  CHECK(params.IsInitialized()) << params.InitializationErrorString();
}

bool VggAffineSiftFeatureExtractor::Run(const std::string& image_data, iw::ImageFeatures* features){
  CHECK(image_data.size());
  CHECK(features);
  features->Clear();
  boost::timer timer;
  bool status = vgg_.ExtractFromData(image_data, features);
  LOG(INFO) << features->keypoints().size();
  features->set_extraction_time(timer.elapsed());
  if (params_.root_sift_normalization()){
    ApplyRootSiftTransform(features);
  }
  return status;
}

/*
bool MserFeatureExtractor::Run(const std::string& image_data, iw::ImageFeatures* features){
  CHECK(image_data.size());
  CHECK(features);
  features->Clear();
  boost::timer timer;

  cv::Mat image_bytes(1, image_data.size(), CV_8U, (void*)image_data.data()); // TODO: is there a better way without casting away constness?
  cv::Mat img = cv::imdecode(image_bytes, CV_LOAD_IMAGE_COLOR); // force load as color (

  if ( !(img.size().width && img.size().height) ){
    LOG(INFO) << "failed to decode image";
    return false;
  }

  // find keypoints
  std::vector<cv::KeyPoint> keypoints;
  mser_detector_.detect(img, keypoints);
  LOG(INFO) << keypoints.size();

  // compute descriptors
  cv::Mat descriptors;
  bool use_provided_keypoints = true;
  sift_(img, cv::noArray(), keypoints, descriptors, use_provided_keypoints);

  for (int i=0; i < keypoints.size();++i){
    iw::FeatureKeypoint* new_keypoint = features->add_keypoints();
    //CHECK_EQ(keypoints[i].angle, 0); // upright invariant
    //new_keypoint->set_angle(0);
    new_keypoint->set_angle(keypoints[i].angle);
    new_keypoint->set_radius(keypoints[i].size*OPENCV_SIFT_SIZE_TO_SIFT_RADIUS_SCALING);
    new_keypoint->mutable_pos()->set_x(keypoints[i].pt.x);
    new_keypoint->mutable_pos()->set_y(keypoints[i].pt.y);
    //LOG(INFO) << new_keypoint->DebugString();
    iw::FeatureDescriptor* new_desc = features->add_descriptors();
    std::string* desc_data = new_desc->mutable_data();
    desc_data->resize(128);
    for (int j=0; j<128; ++j){
      desc_data->at(j) = descriptors.at<float>(i,j);
    }
  }
  features->set_width(img.cols);
  features->set_height(img.rows);
  features->set_extraction_time(timer.elapsed());
  ApplyRootSiftTransform(features);
  return true;
}



bool DenseFeatureExtractor::Run(const std::string& image_data, iw::ImageFeatures* features){
  CHECK(image_data.size());
  CHECK(features);
  features->Clear();
  boost::timer timer;

  cv::Mat image_bytes(1, image_data.size(), CV_8U, (void*)image_data.data()); // TODO: is there a better way without casting away constness?
  cv::Mat img = cv::imdecode(image_bytes, 0); // force load as gray
  CHECK(img.size().width && img.size().height) << "failed to decode image";

  // find keypoints
  std::vector<cv::KeyPoint> keypoints;
  dense_detector_.detect(img, keypoints);
  LOG(INFO) << keypoints.size();

  // compute descriptors
  cv::Mat descriptors;
  bool use_provided_keypoints = true;
  sift_(img, cv::noArray(), keypoints, descriptors, use_provided_keypoints);

  for (int i=0; i < keypoints.size();++i){
    iw::FeatureKeypoint* new_keypoint = features->add_keypoints();
    //CHECK_EQ(keypoints[i].angle, 0); // upright invariant
    new_keypoint->set_angle(0);
    new_keypoint->set_radius(keypoints[i].size*OPENCV_SIFT_SIZE_TO_SIFT_RADIUS_SCALING);
    new_keypoint->mutable_pos()->set_x(keypoints[i].pt.x);
    new_keypoint->mutable_pos()->set_y(keypoints[i].pt.y);
    //LOG(INFO) << new_keypoint->DebugString();
    iw::FeatureDescriptor* new_desc = features->add_descriptors();
    std::string* desc_data = new_desc->mutable_data();
    desc_data->resize(128);
    for (int j=0; j<128; ++j){
      desc_data->at(j) = descriptors.at<float>(i,j);
    }
  }
  features->set_width(img.cols);
  features->set_height(img.rows);
  features->set_extraction_time(timer.elapsed());
  ApplyRootSiftTransform(features);
  return true;
}
*/



}; //close namespace iw
