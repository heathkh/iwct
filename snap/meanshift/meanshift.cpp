/*
 * meanshift.cpp
 *
 *  Created on: Oct 27, 2011
 *      Author: heathkh
 *
 *  Based on Python implementation in sklearn
 *
 *  TODO(kheath): remove dependency on opencv2
 */

#include "snap/meanshift/meanshift.h"
#include <algorithm>
#include <ext/algorithm>
#include "snap/opencv2/flann/flann.hpp"
#include "snap/opencv2/core/core.hpp"
#include "snap/google/glog/logging.h"
#include "snap/google/base/integral_types.h"

using namespace std;
using namespace __gnu_cxx;

namespace meanshift {

// Estimate the bandwith to use with MeanShift algorithm
bool EstimateBandwidth(const cv::Mat& data, double quantile, int num_samples, double* bandwidth){
  // Check pre-conditions
  CHECK_GT(quantile, 0.0);
  CHECK_LT(quantile, 1.0);

  uint32 num_data = data.rows;
  uint32 dim = data.cols;

  if (num_data < 2){
    return false;
  }

  if (num_samples > num_data){
    num_samples = num_data;
  }

  int knn = num_samples*quantile;
  if (knn < 2){
    return false;
  }

  // sample data
  std::vector<uint32> index_range(num_data);
  for (uint32 i=0; i < index_range.size(); ++i) {
    index_range[i] = i;
  }

  vector<uint32> sampled_indices(num_samples);
  random_sample(index_range.begin(), index_range.end(), sampled_indices.begin(),
      sampled_indices.end());

  CHECK_EQ(sampled_indices.size(), num_samples);

  // copy to data matrix
  cv::Mat sampled_data(num_samples, dim, CV_32F);

  for (uint32 i =0; i < num_samples; ++i){
    cv::Mat tmp_row_header = sampled_data.row(i);
    uint32 sample_index = sampled_indices[i];
    CHECK_LT(sample_index, num_data);
    data.row(sample_index).copyTo(tmp_row_header);
  }

  // for each sample, find the dist to quantile fraction nearest neigbors
  // find mean of furtherest nn
  cv::flann::GenericIndex< ::cvflann::L2<float> > ann(sampled_data, cvflann::LinearIndexParams());

  CHECK_LT(knn, num_samples);
  std::vector<int> indices(knn);
  std::vector<float> dists(knn);

  double sum = 0;
  cvflann::SearchParams search_params;
  for (int i=0;  i < num_samples; ++i){
    cv::Mat query = sampled_data.row(i);
    ann.knnSearch(query, indices, dists, knn, search_params);
    sum += dists[knn-1];
  }
  *bandwidth = sum/double(num_samples);
  return true;
}


cv::Mat mean(const cv::Mat& data, const std::vector<int>& indices, int num_indices){
  uint32 dim = data.cols;
  cv::Mat cur_mean = cv::Mat::zeros(1, dim, CV_32F);
  CHECK_LE(num_indices, (int)indices.size());

  for (int i=0; i < num_indices; ++i){
    int selected_row = indices[i];
    CHECK_LT(selected_row, indices.size());
    cur_mean += data.row(selected_row);
  }

  cur_mean = cur_mean*(1.0/float(num_indices));
  return cur_mean;
}

class CenterCandidate {
public:
  CenterCandidate(cv::Mat center, int size) : center(center), size(size), is_unique(true) { }
  cv::Mat center;
  int size;
  bool is_unique;
};


bool comp(const CenterCandidate& a, const CenterCandidate& b) { return (a.size > b.size); }

// Run mean shift clustering with a flat kernel
bool GenerateClusterLabels(const cv::Mat& data, float bandwidth, cv::Mat* centers, std::vector<uint32>* labels){
  int max_iterations = 300;
  uint32 num_points = data.rows;
  uint32 dim = data.cols;

  cvflann::SearchParams search_params;

  double stop_tresh = 1e-3*bandwidth; // termination criteria

  std::vector<CenterCandidate> center_candidates;

  //cv::flann::Index_<float> seed_ann(data, cvflann::LinearIndexParams());
  cv::flann::GenericIndex< ::cvflann::L2<float> > seed_ann(data, cvflann::LinearIndexParams());

  std::vector<uint32> seed_indices;
  //TODO(kheath): implement binning to select seed indices instead of using all data oints as seeds
  for (int i=0; i < num_points; ++i){
    seed_indices.push_back(i);
  }

  // For each seed, clib gradient until convergences  or max_iterations

  std::vector<int> indices(num_points);
  std::vector<float> dists(num_points);
  for (int i = 0; i < seed_indices.size(); ++i){
   int completed_iterations = 0;
   cv::Mat prev_mean = cv::Mat::zeros(1, dim, CV_32F);
   cv::Mat cur_mean = cv::Mat::zeros(1, dim, CV_32F);
   while(true){
     // Find mean of points inside bandwidth distance
     cv::Mat query = data.row(i);

     int num_points_in_radius = seed_ann.radiusSearch(query, indices, dists, bandwidth, search_params);
     if (num_points_in_radius == 0){
       break; // this may happen, depending on seeding strategy
     }

     CHECK_LE(num_points_in_radius, indices.size());

     prev_mean = cur_mean;
     cur_mean = mean(data, indices, num_points_in_radius);

     // if converged or at max_iterations, add the cluster
     bool converged = cv::norm(cur_mean - prev_mean) < stop_tresh;
     if (converged || completed_iterations == max_iterations){
       center_candidates.push_back(CenterCandidate(cur_mean, num_points_in_radius));
       break;
     }
     completed_iterations++;
   }
  }

  // post processing: remove near duplicate points
  // if the distance between two kernels is less than the bandwidth, one is a duplicate.  remove the one with fewer points.

  std::sort(center_candidates.begin(), center_candidates.end(), comp);

  cv::Mat center_candidates_data(center_candidates.size(), dim, CV_32F);
  for (int i=0; i < center_candidates.size(); ++i){
    cv::Mat tmp_row_header = center_candidates_data.row(i);
    center_candidates[i].center.copyTo(tmp_row_header);
  }

  //cv::flann::Index_<float> center_candidates_ann(center_candidates_data, cvflann::LinearIndexParams());
  cv::flann::GenericIndex< ::cvflann::L2<float> > center_candidates_ann(center_candidates_data, cvflann::LinearIndexParams());


  for (int i=0; i < center_candidates.size(); ++i){
    CenterCandidate& center_candidate = center_candidates[i];

    if (center_candidate.is_unique){
      int num_points_in_radius = center_candidates_ann.radiusSearch(center_candidate.center, indices, dists, bandwidth, search_params);
      for (int j=0; j < num_points_in_radius; ++j){
        int supress_index = indices[j];
        if (supress_index > i){
          center_candidates[supress_index].is_unique = false;
        }
      }
    }
  }

  std::vector<uint32> center_indices;
  for (uint32 i=0; i < center_candidates.size(); ++i){
    if (center_candidates[i].is_unique){
      center_indices.push_back(i);
    }
  }

  // select cluster centers

  cv::Mat center_data(center_indices.size(), dim, CV_32F);
  for (int i=0; i < center_indices.size(); ++i){
    cv::Mat tmp_row_header = center_data.row(i);
    center_candidates[center_indices[i]].center.copyTo(tmp_row_header);
  }

  //cv::flann::Index_<float> center_ann(center_data, cvflann::LinearIndexParams());

  cv::flann::GenericIndex< ::cvflann::L2<float> > center_ann(center_data, cvflann::LinearIndexParams());

  labels->resize(num_points);
  for (int i=0; i < num_points; ++i){
    cv::Mat query = data.row(i);
    center_ann.knnSearch(query, indices, dists, 1, search_params);
    uint32 label = indices[0];
    labels->at(i) = label;
  }

  *centers = center_data;

  return true;
}

} // close namespace meanshift
