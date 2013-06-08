#pragma once

#include "iw/iw.pb.h"
#include "iw/matching/cbir/cbir.pb.h"
#include "iw/matching/cbir/full/feature_index.h"
#include "iw/matching/point2d_index.h"
#include "snap/boost/scoped_ptr.hpp"
#include "snap/boost/unordered_map.hpp"
#include "snap/flann/flann.hpp"
#include "snap/google/base/basictypes.h"
#include <map>

namespace cbir {

class ScaleTranslateTransform {
 public:
  // Computes the transform from A->B
  ScaleTranslateTransform(const cbir::Keypoint& a, const cbir::Keypoint& b);

  // Applys the transform to a point in image A, mapping it to image B
  void Apply(const cbir::Keypoint& a, float* x, float* y);

  // Computes the euclidian transfer error when mapping point a under the transform
  float ComputeTransferError(const cbir::Keypoint& a, const cbir::Keypoint& b);

  void Print();
 private:
  float scaling_;
  float dx_;
  float dy_;
};

struct CorrespondenceCandidate {
  CorrespondenceCandidate() {
  }
  CorrespondenceCandidate(int query_feature_index, float score, const cbir::Keypoint& candidate_feature_keypoint) :
    query_feature_index(query_feature_index),
    score(score),
    candidate_feature_keypoint(candidate_feature_keypoint) { }

  int query_feature_index;
  float score;
  cbir::Keypoint candidate_feature_keypoint;
};


// Given a set of candidate correspondences for the query image, find a subset
// of candidates that are consistent with a locally smooth mapping.
class SmoothCorrespondenceFilter {
 public:
  SmoothCorrespondenceFilter(const cbir::ImageFeaturesNeighbors& query_features_neighbors, const cbir::SmoothCorrespondenceFilterParams& params);
  bool Run(const std::vector<CorrespondenceCandidate>& candidates, std::vector<int>* consistent_candidate_ids);

  std::vector<float> GetScores() {return smoothness_scores_; }

 private:
  const cbir::Keypoint& GetQueryKeypoint(int i);
  const cbir::ImageFeaturesNeighbors& query_features_neighbors_;
  SmoothCorrespondenceFilterParams params_;
  std::vector<std::vector<int> > queryfeatureid_to_neighborqueryfeatureids_;
  std::vector<float> smoothness_scores_;
};


class WGCCorrespondenceFilter {
 public:
  WGCCorrespondenceFilter(const cbir::ImageFeaturesNeighbors& query_features_neighbors, const cbir::WGCCorrespondenceFilterParams& params);
  bool Run(const std::vector<CorrespondenceCandidate>& candidates, std::vector<int>* consistent_candidate_ids);

 private:
  double GetCorrespondenceScaling(const CorrespondenceCandidate& candidate);
  float GetScalingForBin(int scaling_histogram_index);
  bool GetScalingHistogramIndex(float scaling, int* index);
  const cbir::ImageFeaturesNeighbors& query_features_neighbors_;
  WGCCorrespondenceFilterParams params_;
  int scaling_histogram_size_;
  float scaling_histogram_bin_size_;
  std::vector< std::vector<int> > scaling_histogram_;
};


class QueryScorer {
 public:
  QueryScorer(const ImageFeatureCountTable& feature_counts, const cbir::QueryScorerParams& params);
  bool Run(const cbir::ImageFeaturesNeighbors& query_neighbors, cbir::QueryResults* results);
  bool TestSmoothnessFilter(const cbir::ImageFeaturesNeighbors& query_features_neighbors, cbir::SmoothnessFilterDebugResults* results);
 private:

  //typedef std::map<uint64, std::vector<CorrespondenceCandidate> > ImageToCandidates;
  typedef boost::unordered_map<uint64, std::vector<CorrespondenceCandidate> > ImageToCandidates;
  void GenerateCorrespondenceCandidates(ImageToCandidates* image_to_candidates,  const cbir::ImageFeaturesNeighbors& query_features_neighbors);
  void KeepTopResults(cbir::QueryResults* results, int num_results);

  typedef ::google::protobuf::RepeatedPtrField< ::cbir::FeatureNeighbor > RepeatedFeatureNeighbor;

  QueryScorerParams params_;
  //const std::map<uint64, int>& imageid_to_numfeatures_;
  const ImageFeatureCountTable& feature_counts_;
};


} // close namespace cbir

