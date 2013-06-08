#include "iw/matching/cbir/cbir.pb.h"
#include "iw/matching/cbir/full/scoring.h"
#include "iw/matching/cbir/full/full.h"
#include "iw/util.h"
#include "snap/boost/filesystem.hpp"
#include "snap/boost/foreach.hpp"
#include "snap/boost/scoped_array.hpp"
#include "snap/boost/timer/timer.hpp"
#include "snap/boost/unordered_map.hpp"
#include "snap/google/base/stringprintf.h"
#include "snap/google/glog/logging.h"
#include "snap/pert/pert.h"
#include "snap/progress/progress.hpp"
#include "snap/scul/scul.hpp"
#include <algorithm>
#include <cfloat>

namespace fs = boost::filesystem;

using namespace std;
using namespace pert;
using namespace iw;
using namespace boost;
using namespace progress;
using namespace scul;

namespace cbir {

////////////////////////////////////////////////////////////////////////////////
// ScaleTranslateTransform
////////////////////////////////////////////////////////////////////////////////

// Computes the transform from A->B
ScaleTranslateTransform::ScaleTranslateTransform(const cbir::Keypoint& a, const cbir::Keypoint& b){
  scaling_ = float(b.radius_100())/a.radius_100();
  dx_ = b.x()/scaling_ - a.x();
  dy_ = b.y()/scaling_ - a.y();
}

// Applys the transform to a point in image A, mapping it to image B
void ScaleTranslateTransform::Apply(const cbir::Keypoint& a, float* x, float* y){
  *x = (a.x()+dx_)*scaling_;
  *y = (a.y()+dy_)*scaling_;
}

// Computes the euclidian transfer error when mapping point a under the transform
float ScaleTranslateTransform::ComputeTransferError(const cbir::Keypoint& a, const cbir::Keypoint& b){
  float x, y;
  Apply(a, &x, &y);
  float x_error = b.x() - x;
  float y_error = b.y() - y;
  return sqrt(x_error*x_error + y_error*y_error);
}

void ScaleTranslateTransform::Print(){
    LOG(INFO) << "scaling: " << scaling_ << " dx: " << dx_ << " dy: " << dy_;
  }

////////////////////////////////////////////////////////////////////////////////
// SmoothCorrespondenceFilter
////////////////////////////////////////////////////////////////////////////////

bool ResultSortComparator(const cbir::QueryResult& r1, const cbir::QueryResult& r2){
  return r1.score() > r2.score();
}


SmoothCorrespondenceFilter::SmoothCorrespondenceFilter(const cbir::ImageFeaturesNeighbors& query_features_neighbors, const SmoothCorrespondenceFilterParams& params) :
  query_features_neighbors_(query_features_neighbors),
  params_(params) {

  iw::FlannPoint2DIndex point_index;
  // build FLANN index over query point locations
  BOOST_FOREACH(const FeatureNeighbors& feature, query_features_neighbors_.features()){
    CHECK(feature.has_keypoint());
    const cbir::Keypoint& keypoint = feature.keypoint();
    point_index.AddPoint(keypoint.x(), keypoint.y());
  }

  point_index.SelfRadiusSearch(params.search_radius(), &queryfeatureid_to_neighborqueryfeatureids_);
}



float ComputeSmoothnessScore(const cbir::Keypoint& c1a, const cbir::Keypoint& c1b, const cbir::Keypoint& c2a, const cbir::Keypoint& c2b){
  ScaleTranslateTransform t1(c1a, c1b);
  ScaleTranslateTransform t2(c2a, c2b);
  return (t1.ComputeTransferError(c2a, c2b) + t2.ComputeTransferError(c1a, c1b))*0.5;
}


int RadiusToQuantizedRadius(float radius){
  int bin_size = 5;
  return int(log10(radius)/log10(2.0));
}


bool SmoothCorrespondenceFilter::Run(const std::vector<CorrespondenceCandidate>& candidates, std::vector<int>* consistent_candidate_ids){
  CHECK(consistent_candidate_ids);
  consistent_candidate_ids->clear();
  smoothness_scores_.clear();
  typedef std::vector<int> CorrespondenceCandidateIds;
  typedef boost::unordered_map<int, CorrespondenceCandidateIds> FeatureIndexToCorrespondenceIds;

  FeatureIndexToCorrespondenceIds queryfeatureid_to_correspondenceids;
  // build mapping of query feature index to sets of associated correspondences (all correspondences that have use that feature in query image)
  for (int i=0; i < candidates.size(); ++i){
    queryfeatureid_to_correspondenceids[candidates[i].query_feature_index].push_back(i);
  }

  // for each candidate
  for (int candidate1_id=0; candidate1_id < candidates.size(); ++candidate1_id){
    const CorrespondenceCandidate& candidate1 = candidates[candidate1_id];
    const cbir::Keypoint& c1a = GetQueryKeypoint(candidate1.query_feature_index);
    const cbir::Keypoint& c1b = candidate1.candidate_feature_keypoint;

    // query FLANN to find other candidates in neighborhood
    //std::vector<int> neighbor_indices;
    //std::vector<float> neighbor_distances;


    //CHECK(point_index_.RadiusSearch(c1a.x(), c1a.y(), params_.search_radius(), &neighbor_indices, &neighbor_distances));
    const std::vector<int>& neighbor_indices = queryfeatureid_to_neighborqueryfeatureids_.at(candidate1.query_feature_index);

    // for each other candidate in neighborhood
    BOOST_FOREACH(int neighbor_index, neighbor_indices){
      // The within-fixed-radius relationship is symmetric... only need to validate pairs from the lower-triangle of the all pairs matrix
      if (neighbor_index >= candidate1.query_feature_index){
        continue;
      }
      // This point is not associated with a correspondence, skip it
      // reason: The index was built over all possible correspondences but this call to Run() may only relate to a subset of them
      CorrespondenceCandidateIds correspondences;
      if (!Find(queryfeatureid_to_correspondenceids, neighbor_index, &correspondences)){
        continue;
      }
      const cbir::Keypoint& c2a = GetQueryKeypoint(neighbor_index);
      BOOST_FOREACH(int candidate2_id, correspondences){
        const CorrespondenceCandidate& candidate2 = candidates[candidate2_id];
        const cbir::Keypoint& c2b = candidate2.candidate_feature_keypoint;
        // compute the smoothness score

        float smoothness = ComputeSmoothnessScore(c1a, c1b, c2a, c2b);
        smoothness_scores_.push_back(smoothness);


        if (smoothness < 100){
          consistent_candidate_ids->push_back(candidate1_id);
          consistent_candidate_ids->push_back(candidate2_id);
        }
      }
    }
  }

  MakeUnique(consistent_candidate_ids);
  Sort(&smoothness_scores_);
  return true;
}

const cbir::Keypoint& SmoothCorrespondenceFilter::GetQueryKeypoint(int index){
  return query_features_neighbors_.features(index).keypoint();
}

/*
bool SmoothCorrespondenceFilter::Run(const std::vector<CorrespondenceCandidate>& candidates, std::vector<int>* consistent_candidate_ids){
  smoothness_scores_.clear();
  typedef std::vector<int> CorrespondenceCandidateIds;
  typedef boost::unordered_map<int, CorrespondenceCandidateIds> FeatureIndexToCorrespondenceIds;

  FeatureIndexToCorrespondenceIds queryfeatureid_to_correspondenceids;
  // build mapping of query feature index to sets of associated correspondences (all correspondences that have use that feature in query image)
  for (int i=0; i < candidates.size(); ++i){
    queryfeatureid_to_correspondenceids[candidates[i].query_feature_index].push_back(i);
  }

  // for each candidate
  for (int candidate1_id=0; candidate1_id < candidates.size(); ++candidate1_id){
    const CorrespondenceCandidate& candidate1 = candidates[candidate1_id];
    const cbir::Keypoint& c1a = query_keypoints_.entries(candidate1.query_feature_index);
    const cbir::Keypoint& c1b = candidate1.candidate_feature_keypoint;

    // query FLANN to find other candidates in neighborhood
    std::vector<int> neighbor_indices;
    std::vector<float> neighbor_distances;
    CHECK(point_index_.RadiusSearch(c1a.x(), c1a.y(), params_.search_radius(), &neighbor_indices, &neighbor_distances));
    // for each other candidate in neighborhood
    BOOST_FOREACH(int neighbor_index, neighbor_indices){
      // The within-fixed-radius relationship is symmetric... only need to validate pairs from the lower-triangle of the all pairs matrix
      if (neighbor_index >= candidate1.query_feature_index){
        continue;
      }
      // This point is not associated with a correspondence, skip it
      // reason: The index was built over all possible correspondences but this call to Run() may only relate to a subset of them
      CorrespondenceCandidateIds correspondences;
      if (!iw::Find(queryfeatureid_to_correspondenceids, neighbor_index, &correspondences)){
        continue;
      }
      const cbir::Keypoint& c2a = query_keypoints_.entries(neighbor_index);
      BOOST_FOREACH(int candidate2_id, correspondences){
        const CorrespondenceCandidate& candidate2 = candidates[candidate2_id];
        const cbir::Keypoint& c2b = candidate2.candidate_feature_keypoint;
        // compute the smoothness score

        float smoothness = ComputeSmoothnessScore(c1a, c1b, c2a, c2b);
        smoothness_scores_.push_back(smoothness);

        if (smoothness < 150){
          consistent_candidate_ids->push_back(candidate1_id);
          consistent_candidate_ids->push_back(candidate2_id);
        }
      }
    }
  }


  std::MakeUnique(consistent_candidate_ids->begin());
  iw:Sort(&smoothness_scores_);

  return true;
}
*/



////////////////////////////////////////////////////////////////////////////////
// WGCCorrespondenceFilter
////////////////////////////////////////////////////////////////////////////////



WGCCorrespondenceFilter::WGCCorrespondenceFilter(const cbir::ImageFeaturesNeighbors& query_features_neighbors, const WGCCorrespondenceFilterParams& params) :
  query_features_neighbors_(query_features_neighbors),
  params_(params) {
  if (params_.type() == WGCCorrespondenceFilterParams::SCALE ||
      params_.type() == WGCCorrespondenceFilterParams::SCALE_AND_TRANSLATION) {
    CHECK(params_.has_scale_max());
    CHECK(params_.has_scale_histogram_size());
    scaling_histogram_size_ = params.has_scale_histogram_size();
    double scaling_histogram_max = log2(params.scale_max());
    scaling_histogram_bin_size_ = 2.0*scaling_histogram_max/scaling_histogram_size_;
  }
}

double WGCCorrespondenceFilter::GetCorrespondenceScaling(const CorrespondenceCandidate& candidate){
  double radius_b_100 = candidate.candidate_feature_keypoint.radius_100();
  double radius_a_100 = query_features_neighbors_.features(candidate.query_feature_index).keypoint().radius_100();
  double scaling = radius_b_100/radius_a_100;
  return scaling;
}

bool WGCCorrespondenceFilter::Run(const std::vector<CorrespondenceCandidate>& candidates, std::vector<int>* consistent_candidate_ids){
  CHECK(consistent_candidate_ids);
  consistent_candidate_ids->clear();

  // When using a scale constraint, select all votes consistent with best scale.
  if (params_.type() == WGCCorrespondenceFilterParams::SCALE ||
      params_.type() == WGCCorrespondenceFilterParams::SCALE_AND_TRANSLATION) {
    // Compute histogram of vote scales
    scaling_histogram_.clear();
    scaling_histogram_.resize(scaling_histogram_size_);
    bool has_valid_scaling = false;
    for (int candidate_id=0; candidate_id < candidates.size(); ++candidate_id){
      int index;
      double scaling = GetCorrespondenceScaling(candidates[candidate_id]);
      if (GetScalingHistogramIndex(scaling, &index)){
        scaling_histogram_[index].push_back(candidate_id);
        has_valid_scaling = true;
      }
    }

    if (!has_valid_scaling){
      return true;
    }

    // Find scale histogram bin with most number of votes.
    int best_scale_index = 0;
    int best_num_votes = 0;
    for (int i=0; i < scaling_histogram_.size(); ++i){
      int num_votes = scaling_histogram_[i].size();
      if (num_votes > best_num_votes){
        best_scale_index = i;
        best_num_votes = num_votes;
      }
    }

    // Also select the votes in the bins to left and right of best scale bin.
    CHECK_GE(best_scale_index, 0);
    CHECK_LT(best_scale_index, scaling_histogram_size_);
    double scaling = GetScalingForBin(best_scale_index);
    int min_index = std::max(0, best_scale_index - 1);
    int max_index = std::min(scaling_histogram_size_-1, best_scale_index + 1);

    for (int index = min_index; index <= max_index; ++index ){
      BOOST_FOREACH(int candidate_id, scaling_histogram_[index]){
        consistent_candidate_ids->push_back(candidate_id);
      }
    }
  }

  // When using a scale + translation constraint, select only votes consistent with best translation at selected best scale.
  if (params_.type() == WGCCorrespondenceFilterParams::SCALE_AND_TRANSLATION) {
    LOG(FATAL) << "not yet implemented";
  }

  return true;
}


float WGCCorrespondenceFilter::GetScalingForBin(int scaling_histogram_index){
  return pow(2.0, scaling_histogram_bin_size_*(scaling_histogram_index+0.5-scaling_histogram_size_/2.0));
}

bool WGCCorrespondenceFilter::GetScalingHistogramIndex(float scaling, int* index) {
  int i = log2(scaling)/scaling_histogram_bin_size_ + scaling_histogram_size_/2.0;
  if (i >= 0 && i < scaling_histogram_size_){
    *index = i;
    return true;
  }
  return false;
}


////////////////////////////////////////////////////////////////////////////////
// QueryScorer
////////////////////////////////////////////////////////////////////////////////


QueryScorer::QueryScorer(const ImageFeatureCountTable& feature_counts, const cbir::QueryScorerParams& params) :
  params_(params),
  feature_counts_(feature_counts)
{
}


bool QueryScorer::TestSmoothnessFilter(const cbir::ImageFeaturesNeighbors& query_features_neighbors, cbir::SmoothnessFilterDebugResults* results){
  CHECK(results);
  ImageToCandidates image_to_candidates;
  GenerateCorrespondenceCandidates(&image_to_candidates, query_features_neighbors);

  // Select votes according to mapping smoothness constraint.
  SmoothCorrespondenceFilter smooth_filter(query_features_neighbors, params_.smooth_correspondence_filter_params());
  BOOST_FOREACH(ImageToCandidates::value_type& v, image_to_candidates){
    uint64 image_b_id = v.first;
    const vector<CorrespondenceCandidate>& candidates = v.second;
    vector<int> consistent_candidate_ids;
    smooth_filter.Run(candidates, &consistent_candidate_ids);

    cbir::SmoothnessFilterDebugResult* result = results->add_entries();
    result->set_image_id(image_b_id);

    BOOST_FOREACH(float value, smooth_filter.GetScores()){
      result->add_smoothness_scores(value);
    }
  }
  return true;
}




bool QueryScorer::Run(const cbir::ImageFeaturesNeighbors& query_features_neighbors, cbir::QueryResults* results){
  boost::timer::auto_cpu_timer t("query scorer time: %ws sec \n");
  CHECK(results);
  ImageToCandidates image_to_candidates;
  GenerateCorrespondenceCandidates(&image_to_candidates, query_features_neighbors);

  boost::scoped_ptr<SmoothCorrespondenceFilter> smooth_filter;
  boost::scoped_ptr<WGCCorrespondenceFilter> wgc_filter;

  float num_features_a = query_features_neighbors.features_size();
  results->Clear();

  BOOST_FOREACH(ImageToCandidates::value_type& v, image_to_candidates){
    uint64 image_b_id = v.first;
    const vector<CorrespondenceCandidate>& candidates = v.second;
    vector<int> consistent_candidate_ids;

    double score = 0.0;

    if (params_.correspondence_filter() == QueryScorerParams::NO_FILTER ){
      for(int candidate_id=0; candidate_id < candidates.size(); ++candidate_id){
        score += candidates[candidate_id].score;
      }
    }
    else if (params_.correspondence_filter() == QueryScorerParams::SMOOTH_FILTER ) {
      if (!smooth_filter.get()){
        smooth_filter.reset(new SmoothCorrespondenceFilter(query_features_neighbors, params_.smooth_correspondence_filter_params()));
      }
      CHECK(params_.has_smooth_correspondence_filter_params());
      CHECK(smooth_filter->Run(candidates, &consistent_candidate_ids));
      // Compute the score as sum of all selected vote scores.
      BOOST_FOREACH(int candidate_id, consistent_candidate_ids){
        score += candidates[candidate_id].score;
      }
    }
    else if (params_.correspondence_filter() == QueryScorerParams::WGC_FILTER ) {
      if (!wgc_filter.get()){
        wgc_filter.reset(new WGCCorrespondenceFilter(query_features_neighbors, params_.wgc_correspondence_filter_params()));
      }
      CHECK(params_.has_wgc_correspondence_filter_params());
      CHECK(wgc_filter->Run(candidates, &consistent_candidate_ids));
      // Compute the score as sum of all selected vote scores.
      BOOST_FOREACH(int candidate_id, consistent_candidate_ids){
        score += candidates[candidate_id].score;
      }
    }
    else{
      LOG(FATAL) << "must set correspondence_filter properly";
    }

    if (score == 0.0){
      continue;
    }

    // Apply requested normalization
    float num_features_b = feature_counts_.GetCountOrDie(image_b_id);
    if (params_.normalization() == QueryScorerParams::NONE){
    }
    else if (params_.normalization() == QueryScorerParams::NRC){
      score *= 1.0/(sqrt(num_features_a)*sqrt(num_features_b));
    }
    else{
      LOG(FATAL) << "invalid normalization type:" << params_.normalization() ;
    }
    cbir::QueryResult* result = results->add_entries();
    result->set_image_id(image_b_id);
    result->set_score(score);
  }

  KeepTopResults(results, params_.max_results());
  return true;
}

// Order decreasing by score and truncate
void QueryScorer::KeepTopResults(cbir::QueryResults* results,  int num_results) {
  CHECK(results);
  CHECK_GE(num_results, 1);
  Sort(results->mutable_entries(), ResultSortComparator);
  Truncate(results->mutable_entries(), num_results);
}

// order increasing by feature index... where feature index is same... order decreasing by score
bool CorrespondenceCandidateOrderComparator(const CorrespondenceCandidate& c1, const CorrespondenceCandidate& c2){
  if (c1.query_feature_index < c2.query_feature_index ){
    return true;
  }
  else if (c1.query_feature_index == c2.query_feature_index ){
    return c1.score > c2.score;
  }
  return false;
}

bool CorrespondenceCandidateEqualityComparator(const CorrespondenceCandidate& c1, const CorrespondenceCandidate& c2){
  return c1.query_feature_index == c2.query_feature_index;
}


void QueryScorer::GenerateCorrespondenceCandidates(ImageToCandidates* image_to_candidates, const cbir::ImageFeaturesNeighbors& query_features_neighbors){

  // Generate votes using specified score weighting method.
  int num_neighbors = -1; // the number
  //BOOST_FOREACH(const cbir::FeatureNeighbors& feature_knn, IterConst(query_neighbors.feature())){
  for (int query_feature_index = 0; query_feature_index < query_features_neighbors.features_size(); ++query_feature_index ){
    const FeatureNeighbors& neighbors = query_features_neighbors.features(query_feature_index);
    num_neighbors = (num_neighbors < 0) ? neighbors.entries_size() : num_neighbors;
    CHECK_EQ(neighbors.entries_size(), num_neighbors); // Verify we recieve the same num of neighbors for each feature in image
    float neighbor_k_distance = neighbors.entries(num_neighbors-1).distance();

    BOOST_FOREACH(const cbir::FeatureNeighbor& neighbor, neighbors.entries()){
      float score = 0.0f;
      if (params_.scoring() == QueryScorerParams::COUNT){
        score = 1.0;
      }
      else if (params_.scoring() == QueryScorerParams::ADAPTIVE){
        score = std::max(neighbor_k_distance - neighbor.distance(), 0.0f);
      }
      else{
        LOG(FATAL) << "invalid scoring type:" << params_.scoring() ;
      }
      (*image_to_candidates)[neighbor.image_id()].push_back(CorrespondenceCandidate(query_feature_index, score, neighbor.keypoint()));
    }
  }

  if (!params_.bursty_candidates()){
    // ensure a feature votes at most once for this image
    BOOST_FOREACH(ImageToCandidates::value_type v, *image_to_candidates){
      std::vector<CorrespondenceCandidate>& candidates = v.second;
      MakeUnique(&candidates, CorrespondenceCandidateOrderComparator, CorrespondenceCandidateEqualityComparator);
    }
  }


}

/////////////////////////////


} // close namespace cbir
