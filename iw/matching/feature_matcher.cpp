#include "iw/matching/feature_matcher.h"
#include "snap/google/glog/logging.h"
#include "snap/boost/timer.hpp"
#include <algorithm>
#include <iostream>
#include <set>
#include <iostream>
using namespace std;

namespace iw {

FlannFeatureIndex::FlannFeatureIndex(const ::google::protobuf::RepeatedPtrField< ::iw::FeatureDescriptor >& descriptors) {
  num_rows_ = descriptors.size();
  num_cols_ = 128;

  dataset_ = flann::Matrix<unsigned char>(new unsigned char[num_cols_*num_rows_], num_rows_, num_cols_);
  for (int i=0; i < num_rows_; ++i){
    const std::string& desc = descriptors.Get(i).data();
    // need to copy one-by-one because string entries may not be contiguous in memory
    unsigned char* entry = dataset_[i];
    for (int j=0; j < num_cols_; ++j){
      entry[j] = desc[j];
    }
  }

 //#define AUTOTUNE
  #ifdef AUTOTUNE
  index_params_ = flann::AutotunedIndexParams(0.70, // target_precision
                             0.01, //  build_weight
                             0, // memory_weight
                             1.0); // sample_fraction

  flann::Logger::setLevel(flann::FLANN_LOG_INFO);
  #else


  // 95%
  index_params_["algorithm"] = flann::FLANN_INDEX_KMEANS;
  index_params_["branching"] = 16;
  index_params_["iterations"] = 1;
  index_params_["centers_init"] = flann::FLANN_CENTERS_RANDOM;
  index_params_["cb_index"] = 0.2f;

  search_params_.checks = 50;
  search_params_.eps = 0;
  search_params_.sorted = 1;
  search_params_.max_neighbors = -1;
  search_params_.cores = -1;


/*
  // 70%
  index_params_["algorithm"] = flann::FLANN_INDEX_KDTREE;
  index_params_["trees"] = 1;
  search_params_.checks = 48;
  search_params_.cores = -1;
*/

/*
  // 10%
  index_params_["algorithm"] = flann::FLANN_INDEX_KDTREE;
  index_params_["trees"] = 4;
  search_params_.checks = 20;
  search_params_.cores = -1;
  */
#endif

  index_.reset(new Index(dataset_, index_params_));
  index_->buildIndex();

  #ifdef AUTOTUNE
  flann::print_params(index_->getParameters());
  exit(1);
  #endif
}

FlannFeatureIndex::~FlannFeatureIndex(){
  delete [] dataset_.ptr();
}

bool FlannFeatureIndex::GetNeighbors(const FeatureDescriptor& query_descriptor, int num_neighbors, std::vector<int>* neighbor_indices, std::vector<float>* neighbor_distances ){
  CHECK(neighbor_indices);
  CHECK(neighbor_distances);
  neighbor_indices->resize(num_neighbors);
  neighbor_distances->resize(num_neighbors);
  flann::Matrix<int> indices(neighbor_indices->data(), 1, num_neighbors);
  flann::Matrix<float> dists(neighbor_distances->data(), 1, num_neighbors);
  DescriptorValue query_data[num_cols_];
  const std::string& desc = query_descriptor.data();
  CHECK_EQ(desc.size(), num_cols_);
  const DescriptorValue* desc_data = (DescriptorValue*)desc.data();
  for (int j=0; j < num_cols_; ++j){
    query_data[j] = desc_data[j];
  }
  flann::Matrix<DescriptorValue> query(query_data, 1, num_cols_);
  index_->knnSearch(query, indices, dists, num_neighbors, search_params_);
  return true;
}


////////////

FeatureCorrespondence::FeatureCorrespondence(int index_a, int index_b, float dist) :
  index_a(index_a),
  index_b(index_b),
  dist(dist)
{
}

bool set_sort_comparator(const FeatureCorrespondence& m1, const FeatureCorrespondence& m2){
  if (m1.index_a < m2.index_a){
    return true;
  }
  else if (m1.index_a == m2.index_a){
    if (m1.index_b < m2.index_b){
      return true;
    }
  }
  return false;
}

////////////

BidirectionalFeatureMatcher::BidirectionalFeatureMatcher() {
}

BidirectionalFeatureMatcher::~BidirectionalFeatureMatcher() {
}

bool BidirectionalFeatureMatcher::Run(const iw::ImageFeatures& features_a,
           const iw::ImageFeatures& features_b,
           FeatureCorrespondences* matches){
  CHECK(matches);
  FeatureCorrespondences matches_a_to_b;
  FeatureCorrespondences matches_b_to_a;
  MatchDescriptors(features_a.descriptors(), features_b.descriptors(), A_TO_B, &matches_a_to_b, 1);
  MatchDescriptors(features_a.descriptors(), features_b.descriptors(), B_TO_A, &matches_b_to_a, 1);
  sort(matches_a_to_b.begin(), matches_a_to_b.end(), set_sort_comparator);
  sort(matches_b_to_a.begin(), matches_b_to_a.end(), set_sort_comparator);
  set_intersection(matches_a_to_b.begin(), matches_a_to_b.end(),
                   matches_b_to_a.begin(), matches_b_to_a.end(),
                   back_inserter(*matches), set_sort_comparator);
  return true;
}

void BidirectionalFeatureMatcher::MatchDescriptors(const DescriptorVector& descriptors_a,
                                                   const DescriptorVector& descriptors_b,
                                                   MatchDirection match_direction,
                                                   FeatureCorrespondences* matches,
                                                   int num_neighbors){
  CHECK(matches);
  matches->clear();
  if (descriptors_a.size() < num_neighbors + 1 || descriptors_b.size() < num_neighbors + 1) {
    LOG(WARNING) << "Not enough features to match: " << descriptors_a.size() << ", " << descriptors_b.size() ;
    return;
  }

  const DescriptorVector& query_descriptors = (match_direction == A_TO_B) ?  descriptors_a : descriptors_b;
  const DescriptorVector& target_descriptors = (match_direction == A_TO_B) ?  descriptors_b : descriptors_a;
  FlannFeatureIndex ann(target_descriptors);
  int num_queries = query_descriptors.size();

  std::vector<int> nn;
  std::vector<float> dist;
  for (int i=0; i < num_queries; ++i){
    bool success = ann.GetNeighbors(query_descriptors.Get(i), num_neighbors, &nn, &dist);
    CHECK_EQ(dist.size(), num_neighbors);
    if (success){
      for (int j=0; j < num_neighbors; ++j){
        int aIdx = 0;
        int bIdx = 0;
        if (match_direction == A_TO_B){
         aIdx = i;
         bIdx = nn[j];
        }
        else if(match_direction == B_TO_A){
         aIdx = nn[j];
         bIdx = i;
        }
        else{
         LOG(ERROR) << "invalid match direction";
        }
        matches->push_back(FeatureCorrespondence(aIdx, bIdx, dist[j]));
      }
    }
  }
}



} // close namespace iw
