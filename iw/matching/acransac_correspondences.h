#pragma once

#include "iw/iw.pb.h"
#include "iw/matching/feature_matcher.h"

namespace iw {

typedef ::google::protobuf::RepeatedPtrField< ::iw::Correspondence > Correspondences;

// Generates uncorreleated correspondences which are required for a-contrario methods
class ACRansacIndependenceFilter {
public:
  ACRansacIndependenceFilter(const ImageFeatures& features_a, const ImageFeatures& features_b, const FeatureCorrespondences& initial_correspondences);
  bool Run(Correspondences* correspondences, Correspondences* discarded_correspondences = NULL);

private:
  const FeatureKeypoint& GetKeypoint(int correspondence_index, bool a_is_primary);
  float GetDescriptorDistance(int correspondence_index);
  void FilterByDirectionV1(bool a_is_primary);
  //void FilterByDirectionV2(bool a_is_primary);

  const ImageFeatures& features_a_;
  const ImageFeatures& features_b_;
  const FeatureCorrespondences& initial_correspondences_;
  std::vector<bool> is_discarded_;
};


/*
// Half-baked alternative to above that may or may not be better

class FlannPoint2DIndex {
public:
  FlannPoint2DIndex();
  virtual ~FlannPoint2DIndex();

  void Add(const Point2D& point);
  bool GetNeighbors(const Point2D& query, std::vector<int>* neighbor_indices, std::vector<float>* neighbor_distances );

private:

  bool BuildIndex();
  std::vector<float> data_;
  typedef float ValueType;
  int num_rows_;
  int num_cols_;
  flann::Matrix<ValueType> dataset_;
  flann::IndexParams index_params_;
  flann::SearchParams search_params_;
  typedef flann::Index<flann::L2<ValueType> > Index;
  boost::scoped_ptr<Index> index_;
};
*/

} // close namespace iw
