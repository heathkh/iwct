#pragma once

#include "snap/flann/flann.hpp"
#include "snap/boost/scoped_ptr.hpp"
#include "iw/iw.pb.h"

namespace iw {

/** Class for performing ANN queries with sift descriptors.
 * Just a wrapper around David Lowe's FLANN library.
 */
class FlannFeatureIndex
{
public:
  FlannFeatureIndex(const ::google::protobuf::RepeatedPtrField< ::iw::FeatureDescriptor >& descriptors);
  virtual ~FlannFeatureIndex();

  bool GetNeighbors(const ::iw::FeatureDescriptor& query, int num_neighbors, std::vector<int>* neighbor_indices, std::vector<float>* neighbor_distances );

private:
  typedef unsigned char DescriptorValue;
  int num_rows_;
  int num_cols_;
  flann::Matrix<DescriptorValue> dataset_;
  flann::IndexParams index_params_;
  flann::SearchParams search_params_;
  typedef flann::Index<flann::L2<DescriptorValue> > Index;
  boost::scoped_ptr<Index> index_;
};



class FeatureCorrespondence {
public:
  FeatureCorrespondence(int index_a, int index_b, float dist);
  int index_a;
  int index_b;
  float dist;
};

typedef std::vector<FeatureCorrespondence> FeatureCorrespondences;

// Matches features using the bidirectional mutual best match strategy
class BidirectionalFeatureMatcher {
public:
  BidirectionalFeatureMatcher();
	~BidirectionalFeatureMatcher();

	bool Run(const iw::ImageFeatures& features_a,
           const iw::ImageFeatures& features_b,
           FeatureCorrespondences* correspondences);

protected:
	enum MatchDirection {A_TO_B, B_TO_A};
	typedef ::google::protobuf::RepeatedPtrField< ::iw::FeatureDescriptor> DescriptorVector;
	void MatchDescriptors(const DescriptorVector& descriptors_a,
                          const DescriptorVector& descriptors_b,
                          MatchDirection match_dir,
                          FeatureCorrespondences* match_indices,
                          int num_neighbors);

};


bool set_sort_comparator(const FeatureCorrespondence& m1, const FeatureCorrespondence& m2);


} // close namespace iw
