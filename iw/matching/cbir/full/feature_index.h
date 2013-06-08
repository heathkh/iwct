#pragma once

#include "iw/iw.pb.h"
#include "iw/matching/cbir/full/full.pb.h"
#include "iw/taskqueue.hpp"
#include "snap/boost/scoped_ptr.hpp"
#include "snap/boost/unordered_map.hpp"
#include "snap/flann/flann.hpp"
#include "snap/google/base/basictypes.h"
#include <map>

namespace cbir {

// Truncate list of features to given length.
void ReduceImageFeatures(int max_features, iw::ImageFeatures* features);


class ImageFeatureCountTable {
 public:
  ImageFeatureCountTable();
  bool Create(const std::string& features_uri); // compute directly from features pert
  bool Create(const std::vector<cbir::FeatureIndexImageMetadata>& feature_index_metadata); // compute from the metadata required for a cbir index
  bool Save(const std::string& feature_counts_uri);
  bool Load(const std::string& feature_counts_uri);
  int GetCountOrDie(uint64 image_id) const;

 private:
  bool ready_;
  typedef boost::unordered_map<uint64, int> ImageToFeatureCount;
  ImageToFeatureCount imageid_to_numfeatures_;
};



// Represents a part of a distributed ANN index of sift features.
class FeatureIndex {
public:
  FeatureIndex();
  ~FeatureIndex();

  void StartPreallocate(uint64 num_preallocated_features);
  bool AddPreallocated(uint64 image_id, const iw::ImageFeatures& features);
  void EndPreallocate();

  bool Add(uint64 image_id, const iw::ImageFeatures& features);
  void Build(const cbir::FlannParams& params);
  bool Save(const std::string& uri);
  bool Load(const std::string& uri);
  bool Search(uint64 ignore_image_id, int k, bool include_keypoints,  const iw::ImageFeatures& features, cbir::ImageFeaturesNeighbors* features_neighbors) const;
  bool Search(const std::set<uint64>& ignore_image_ids, int k, bool include_keypoints, const iw::ImageFeatures& features, cbir::ImageFeaturesNeighbors* features_neighbors) const;
  bool GetImageFeatureCountTable(ImageFeatureCountTable* ifct) const{ CHECK(ifct); ifct->Create(this->index_metadata_); return true; }

private:
  typedef unsigned char ElementType;
  uint64 GetImageContainingFeature(int64 feature_index) const;
  void ResultIndexToImageFeatureIndices(int64 feature_index, uint64* result_image_id, uint32* result_image_feature_id) const;
  bool SearchWithDepth(const std::set<uint64>& ignore_image_ids, int knn_search_depth, int num_neighbors, const flann::Matrix<ElementType>& queries, cbir::ImageFeaturesNeighbors* features_neighbors) const;

  FlannParams params_;
  size_t num_preallocated_features_;
  size_t num_dims_;
  size_t num_features_;
  std::vector<ElementType> feature_data_;
  std::vector<cbir::Keypoint> keypoint_metadata_;
  flann::Matrix<ElementType> feature_matrix_;
  flann::IndexParams index_params_;
  flann::SearchParams search_params_;
  typedef flann::L2<ElementType> DistanceType;
  typedef flann::Index<DistanceType> Index;
  boost::scoped_ptr<Index> index_;
  typedef std::vector<cbir::FeatureIndexImageMetadata> MetadataVector;
  MetadataVector index_metadata_;
  int max_features_per_image_;
  unsigned char* feature_data_ptr_;
};


class NearestNeighborsAccumulator {
  public:
  NearestNeighborsAccumulator();
  void Add(const cbir::ImageFeaturesNeighbors& features_knn);
  bool GetResult(cbir::ImageFeaturesNeighbors* merged_neighbors);
 private:
  cbir::ImageFeaturesNeighbors accumulated_features_knn_;
};


struct IndexSearchTaskResult {
  IndexSearchTaskResult(uint64 image_id, boost::shared_ptr<cbir::ImageFeaturesNeighbors> neighbors) :
    image_id(image_id),
    neighbors(neighbors) {
  }
  uint64 image_id;
  boost::shared_ptr<cbir::ImageFeaturesNeighbors> neighbors;
};

struct IndexSearchTask {
  typedef IndexSearchTaskResult result_type;

  IndexSearchTask(const FeatureIndex& index, uint64 ignore_image_id, int k, bool include_keypoints, boost::shared_ptr<iw::ImageFeatures> features) :
    index_(index),
    ignore_image_id_(ignore_image_id),
    k_(k),
    include_keypoints_(include_keypoints),
    features_(features) {
  }

  result_type operator()() {
    boost::shared_ptr<cbir::ImageFeaturesNeighbors> neighbors(new cbir::ImageFeaturesNeighbors);
    index_.Search(ignore_image_id_, k_, include_keypoints_, *features_.get(), neighbors.get());
    result_type result(ignore_image_id_, neighbors);
    return result;
  }

 private:
  const FeatureIndex& index_;
  uint64 ignore_image_id_;
  int k_;
  bool include_keypoints_;
  boost::shared_ptr<iw::ImageFeatures> features_;
};


class ConcurrentFeatureIndex {
 public:
  ConcurrentFeatureIndex(const FeatureIndex& index );

  void AddJob(uint64 ignore_image_id, int k, bool include_keypoints, boost::shared_ptr<iw::ImageFeatures> features);
  bool JobsDone(); // returns true if GetJob would not block
  void GetJob(uint64* image_id, cbir::ImageFeaturesNeighbors* features_neighbors);
  int  NumJobsPending();

 private:
  TaskQueue<IndexSearchTask> task_queue_;
  const FeatureIndex& index_;
};



} // close namespace cbir

