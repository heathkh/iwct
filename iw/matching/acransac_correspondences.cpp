#include "iw/matching/acransac_correspondences.h"
#include "iw/matching/feature_matcher.h"
#include "snap/google/glog/logging.h"
#include "snap/boost/foreach.hpp"
#include <map>

namespace iw {

struct KeypointNeighborCellComparator {
  KeypointNeighborCellComparator(float neighbor_threshold) : neighbor_threshold_(neighbor_threshold) { }

  bool operator() (const FeatureKeypoint* a, const FeatureKeypoint* b) {
    // First group by X POS
    if (a->pos().x() < b->pos().x() - neighbor_threshold_) {
      return true;
    }
    else {
      if (a->pos().x() > b->pos().x() + neighbor_threshold_){
        return false;
      }
      else{
        // Invariant: x coords are within +/- EQUIV_CLASS_THRESH_POS
        // Then group by Y POS
        if (a->pos().y() < b->pos().y() - neighbor_threshold_){
          return true;
        }
        else{
          if (a->pos().y() > b->pos().y() + neighbor_threshold_) {
            return false;
          }
        }
      }
    }
    // Invariant: both x and y coords are within +/- EQUIV_CLASS_THRESH_POS
    return false;
  }
private:
  float neighbor_threshold_;
};

typedef std::map<const iw::FeatureKeypoint*, std::vector<int>, KeypointNeighborCellComparator > KeypointNeighborCells;

/////////////////////////////////////////////////////

ACRansacIndependenceFilter::ACRansacIndependenceFilter(const ImageFeatures& features_a, const ImageFeatures& features_b, const FeatureCorrespondences& initial_correspondences) :
  features_a_(features_a),
  features_b_(features_b),
  initial_correspondences_(initial_correspondences) {
}


// Performs bi-directional feature matching and removes correlated correspondences
bool ACRansacIndependenceFilter::Run(Correspondences* correspondences, Correspondences* discarded_correspondences){
  CHECK(correspondences);

  if (discarded_correspondences) {
    discarded_correspondences->Clear();
  }
  is_discarded_.resize(initial_correspondences_.size());
  std::fill(is_discarded_.begin(), is_discarded_.end(), false);

  FilterByDirectionV1(true);
  FilterByDirectionV1(false);

  // copy keypoints to RepeatedCorrespondence
  for (int i=0; i < initial_correspondences_.size(); ++i){
    Correspondence* c = NULL;
    if (!is_discarded_[i]){
      c = correspondences->Add();
    }
    else {
      if (discarded_correspondences){
        c = discarded_correspondences->Add();
      }
    }
    if (c){
      const FeatureCorrespondence& correspondence = initial_correspondences_[i];
      c->mutable_a()->CopyFrom(features_a_.keypoints(correspondence.index_a));
      c->mutable_b()->CopyFrom(features_b_.keypoints(correspondence.index_b));
      c->set_id_a(correspondence.index_a);
      c->set_id_b(correspondence.index_b);
    }
  }

  return true;
}

const FeatureKeypoint& ACRansacIndependenceFilter::GetKeypoint(int correspondence_index, bool a_is_primary){
  const FeatureCorrespondence& correspondence = initial_correspondences_.at(correspondence_index);
  if (a_is_primary){
    return features_a_.keypoints(correspondence.index_a);
  }
  else{
    return features_b_.keypoints(correspondence.index_b);
  }
}

float ACRansacIndependenceFilter::GetDescriptorDistance(int correspondence_index){
  return initial_correspondences_.at(correspondence_index).dist;
}

float Distance(const iw::Point2D& p1, const iw::Point2D& p2 ){
  float dx = p1.x() - p2.x();
  float dy = p1.y() - p2.y();
  return sqrt(dx*dx + dy*dy);
}

void ACRansacIndependenceFilter::FilterByDirectionV1(bool a_is_primary){
  // Check image for correspondences that have approximately the same position
  float cell_size = 5;
  KeypointNeighborCellComparator cmp(cell_size);
  KeypointNeighborCells keypoint_cells(cmp);
  CHECK_EQ(initial_correspondences_.size(), is_discarded_.size());

  for (int index=0; index < initial_correspondences_.size(); ++index){
    const FeatureKeypoint& keypoint = GetKeypoint(index, a_is_primary);
    keypoint_cells[&keypoint].push_back(index);
  }

  BOOST_FOREACH(KeypointNeighborCells::value_type v, keypoint_cells){
    std::vector<int>& indices = v.second;
    // for all pairs of correspondences that share (about) the same endpoint in primary image
    for (int i_index=0; i_index < indices.size(); ++i_index){
      int i = indices[i_index];
      if (is_discarded_[i]) continue;
      for (int j_index=i_index+1; j_index < indices.size(); ++j_index){
        int j = indices[j_index];
        if (is_discarded_[j]) continue;
        // check if the keypoints in the secondary image are "close"
        const FeatureKeypoint& keypoint_i = GetKeypoint(i, a_is_primary);
        const FeatureKeypoint& keypoint_j = GetKeypoint(j, a_is_primary);

        float min_radius = std::min(keypoint_i.radius(), keypoint_j.radius());
        CHECK_GT(min_radius, 0);
        float dist = Distance(keypoint_i.pos(), keypoint_j.pos());
        //LOG(INFO) << "dist: " << dist;
        bool correlated = dist < min_radius;
        if (correlated) {
          int discard_index = GetDescriptorDistance(i) < GetDescriptorDistance(j) ? j : i;
          is_discarded_[discard_index] = true;

          //LOG(INFO) << "discarding correlated correspondence... dist: " << dist << " min_scale: " << min_scale;
          //LOG(INFO) << "i (" << i << "): " << GetDescriptorDistance(i);
          //LOG(INFO) << "j (" << j << "): " << GetDescriptorDistance(j);
          //LOG(INFO) << "discarded :" << discard_index;
        }
      }
    }
  }
}

/*
void ACRansacCorrespondenceGenerator::FilterByDirectionV2(bool a_is_primary){
  // Check image for correspondences that have approximately the same position

  // Build ann index of points in primary image
  FlannPoint2DIndex ann();
  for (int index=0; index < initial_correspondences_.size(); ++index){
    const FeatureKeypoint& keypoint = GetKeypoint(index, a_is_primary);
    ann.Add(Point2D(keypoint.pos().x(),keypoint.pos().y()));
  }

  // For each point in primary image, find other points that are within it's support region
  for (int index=0; index < initial_correspondences_.size(); ++index){
    const FeatureKeypoint& keypoint = GetKeypoint(index, a_is_primary);
    Point2D query(keypoint.pos().x(),keypoint.pos().y())
    double query_radius = keypoint.size()/2;
    ann.GetNeighbors(query, , std::vector<int>* neighbor_indices, std::vector<float>* neighbor_distances ) {

    std::vector<int>& indices =


    // for all pairs of correspondences that share (about) the same endpoint in primary image
    for (int i_index=0; i_index < indices.size(); ++i_index){
      int i = indices[i_index];
      if (is_discarded_[i]) continue;
      for (int j_index=i_index+1; j_index < indices.size(); ++j_index){
        int j = indices[j_index];
        if (is_discarded_[j]) continue;
        // check if the keypoints in the secondary image are "close"
        const FeatureKeypoint& keypoint_i = GetKeypoint(i, a_is_primary);
        const FeatureKeypoint& keypoint_j = GetKeypoint(j, a_is_primary);

        float min_size = std::min(keypoint_i.size(), keypoint_j.size());
        CHECK_GT(min_size, 0);
        float dist = Distance(keypoint_i.pos(), keypoint_j.pos());
        //LOG(INFO) << "dist: " << dist;
        bool correlated = dist < min_size/2.0; // size is a diameter so need to compare to radius
        if (correlated) {
          int discard_index = GetDescriptorDistance(i) < GetDescriptorDistance(j) ? j : i;
          is_discarded_[discard_index] = true;

          //LOG(INFO) << "discarding correlated correspondence... dist: " << dist << " min_scale: " << min_scale;
          //LOG(INFO) << "i (" << i << "): " << GetDescriptorDistance(i);
          //LOG(INFO) << "j (" << j << "): " << GetDescriptorDistance(j);
          //LOG(INFO) << "discarded :" << discard_index;
        }
      }
    }
  }
}

// Half-baked alternative to above that may or may not be better

FlannPoint2DIndex::FlannPoint2DIndex() {}

FlannPoint2DIndex::~FlannPoint2DIndex(){ }

void FlannPoint2DIndex::Add(const Point2D& point){
  data_.push_back(point.x());
  data_.push_back(point.y());
}

#define AUTOTUNE

bool FlannPoint2DIndex::BuildIndex() {
  CHECK_NULL(index_.get()) << "index already built";
  num_cols_ = 2;
  num_rows_ = data_.size()/num_cols_;
  dataset_ = flann::Matrix<float>(&data_[0], num_rows_, num_cols_);
  #ifdef AUTOTUNE
  index_params_ = AutotunedIndexParams(0.95, // target_precision
                                       0.5, //  build_weight
                                       0, // memory_weight
                                       0.5); // sample_fraction

  flann::Logger::setLevel(FLANN_LOG_INFO);
  #else

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

  #endif

  index_.reset(new Index(dataset_, index_params_));
  index_->buildIndex();

  #ifdef AUTOTUNE
  flann::print_params(index_->getParameters());
  exit(1);
  #endif
  return true;
}

bool FlannPoint2DIndex::GetNeighbors(const Point2D& query, float radius, std::vector<int>* neighbor_indices, std::vector<float>* neighbor_distances ) {
  CHECK(neighbor_indices);
  CHECK(neighbor_distances);

  if (!index_.get()){
    CHECK(BuildIndex());
  }

  neighbor_indices->resize(num_neighbors);
  neighbor_distances->resize(num_neighbors);
  flann::Matrix<int> indices(neighbor_indices->data(), 1, num_neighbors);
  flann::Matrix<float> dists(neighbor_distances->data(), 1, num_neighbors);

  float query_data[2];
  query_data[0] = query.x();
  query_data[1] = query.y();
  flann::Matrix<DescriptorValue> query_mat(query_data, 1, num_cols_);
  std::vector<std::vector<int> > indices;
  std::vector<std::vector<DistanceType> > dists;

  index_->radiusSearch(query_mat, indices, dists, radius, search_params_);
  CHECK_EQ(indices.size(), 1);
  CHECK_EQ(dists.size(), 1);
  neighbor_indices->assign(indices[0]);
  neighbor_distances->assign(dists[0]);
  return true;
}

*/


} // close namespace iw
