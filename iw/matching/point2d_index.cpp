#include "iw/matching/point2d_index.h"
#include "snap/google/glog/logging.h"

namespace iw {

FlannPoint2DIndex::FlannPoint2DIndex() {}

FlannPoint2DIndex::~FlannPoint2DIndex(){ }

void FlannPoint2DIndex::AddPoint(int x, int y){
  data_.push_back(x);
  data_.push_back(y);
}

//#define AUTOTUNE

bool FlannPoint2DIndex::BuildIndex() {
  CHECK(!index_.get()) << "index already built";
  num_cols_ = 2;
  num_rows_ = data_.size()/num_cols_;
  dataset_ = flann::Matrix<int>(&data_[0], num_rows_, num_cols_);
  #ifdef AUTOTUNE
  index_params_ = flann::AutotunedIndexParams(0.75, // target_precision
                                       0.1, //  build_weight
                                       0, // memory_weight
                                       1.0); // sample_fraction

  flann::Logger::setLevel(flann::FLANN_LOG_INFO);
  #else

  // tuned for precision = 0.95
  /*
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
  */

  // tuned for 0.75
  index_params_["algorithm"] = flann::FLANN_INDEX_KDTREE;
  index_params_["trees"] = 1;

  search_params_.checks = 5;
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


bool FlannPoint2DIndex::RadiusSearch(int x, int y, float radius, std::vector<int>* neighbor_indices, std::vector<float>* neighbor_distances ){
  CHECK(neighbor_indices);
  CHECK(neighbor_distances);
  if (!index_.get()){
    CHECK(BuildIndex());
  }

  int query_data[2];
  query_data[0] = x;
  query_data[1] = y;
  flann::Matrix<int> query_mat(query_data, 1, num_cols_);
  std::vector<std::vector<int> > indices;
  std::vector<std::vector<float> > dists;
  index_->radiusSearch(query_mat, indices, dists, radius, search_params_);
  CHECK_EQ(indices.size(), 1);
  CHECK_EQ(dists.size(), 1);
  *neighbor_indices = indices[0];
  *neighbor_distances = dists[0];
  return true;
}

bool FlannPoint2DIndex::SelfRadiusSearch(float radius, std::vector<std::vector<int> >* indices){
  CHECK(indices);
  if (!index_.get()){
    CHECK(BuildIndex());
  }

  std::vector<std::vector<float> > dists(dataset_.rows);
  indices->resize(dataset_.rows);
  index_->radiusSearch(dataset_, *indices, dists, radius, search_params_);
  return true;
}




} // close namespace iw
