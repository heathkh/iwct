#pragma once

#include "snap/flann/flann.hpp"
#include "snap/boost/scoped_ptr.hpp"

namespace iw {

class FlannPoint2DIndex {
public:
  FlannPoint2DIndex();
  virtual ~FlannPoint2DIndex();
  void AddPoint(int x, int y);
  bool RadiusSearch(int x, int y, float radius, std::vector<int>* neighbor_indices, std::vector<float>* neighbor_distances );
  bool SelfRadiusSearch(float radius, std::vector<std::vector<int> >* indices);

private:
  bool BuildIndex();
  std::vector<int> data_;
  typedef int ValueType;
  int num_rows_;
  int num_cols_;
  flann::Matrix<ValueType> dataset_;
  flann::IndexParams index_params_;
  flann::SearchParams search_params_;
  typedef flann::Index<flann::L2<ValueType> > Index;
  boost::scoped_ptr<Index> index_;
};


} // close namespace iw
