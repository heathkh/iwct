#pragma once

#include "iw/iw.pb.h"
#include "iw/matching/cbir/bow/bow.pb.h"
#include "snap/flann/flann.hpp"
#include "snap/boost/scoped_ptr.hpp"

namespace cbir {

// Computes the visual words for a given set of SIFT descriptors
class Quantizer {
public:
  Quantizer();
  ~Quantizer();

  bool Init(std::string visual_vocab_uri, bool exact = false);
  bool Quantize(const iw::ImageFeatures& features,
                cbir::BagOfWords* bag_of_words);
  int GetNumDims() { return num_dims_; }


private:
  int num_words_;
  int num_dims_;
  flann::Matrix<float> dataset_;
  flann::IndexParams index_params_;
  flann::SearchParams search_params_;

  typedef flann::Index<flann::L2<float> > Index;
  boost::scoped_ptr<Index> index_;
};

} // close namespace

