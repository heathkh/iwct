#pragma once

#include "snap/google/base/basictypes.h"
#include "snap/google/glog/logging.h"
#include "iw/iw.pb.h"
#include "iw/matching/cbir/bow/bow.pb.h"
#include <vector>
#include <string>
#include <algorithm>

namespace cbir {

void BowToL1NormalizedSparseVector(const cbir::BagOfWords& bow, cbir::SparseVector* doc_vec, bool binarize = true);

// When constructing or contactinating sparse vectors, the indices may become unordered
// this restores the index ordering.
void SortedCopy(const cbir::SparseVector& in_vec, cbir::SparseVector* out_vec);

template <typename RepeatedField>
void ResizeRepeatedField(RepeatedField* rf, size_t size){
  rf->Reserve(size); // allocates the space
  for (size_t i=0; i < size; ++i) {
    rf->AddAlreadyReserved(); // increments cur size counter so
  }
}

} // close namespace
