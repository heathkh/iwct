#include "iw/matching/cbir/bow/util.h"
#include "snap/boost/foreach.hpp"
#include "snap/scul/scul.hpp"
#include "snap/google/glog/logging.h"
#include <set>

namespace cbir {

void BowToL1NormalizedSparseVector(const BagOfWords& bow, SparseVector* doc_vec, bool binarize){

  uint32 cur_word_id = 0;
  uint32 cur_word_count = 0;
  doc_vec->Clear();
  BOOST_FOREACH(uint32 next_word_id, bow.word_id()){
    CHECK_LE(cur_word_id, next_word_id) << "bow must be sorted ascending";

    // we found the boundary between a run of different word ids
    if (next_word_id != cur_word_id && cur_word_count){
      doc_vec->add_indices(cur_word_id);
      doc_vec->add_values(cur_word_count);

      // prepare counter to count the new word_id
      cur_word_count = 0;
    }

    cur_word_count++;
    cur_word_id = next_word_id;
  }

  // add the TF or binary term occurrence flag to the vector (some say binary works better for large vocab sizes)
  if (cur_word_count){
    doc_vec->add_indices(cur_word_id);
    if (binarize){
      doc_vec->add_values(1);
    }
    else{
      doc_vec->add_values(cur_word_count);
    }
  }


  // Normalize (if not entirely zero)
  bool has_non_zero_entries = doc_vec->indices_size();
  if (has_non_zero_entries){
    // since vector are counters incremented for each entry in bow vector, the L1 norm is just the len of the bow vector
    double l1_norm = bow.word_id_size();
    CHECK_GT(l1_norm, 0.0);
    double scale = 1.0/l1_norm;

    // normalize vector to unit L1 length
    BOOST_FOREACH(double& value, *doc_vec->mutable_values()){
      value = value * scale;
    }
  }
}


void SortedCopy(const SparseVector& in_vec, SparseVector* out_vec){

  // START: Disable after debug!!
  // Make sure there are no duplicate indices in input
  {
    std::set<uint64> prev_indices;
    for (int i=0; i < in_vec.indices_size(); ++i){
      uint64 cur_index = in_vec.indices(i);
      CHECK(!scul::Contains(prev_indices, cur_index)) << "cur_index: " << cur_index;
      prev_indices.insert(cur_index);
    }
  }
  // END

  CHECK(out_vec);
  out_vec->Clear();
  scul::Permutation p;
  p.InitSortingPermutation(in_vec.indices());
  CHECK_EQ(p.size(), in_vec.indices_size());

  ResizeRepeatedField(out_vec->mutable_indices(), p.size());
  ResizeRepeatedField(out_vec->mutable_values(), p.size());

  p.Apply(in_vec.indices(), out_vec->mutable_indices());
  p.Apply(in_vec.values(), out_vec->mutable_values());

  // START: Disable after debug!!
  // Make sure there are no duplicate indicies
  {
    uint64 prev_index = 0;
    for (int i=0; i < out_vec->indices_size(); ++i){
      uint64 cur_index = out_vec->indices(i);
      if (i){
        CHECK_GT(cur_index, prev_index) << "i: " << i << " size: " << out_vec->indices_size();
      }
      prev_index = cur_index;
    }
  }
  // END
}

} // close namespace cbir
