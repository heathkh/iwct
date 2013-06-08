#include "gtest/gtest.h"
#include "snap/google/glog/logging.h"
#include "iw/iw.pb.h"
#include "iw/matching/cbir/bow/util.h"
#include <math.h>
#include "snap/boost/foreach.hpp"

using namespace std;

namespace {

using namespace cbir;

BagOfWords CreateUnsortedTestBow(){
  BagOfWords bow;

  bow.add_word_id(10);
  bow.add_word_id(12);
  bow.add_word_id(8); // must fail!
  return bow;
}

BagOfWords CreateSortedTestBow(){
  BagOfWords bow;

  bow.add_word_id(10);
  bow.add_word_id(10);
  bow.add_word_id(12);
  bow.add_word_id(12);
  bow.add_word_id(12);
  bow.add_word_id(12);
  bow.add_word_id(14);
  bow.add_word_id(15);
  return bow;
}

/* TODO(kheath): figure out how to use death tests
TEST(BowToL1NormalizedSparseVector, InvalidInput) {
  iw::BagOfWords bow =  CreateUnsortedTestBow();
  iw::SparseVector doc_vec;
  ASSERT_DEATH(iw::BowToL1NormalizedSparseVector(bow, &doc_vec), ".*");
}
*/

bool equal(const SparseVector& v1, const SparseVector& v2, double eps = 1e-6){
  CHECK_EQ(v1.indices_size(), v1.values_size());
  CHECK_EQ(v2.indices_size(), v2.values_size());
  if (v1.indices_size() != v2.indices_size()){
    return false;
  }
  uint32 len = v1.indices_size();
  for (uint32 i=0; i < len; ++i){
    if (v1.indices(i) != v2.indices(i)){
      return false;
    }
    double diff = fabs(v1.values(i) - v2.values(i));
    if (diff > eps){
      return false;
    }
  }
  return true;
}

TEST(BowToL1NormalizedSparseVector, ValidInput) {
  BagOfWords bow =  CreateSortedTestBow();
  SparseVector doc_vec;
  BowToL1NormalizedSparseVector(bow, &doc_vec);

  // TODO(kheath): check that doc_vec is correct

  SparseVector gt;
  gt.add_indices(10);
  gt.add_values(2.0/8);
  gt.add_indices(12);
  gt.add_values(4.0/8);
  gt.add_indices(14);
  gt.add_values(1.0/8);
  gt.add_indices(15);
  gt.add_values(1.0/8);

  CHECK(equal(doc_vec, gt));
}


SparseVector CreateFromIndices(std::vector<int> indices){
  SparseVector vec;
  BOOST_FOREACH(int i, indices){
    vec.add_indices(i);
    vec.add_values(i);
  }
  return vec;
}

TEST(MergeAndSortPostings, v1) {

  std::vector<int> indices_1;
  indices_1.push_back(8);
  indices_1.push_back(7);
  indices_1.push_back(6);

  std::vector<int> indices_2;
  indices_2.push_back(3);
  indices_2.push_back(2);
  indices_2.push_back(1);

  SparseVector p1 = CreateFromIndices(indices_1);
  SparseVector p2 = CreateFromIndices(indices_2);

  SparseVector unsorted_postings;
  unsorted_postings.MergeFrom(p1);
  unsorted_postings.MergeFrom(p2);

  LOG(INFO) << unsorted_postings.DebugString();

  // after concatenating, we need to sort SparseVector entries by index
  SparseVector postings;
  SortedCopy(unsorted_postings, &postings);

  LOG(INFO) << postings.DebugString();

}


} // close empty namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
