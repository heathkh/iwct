#pragma once

#include "snap/google/base/basictypes.h"
#include "iw/matching/cbir/cbir.pb.h"
#include "iw/matching/cbir/bow/bow.pb.h"
#include <vector>
#include <string>

namespace cbir {

class Index {
public:
  Index() {};
  virtual ~Index() {};
  virtual bool Load(const std::string& name) = 0;
  virtual bool Save(const std::string& name) = 0;

  virtual int Size() = 0;
  virtual bool Query(uint64 query_image_id, const cbir::BagOfWords& bag_of_words, int max_count, cbir::QueryResults* results) = 0;
};


} // close namespace
