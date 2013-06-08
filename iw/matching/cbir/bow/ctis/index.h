#pragma once

#include <vector>
#include <string>
#include "iw/matching/cbir/bow/index.h"
#include "snap/ctis/ccInvertedFile.hpp"

namespace cbir {

class CtisIndex : public Index {
public:
  CtisIndex();
  virtual ~CtisIndex();

  virtual bool Load(const std::string& filename);
  virtual bool Save(const std::string& filename);

  void StartCreate(uint32 num_words,  uint32 num_docs);
  virtual bool Add(uint64 image_id, const cbir::BagOfWords& bow);
  void EndCreate();

  virtual int Size();
  virtual bool Query(uint64 query_image_id, const cbir::BagOfWords& bag_of_words, int max_count, cbir::QueryResults* results);

protected:

  ivFile::Weight weight_;
  ivFile::Norm norm_;
  ivFile::Dist dist_;
  std::vector<uint64> image_ids_;
  ivFile ivf_;
  bool ready_;
};


}
