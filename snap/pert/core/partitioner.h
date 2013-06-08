#pragma once

#include "snap/google/base/basictypes.h"
#include <string>

namespace pert {

class KeyPartitioner {
public:
  virtual ~KeyPartitioner() {};
  virtual uint32 Partition(const std::string& key) const = 0;
};

class ModKeyPartitioner: public KeyPartitioner {
public:
  ModKeyPartitioner(uint32 num_shards);
  virtual ~ModKeyPartitioner();
  virtual uint32 Partition(const std::string& key) const;
  virtual uint32 NumShards() const;

private:
  uint32 num_shards_;
  std::string salt_;
};


}
