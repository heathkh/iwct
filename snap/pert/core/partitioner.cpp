#include "snap/pert/core/partitioner.h"
#include "snap/google/glog/logging.h"
#include <string>
#include "snap/google/base/hashutils.h"

namespace pert {


ModKeyPartitioner::ModKeyPartitioner(uint32 num_shards) : num_shards_(num_shards), salt_("foobarandstuff") {

}

ModKeyPartitioner::~ModKeyPartitioner() { }

uint32 ModKeyPartitioner::Partition(const std::string& key) const {
  std::string tmp = salt_+key;
  //return FingerprintString(tmp) % num_shards_;
  return FingerprintString(tmp.data(), tmp.size()) % num_shards_;
}

uint32 ModKeyPartitioner::NumShards() const{
  return num_shards_;
}

} // close namespace pert
