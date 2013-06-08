#pragma once

#include <string>
#include "snap/google/glog/logging.h"
#include "snap/google/base/integral_types.h"
#include "snap/pert/pert.h"

namespace iw {
  void ParseImagePairKey(const std::string& image_pair_key, uint64* image_id_1, uint64* image_id_2);
  std::string UndirectedEdgeKey(uint64 a, uint64 b);
  bool WriteStringToFile(const std::string& filename, const std::string& str);
} // close namespace iw
