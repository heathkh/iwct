#include "snap/pert/core/comparator.h"
#include "snap/google/glog/logging.h"
#include <algorithm>
#include <string>
#include "snap/boost/scoped_ptr.hpp"

namespace pert {

int MemcmpKeyComparator::CompareKey(const void* v1, size_t l1, const void* v2,
    size_t l2) const {
  size_t overlap_len = std::min(l1, l2);
  int result = memcmp(v1, v2, overlap_len);

  // if one key, is a prefix of the other the shorter one comes first
  if (result == 0) {
    if (l1 < l2) { // if key 1 is a prefix of key 2, key 1 comes first
      result = -1;
    } else if (l1 > l2) { // if key 2 is a prefix of key 1, key 2 comes first
      result = 1;
    }
  }
  return result;
}

KeyComparator* CreateComparator(const std::string& comparator_name) {
  KeyComparator* comparator = NULL;
  if (comparator_name == "memcmp") {
    comparator = new MemcmpKeyComparator;
  } else {
    LOG(ERROR) << "unknown comparator name: " << comparator_name;
    LOG(FATAL) << "valid comparator names include {memcmp}";
  }
  return comparator;
}

bool ValidComparatorName(const std::string& comparator_name) {

  if (comparator_name == "none"){
    return true;
  }

  boost::scoped_ptr<KeyComparator> cmp;
  cmp.reset(CreateComparator(comparator_name));
  return cmp.get() != NULL;
}

}
