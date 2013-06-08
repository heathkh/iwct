#pragma once

#include <string>

namespace pert {

// Returns <0 if v1 < v2
//          0 if v1 == v2
//         >0 if v1 > v2
class KeyComparator {
public:
  virtual ~KeyComparator() {};
  virtual int CompareKey(const void* v1, size_t l1, const void* v2,
      size_t l2) const = 0;
};

class MemcmpKeyComparator: public KeyComparator {
public:
  virtual ~MemcmpKeyComparator() {};
  virtual int CompareKey(const void* v1, size_t l1, const void* v2,
      size_t l2) const;
};

KeyComparator* CreateComparator(const std::string& comparator_name);

bool ValidComparatorName(const std::string& comparator_name);

}
