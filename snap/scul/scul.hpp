#pragma once
// SCUL -- Simple Container Utility Library

#include <algorithm>
#include "snap/google/glog/logging.h"
#include "snap/boost/foreach.hpp"
#include "snap/boost/range.hpp"
#include "snap/boost/range/numeric.hpp" // for boost::accumulate
#include "snap/boost/range/algorithm.hpp" // for boost::find
#include "snap/boost/range/sub_range.hpp"
#include "snap/boost/range/adaptor/map.hpp"
#include "snap/boost/unordered_map.hpp" // for boost::find

namespace scul {

////////////////////////////////////////////////////////////////////////////////
// Iterating over contents of containers
////////////////////////////////////////////////////////////////////////////////

/*
// Needed to allow FOREACH to grab const references from a container. (FOREACH assumes mutable access by default)
template <typename T>
std::pair<typename T::const_iterator, typename T::const_iterator> IterConst(const T& container){
  return std::pair<typename T::const_iterator, typename T::const_iterator>(container.begin(), container.end());
}
*/

#define scul_for_each                           BOOST_FOREACH
#define scul_for_each_value(value_type, range)  BOOST_FOREACH(value_type, boost::adaptors::values(range))
#define scul_for_each_key(value_type, range)    BOOST_FOREACH(value_type, boost::adaptors::keys(range))

////////////////////////////////////////////////////////////////////////////////
// Non-mutating (don't change the container)
////////////////////////////////////////////////////////////////////////////////

// Helper function to convert a container into a range
/*
template <typename Container>
std::pair<typename Container::const_iterator, typename Container::const_iterator> CreateConstRange(Container c) {
  return std::make_pair(c.begin(), c.end());
}
*/



// Sum
template <typename Range>
typename boost::range_value<Range>::type RangeSum(const Range& range) {
  return boost::accumulate(range, typename boost::range_value<Range>::type(0));
}

template <typename Container>
typename Container::value_type Sum(const Container& container) {
  return RangeSum(boost::make_iterator_range(container.begin(), container.end()));
}

// Mean
template <typename Range>
double RangeMean(const Range& range) {
  return double(RangeSum(range))/boost::size(range);
}

template <typename C>
double Mean(const C& container) {
  return RangeMean(boost::make_iterator_range(container.begin(), container.end()));
}

template <typename Range, typename V>
bool RangeContains(const Range& range, const V& value){
  if (boost::find(range, value) != boost::end(range)){
    return true;
  }
  return false;
}

template <typename C, typename V>
bool Contains(const C& container, const V& value){
  return RangeContains(boost::make_iterator_range(container.begin(), container.end()), value);
}


template <typename C>
typename C::mapped_type& FindOrDie(C& container, const typename C::key_type& key) {
  typename C::iterator iter = container.find(key);
  if (iter != container.end()){
    return iter->second;
  }
  LOG(FATAL) << "Requested key was not in container: " << key;
}


template <typename C>
const typename C::mapped_type& FindOrDie(const C& container, const typename C::key_type& key) {
  typename C::const_iterator iter = container.find(key);
  if (iter != container.end()){
    return iter->second;
  }
  LOG(FATAL) << "Requested key was not in container: " << key;
}


template <typename C, typename K, typename V>
bool Find(const C& container, const K& key, V* value){
  typename C::const_iterator iter;
  iter = container.find(key);
  if (iter != container.end()){
    *value = iter->second;
    return true;
  }
  return false;
}


////////////////////////////////////////////////////////////////////////////////
// Mutating operations (change the container)
////////////////////////////////////////////////////////////////////////////////

// Makes sorting stl containers more concise.
template <typename C>
void Sort(C* container){
  std::sort(container->begin(), container->end());
}


template <typename C, typename Comparator>
void Sort(C* container, Comparator comparator){
  std::sort(container->begin(), container->end(), comparator);
}

template <typename RepeatedProtoField>
void Truncate(RepeatedProtoField* repeated_field, int size){
  while(repeated_field->size() > size){
    repeated_field->RemoveLast();
  }
}


template <typename C1, typename C2>
void Append(const C1& src, C2* dst){
  dst->insert(dst->end(), src.begin(), src.end());
}

// Makes uniquifying stl containers more concise.
template <typename C, typename SortComparator, typename EqualityComparator>
void MakeUnique(C* container, SortComparator sort_cmp, EqualityComparator eq_cmp){
  Sort(container, sort_cmp);
  typename C::iterator new_end = std::unique(container->begin(), container->end(), eq_cmp);
  ptrdiff_t new_size = new_end - container->begin();
  CHECK_GE(new_size, 0);
  container->resize(new_size);
}


template <typename C>
void MakeUnique(C* container){
  Sort(container);
  typename C::iterator new_end = std::unique(container->begin(), container->end());
  ptrdiff_t new_size = new_end - container->begin();
  CHECK_GE(new_size, 0);
  CHECK_LE(new_size, container->size());
  if (new_size < container->size()){
    container->resize(new_size);
  }
}


// Given an array of N elements defining a bidirectional mapping between the
// integers 0 .. N and the elements.  Like an array, but can go the other direction.
template <typename Element>
class BidiractionalArray {
  typedef boost::unordered_map<Element, unsigned int> ElementToIndexMap;
 public:
  BidiractionalArray() { }
  BidiractionalArray(const std::vector<Element>& elements) {
    index_to_elements_ = elements;
    for (unsigned int i=0; i < elements.size(); ++i){
      element_to_index_[elements[i]] = i;
    }
  }

  size_t Size() const {
    return index_to_elements_.size();
  }

  Element GetElement( unsigned int index) const {
    CHECK_LT(index, index_to_elements_.size());
    return index_to_elements_[index];
  }

  const std::vector<Element>& GetElements() const {
    return index_to_elements_;
  }

  unsigned int GetIndex(const Element& element) const {
    typename ElementToIndexMap::const_iterator iter = element_to_index_.find(element);
    CHECK(iter != element_to_index_.end()) << "Element must exist in table: " << element;
    return iter->second;
  }

 private:
  std::vector<Element> index_to_elements_;
  ElementToIndexMap element_to_index_;
};



// Builds an map of integers to elements.  An element is given the next available
// index the first time it is seen, and the same index thereafter.
template <typename Element>
class AutoIndexer {
  typedef boost::unordered_map<Element, unsigned int> ElementToIndexMap;
 public:
  AutoIndexer() { }

  size_t Size() const {
    return index_to_elements_.size();
  }

  Element GetElement( unsigned int index) const {
    CHECK_LT(index, index_to_elements_.size());
    return index_to_elements_[index];
  }

  const std::vector<Element>& GetElements() const {
    return index_to_elements_;
  }

  unsigned int GetIndex(const Element& element)  {
    typename ElementToIndexMap::const_iterator iter = element_to_index_.find(element);
    // if element never seen, give it the next available index
    unsigned int index = 0;
    if (iter == element_to_index_.end()){
      index = index_to_elements_.size();
      index_to_elements_.push_back(element);
      element_to_index_[element] = index;
    }
    else {
      index = iter->second;
    }
    return index;
  }

 private:
  std::vector<Element> index_to_elements_;
  ElementToIndexMap element_to_index_;
};


class Permutation {
public:
  Permutation() {
  }

  template <typename ContainerType>
  void InitSortingPermutation(const ContainerType& src){
    InitSortingPermutation(src.begin(), src.end());
  }

  template <typename SrcIter>
  void InitSortingPermutation(const SrcIter& src_begin, const SrcIter& src_end){
    // Computes the ordering that sorts the given iterator range.
    typedef PermutationRecordBase<SrcIter> PermutationRecord;
    typedef std::vector<PermutationRecord > IteratorPermutation;
    IteratorPermutation iter_permutation;

    size_t size = src_end - src_begin;
    iter_permutation.resize(size);
    size_t n = 0;
    for (SrcIter it = src_begin; it != src_end; ++it, ++n){
      iter_permutation[n] = PermutationRecord(n, it);
    }

    std::sort(iter_permutation.begin(), iter_permutation.end());
    // store permutation for future use
    permutation_.resize(size);
    for ( unsigned int i=0; i < iter_permutation.size(); ++i){
      permutation_[i] = iter_permutation[i].index;
    }
  }

  template <typename ContainerType>
  void Apply(const ContainerType& src_container, ContainerType* dst_container) {
    // Applies the precomputed ordering to a new iterator range
    size_t dst_size = dst_container->end() - dst_container->begin();
    CHECK_EQ(dst_size, permutation_.size());

    typedef typename ContainerType::iterator Iter;
    typedef typename ContainerType::const_iterator ConstIter;

    Iter dst_iter = dst_container->begin();
    size_t size = permutation_.size();
    for (size_t i = 0; i < size; ++i){
      ConstIter src_iter = src_container.begin() + permutation_[i];
      *dst_iter = *src_iter;
      dst_iter++;
    }
  }

  size_t size() {
    return permutation_.size();
  }

protected:
  template <typename SrcIter>
  class PermutationRecordBase{
  public:
    PermutationRecordBase() {}
    PermutationRecordBase(size_t index, SrcIter iter) : index(index), iter(iter) {}

    size_t index;
    SrcIter iter;
    bool operator<(const PermutationRecordBase& rhs) const {
      return *iter < *(rhs.iter);
    }
  };

  std::vector< unsigned int> permutation_;
};

} // close namespace scul
