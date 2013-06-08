#pragma once

#include <string>
#include "snap/pert/pert.pb.h"
#include "snap/google/base/basictypes.h"
#include "snap/boost/bloom_filter/dynamic_bloom_filter.hpp"

namespace pert {

typedef boost::bloom_filters::dynamic_bloom_filter<std::string, boost::mpl::vector<boost::bloom_filters::boost_hash<std::string> >, uint64> StringBloomFilter;

double MeasureErrorRate(const std::vector<std::string>& pos_keys, const std::vector<std::string>& neg_keys, uint64 num_bits);
uint64 PredictRequiredBits(uint64 max_capacity, double false_positive_rate);
uint64 TuneRequiredBits(const std::vector<std::string>& pos_keys, const std::vector<std::string>& neg_keys, double desired_error_rate);

class KeyBloomFilterGenerator {
public:

  //KeyBloomFilterGenerator(uint64 max_capacity, double false_positive_rate);
  KeyBloomFilterGenerator(uint64 num_bits);
  void Add(const std::string& key);
  void Save(::pert::BloomFilterData* data);
private:
  //uint64 max_capacity_;
  //double false_positive_rate_;
  StringBloomFilter bloom_filter_;
};

class KeyBloomFilter {
public:
  KeyBloomFilter(const ::pert::BloomFilterData& data);
  bool ProbablyContains(const std::string& key);
private:
  StringBloomFilter bloom_filter_;
};

} // close namespace pert
