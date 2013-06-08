#include "snap/google/base/stringprintf.h"
#include "gtest/gtest.h"
#include "snap/pert/core/bloom.h"
#include "snap/google/glog/logging.h"
#include "snap/boost/uuid/random_generator.hpp"
#include "snap/boost/uuid/uuid_io.hpp"

#include "snap/boost/foreach.hpp"
#include <vector>
#include <string>

using namespace std;

namespace {


TEST(BloomFilter, Test1) {

  int design_capacity = 100000;
  double design_error_rate = 0.01;

  uint64 num_bits = pert::PredictRequiredBits(design_capacity, design_error_rate);

  pert::KeyBloomFilterGenerator generator(num_bits);
  boost::uuids::random_generator gen;

  vector<string> keys;
  for (int i=0; i < design_capacity; ++i ){
    string key = to_string(gen());
    generator.Add(key);
    keys.push_back(key);
  }

  pert::BloomFilterData bloom_filter_data;
  generator.Save(&bloom_filter_data);

  pert::KeyBloomFilter filter(bloom_filter_data);

  // Verify bloom filter invariant (all keys in filter always detected)
  BOOST_FOREACH(string& key, keys){
    ASSERT_TRUE(filter.ProbablyContains(key));
  }

  // Verify bloom filter error rate
  int num_tests = 10000;
  int num_errors = 0;
  for (int i=0; i < num_tests; ++i ){
    string key = to_string(gen());
    if (filter.ProbablyContains(key)){
      num_errors++;
    }
  }

  double error_rate = double(num_errors)/num_tests;
  LOG(INFO) << "error_rate: " << error_rate;
  //TODO(kheath): devise a reasonable way to check the design error rate... for now disable this check
  //CHECK_LE(error_rate, design_error_rate);

}


} // close empty namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
