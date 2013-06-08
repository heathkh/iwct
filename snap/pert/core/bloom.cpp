#include "snap/pert/core/bloom.h"
#include "snap/google/glog/logging.h"
#include <math.h>
#include <iterator>
#include "snap/boost/foreach.hpp"

namespace pert {

uint64 PredictRequiredBits(uint64 max_capacity, double false_positive_rate){
  // Check preconditions
  CHECK_GT(false_positive_rate, 0.0);

  // Compute required number of bits (see http://en.wikipedia.org/wiki/Bloom_filter)
  double required_bits_ideal = - double(max_capacity) * log(false_positive_rate)/(log(2.0)*log(2.0));
  double safety_factor = 1.8; // emperical fudge factor based on uuid test case (account for lack of ideal hashing??)
  uint64 required_bits = ceil(required_bits_ideal*safety_factor);
  CHECK_GE(required_bits, 1);
  //LOG(INFO) << "creating bloom filter with size: " << required_bits;
  CHECK_GT(required_bits, 0);
  return required_bits;
}

double MeasureErrorRate(const std::vector<std::string>& pos_keys, const std::vector<std::string>& neg_keys, uint64 num_bits){
  BloomFilterData bloom_data;
  {
    KeyBloomFilterGenerator generator(num_bits);
    BOOST_FOREACH(const std::string& key, pos_keys){
      generator.Add(key);
    }
    generator.Save(&bloom_data);
  }

  KeyBloomFilter filter(bloom_data);

  uint64 num_error = 0;
  BOOST_FOREACH(const std::string& key, neg_keys){
    if (filter.ProbablyContains(key)){
      num_error++;
    }
  }
  double emperical_error_rate = double(num_error) / neg_keys.size();
  return emperical_error_rate;
}

uint64 TuneRequiredBits(const std::vector<std::string>& pos_keys, const std::vector<std::string>& neg_keys, double desired_error_rate){

  uint64 num_bits_ideal = PredictRequiredBits(pos_keys.size(), desired_error_rate);
  uint64 cur_num_bits = num_bits_ideal;
  double cur_error_rate = MeasureErrorRate(pos_keys, neg_keys, cur_num_bits);

  while (cur_error_rate > desired_error_rate){
    cur_num_bits += num_bits_ideal*0.25;
    cur_error_rate = MeasureErrorRate(pos_keys, neg_keys, cur_num_bits);
    LOG(INFO) << "cur_num_bits: " << cur_num_bits << " cur_error_rate: " << cur_error_rate << " inefficiency_factor: " << double(cur_num_bits)/num_bits_ideal;
  }
  return cur_num_bits;
}



/*
KeyBloomFilterGenerator::KeyBloomFilterGenerator(uint64 max_capacity, double false_positive_rate) :
    max_capacity_(max_capacity),
    false_positive_rate_(false_positive_rate){

  uint64 required_bits = PredictRequiredBits(max_capacity_, false_positive_rate_);
  bloom_filter_.resize(required_bits);
}
*/

KeyBloomFilterGenerator::KeyBloomFilterGenerator(uint64 num_bits){
  bloom_filter_.resize(num_bits);
}

void KeyBloomFilterGenerator::KeyBloomFilterGenerator::Add(const std::string& key){
  bloom_filter_.insert(key);
}

void KeyBloomFilterGenerator::Save(BloomFilterData* data){
  CHECK(data);
  data->Clear();

  //data->set_target_capacity(max_capacity_);
  //data->set_target_error_rate(false_positive_rate_);
  data->set_num_entries(bloom_filter_.count());
  data->set_num_bits(bloom_filter_.bit_capacity());

  // TODO(kheath): can you copy directly into the protobuf repeated field instead of copying from temp vector?
  std::vector<uint64> tmp_state;
  bloom_filter_.to_block_range(std::back_inserter(tmp_state));
  ::google::protobuf::RepeatedField< ::google::protobuf::uint64 >* state = data->mutable_filter_state();
  state->Reserve(tmp_state.size()); // we know size in advance and can prevent copies on resize
  BOOST_FOREACH(uint64 block, tmp_state){
    state->Add(block);
  }
}


KeyBloomFilter::KeyBloomFilter(const BloomFilterData& data){
  data.CheckInitialized();
  bloom_filter_.from_block_range(data.num_bits(), data.filter_state().begin(), data.filter_state().end());
}

bool KeyBloomFilter::ProbablyContains(const std::string& key){
  return bloom_filter_.probably_contains(key);
}

} // close namespace pert
