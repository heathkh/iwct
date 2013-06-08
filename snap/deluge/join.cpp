#include "deluge/join.h"
#include "deluge/util.h"
#include "deluge/job.h"
#include "snap/google/base/stringprintf.h"

using namespace std;

namespace deluge {

JoinMapper::JoinMapper(HadoopPipes::TaskContext& context) : join_tuple_(2), secondary_input_done_(false), advance_k2_(true) {

  JobInfo info(context);

  // create counters for join stats
  primary_key_counter_  = context.getCounter("map_join", "primary_key_counter_");
  secondary_key_counter_  = context.getCounter("map_join", "secondary_key_counter_");
  matching_key_counter_  = context.getCounter("map_join", "matching_key_counter_");

  // Get path of primary input shard from config
  std::string primary_input_file = info.GetStringOrDie("map.input.file");

  // Get shard number of primary input file
  int shard_num;
  CHECK(ParseShardNum(primary_input_file, &shard_num)) << "Expected primary input with sharded format part-00000";

  LOG(INFO) << "shard_num: " << shard_num;

  // Get secondary input dir from config
  std::string primary_input_uri = info.GetStringOrDie("join.primary.input.uri");
  std::string secondary_input_uri = info.GetStringOrDie("join.secondary.input.uri");

  // Determine name of secondary input shard
  string secondary_input_file = StringPrintf("%s/part-%05d", secondary_input_uri.c_str(), shard_num);

  // Open secondary input shard
  CHECK(secondary_reader_.Open(secondary_input_file)) << "Failed to open secondary input file: " << secondary_input_file;
  CHECK(secondary_reader_.IsSorted()) << "join inputs must be sorted";

  LOG(INFO) << "opened secondary input: " << secondary_input_file;

  // Get join type from config
  std::string join_type = info.GetStringOrDie("join.type");
  CHECK_EQ(join_type, "restricted_inner_join");

  // Validate map join pre-conditions
  int num_shards_primary = pert::GetNumShards(primary_input_uri);
  int num_shards_secondary = pert::GetNumShards(secondary_input_uri);
  // primary and secondary inputs have same number of shards
  CHECK_EQ(num_shards_primary, num_shards_secondary) << " MapJoin requires primary and secondary inputs to have identical number of shards.";


  pert::FileInfo primary_shard_info;
  CHECK(pert::StringTableShardReader::GetShardInfo(primary_input_file, &primary_shard_info));
  // Check primary shards are sorted
  CHECK(primary_shard_info.has_ordered_key_info());
  // Check primary uses same comparator function
  CHECK_EQ(primary_shard_info.ordered_key_info().comparator(), secondary_reader_.ComparatorName());

  // TODO: Check primary uses same partition function

  // create output comparator
  comparator_.reset(pert::CreateComparator("memcmp"));
}

JoinMapper::~JoinMapper() {

}

void JoinMapper::map(HadoopPipes::MapContext& context){

  context.incrementCounter(primary_key_counter_, 1);

  if (secondary_input_done_){
    return;
  }

  const std::string& k1 = context.getInputKey();
  const std::string& v1 = context.getInputValue();

  // Check precondition of "restricted" version of inner join

  CHECK_NE(k1, prev_k1_) << "Restricted Inner Join condition violated - Requires that the primary input has no duplicate keys... Use the less efficient InnerJoin instead.";
  CHECK_GT(k1, prev_k1_) << "Map-Join condition violated - Primary input must be sorted in non-decreasing order.";

  join_tuple_.key_ = &k1;

  join_tuple_.tuple_values_[0].is_null = false;
  join_tuple_.tuple_values_[0].buffer = v1.data();
  join_tuple_.tuple_values_[0].length = v1.size();

  while(true){
    if (advance_k2_) {
      if (!secondary_reader_.NextGreaterEqualNoCopy(k1, &k2_buf, &k2_len, &v2_buf, &v2_len)){
        secondary_input_done_ = true;
        break;
      }
      context.incrementCounter(secondary_key_counter_, 1);
      advance_k2_ = false;
    }

    // if equal, update join context with current tuple and call map
    int k1_cmp_k2 = comparator_->CompareKey(k1.data(), k1.size(), k2_buf, k2_len);

    if (k1_cmp_k2 == 0){
      join_tuple_.tuple_values_[1].is_null = false;
      join_tuple_.tuple_values_[1].buffer = v2_buf;
      join_tuple_.tuple_values_[1].length = v2_len;
      map(join_tuple_, context);
      context.incrementCounter(matching_key_counter_, 1);
      advance_k2_ = true;
    }
    else if (k1_cmp_k2 < 0){
      // K1 < K2 -> advance K1
      break;
    }
    else if (k1_cmp_k2 > 0){
      // K1 > K2 -> advance K2
      advance_k2_ = true;
    }
    else{
      CHECK(false) << "can't happen";
    }
  }

  prev_k1_ = k1;
}

} // close namespace deluge
