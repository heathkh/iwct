#include "snap/pert/pert.h"
#include "snap/google/base/hashutils.h"
#include "snap/google/base/stringprintf.h"
#include "snap/google/gflags/gflags.h"
#include "gtest/gtest.h"
#include <set>
#include <vector>
#include <string>
#include "snap/boost/foreach.hpp"
#include "snap/boost/timer/timer.hpp"
#include "snap/boost/uuid/random_generator.hpp"
#include "snap/boost/uuid/uuid_io.hpp"
#include "snap/boost/foreach.hpp"
#include <iostream>
#include "snap/scul/scul.hpp"

using namespace std;

namespace {


std::vector< std::string> GetUriPrefixes() {
  std::vector<std::string> uri_prefixes;
  //uri_prefixes.push_back("local://tmp/foo10/");
  uri_prefixes.push_back("maprfs://tmp/");
  return uri_prefixes;
}


using ::testing::TestWithParam;
using ::testing::Bool;
using ::testing::Values;
using ::testing::ValuesIn;
using ::testing::Combine;

// uri_base, compression, num_shards
typedef ::std::tr1::tuple<std::string, int, int> MergeTestParams;

class ParameterizedTest : public ::testing::TestWithParam<MergeTestParams> {
protected:
  std::string base_uri_;
  int num_input_tables_;
  int num_shards_pert_table_;
  std::vector<std::string> input_table_uris_;

  void CreateInputTables(){
    CHECK(base_uri_.size());
    for (int table_index=0; table_index < num_input_tables_; ++table_index){
      std::string uri = StringPrintf("%s/test/table_%d.pert", base_uri_.c_str(), table_index);
      if (pert::Exists(uri)){
        pert::Remove(uri);
      }
      input_table_uris_.push_back(uri);

      pert::StringTableWriter writer;
      CHECK(writer.Open(uri, num_shards_pert_table_));
      for (int i=0; i < 100;++i){
        writer.Add(Uint64ToKey(i), "fooooo!");
      }
    }
  }
};


class MergeStringTable : public ParameterizedTest {
protected:
};

TEST_P(MergeStringTable, SimpleMerge) {
  tr1::tie(base_uri_, num_input_tables_, num_shards_pert_table_) = GetParam();
  CreateInputTables();
  string output_uri = StringPrintf("%s/test/merged_tables.pert", base_uri_.c_str());

  pert::MergeTables(input_table_uris_, output_uri);

  pert::StringTableReader reader;
  ASSERT_TRUE(reader.Open(output_uri));

  // check that merged table is in fact sorted and the keys show up the correct
  // number of times
  string key, prev_key, value;
  typedef map<string, int> KeyToCountMap;
  KeyToCountMap key_to_count;
  while (reader.Next(&key, &value)){
    ASSERT_GE(key, prev_key);
    prev_key = key;
    key_to_count[key] += 1;
  }

  int num_key_replications = num_input_tables_;
  BOOST_FOREACH(const KeyToCountMap::value_type& v, key_to_count){
    ASSERT_EQ(num_key_replications, v.second);
  }

}


INSTANTIATE_TEST_CASE_P(ParamSet01,
                        MergeStringTable,
                        Combine(ValuesIn(GetUriPrefixes()),
                                Values(1,3),
                                Values(1,3)));


} // close empty namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  return RUN_ALL_TESTS();
}
