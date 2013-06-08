#include "snap/pert/stringtable.h"
#include "snap/pert/core/comparator.h"
#include "snap/pert/test/test.pb.h"
#include "snap/google/base/stringprintf.h"
#include "snap/google/base/hashutils.h"
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


using namespace std;

namespace {


std::vector< string> GetUriPrefixes() {
  std::vector<string> uri_prefixes;
  uri_prefixes.push_back("local://tmp/foo10/");
  //uri_prefixes.push_back("maprfs://data/tmp/");
  return uri_prefixes;
}

std::vector< string> GetCompressionCodecs() {
  std::vector<string> codecs;
  //codecs.push_back("none");
  codecs.push_back("snappy");
  return codecs;
}

using ::testing::TestWithParam;
using ::testing::Bool;
using ::testing::Values;
using ::testing::ValuesIn;
using ::testing::Combine;

// uri_base, compression, num_shards
typedef ::std::tr1::tuple<string> UriParams;
typedef ::std::tr1::tuple<string, string> UriCompressionParams;
typedef ::std::tr1::tuple<string, string, int> UriCompressionShardsParams;

class UriCompressionParameterizedTest : public ::testing::TestWithParam<UriCompressionParams> {
protected:
  string base_uri_;
  string test_uri_;
  string compression_codec_;

  void SetTestUri(){
    CHECK(base_uri_.size());
    string uri = base_uri_ + "/test/foobar.pert";

    if (pert::Exists(uri)){
      //cout << "removing uri: " << uri << endl;
      CHECK(pert::Remove(uri));
      CHECK(!pert::Exists(uri));
    }

    test_uri_ = uri;
  }
};


//////////



TEST(Comparators, MemcmpKeyComparator) {
  char s1[] = "001";
  char s2[] = "010";
  char s3[] = "021";
  char s4[] = "101";

  pert::KeyComparator* cmp = pert::CreateComparator("memcmp");
  ASSERT_LT(cmp->CompareKey(s1, sizeof(s1), s2, sizeof(s2)), 0);
  ASSERT_LT(cmp->CompareKey(s2, sizeof(s2), s3, sizeof(s3)), 0);
  ASSERT_LT(cmp->CompareKey(s3, sizeof(s3), s4, sizeof(s4)), 0);

}



///////////////////////////////////////////////////////////////////////////////
// Tests for StringTableShardWriter and StringTableShardReader
// parameterized by (uri_base, compression)
///////////////////////////////////////////////////////////////////////////////


class StringTableShard : public UriCompressionParameterizedTest {
protected:
};


TEST_P(StringTableShard, WriteReadBig) {
  ::std::tr1::tie(base_uri_, compression_codec_) = GetParam();
  SetTestUri();

  string key, value;
  string empty_value("foooooooooooooooooooooooooooo");
  //empty_value.resize(50);

  uint64 num = 10000;
  pert::WriterOptions writer_options;
  pert::StringTableShardWriter writer;

  writer_options.SetCompressed(compression_codec_);
  ASSERT_TRUE(writer.Open(test_uri_, writer_options));

  for (uint64 i = 0; i < num; ++i) {
    key = StringPrintf("k%016llu", i);
    value = StringPrintf("v %s %s", key.c_str(), empty_value.c_str());
    ASSERT_TRUE(writer.Add(key,value));
  }
  writer.Close();


  pert::StringTableShardReader reader;
  ASSERT_TRUE(reader.Open(test_uri_));

  for (uint64 i = 0; i < num; ++i) {
    //cout << i << endl;
    string expected_key = StringPrintf("k%016llu", i);
    string expected_value = StringPrintf("v %s %s", expected_key.c_str(), empty_value.c_str());
    reader.Next(&key, &value);
    ASSERT_EQ(expected_key, key);
    ASSERT_EQ(expected_value, value);
  }

  ASSERT_FALSE(reader.Next(&key, &value)) << "must return false when all items read";
  reader.Close();
}


TEST_P(StringTableShard, WriteReadEmpty) {
  ::std::tr1::tie(base_uri_, compression_codec_) = GetParam();
  SetTestUri();

  string key, value;
  pert::WriterOptions writer_options;
  pert::StringTableShardWriter writer;

  writer_options.SetCompressed(compression_codec_);
  ASSERT_TRUE(writer.Open(test_uri_, writer_options));
  writer.Close();

  pert::StringTableShardReader reader;
  ASSERT_TRUE(reader.Open(test_uri_));

  //ASSERT_FALSE(reader.Next(&key, &value));
  const char* key_buf;
  uint64 key_buf_len;
  const char* value_buf;
  uint64 value_buf_len;
  string query = "foo";
  ASSERT_FALSE(reader.NextGreaterEqualNoCopy(query, &key_buf, &key_buf_len, &value_buf, &value_buf_len));
}

INSTANTIATE_TEST_CASE_P(ParamSet01,
                    StringTableShard,
                        Combine(ValuesIn(GetUriPrefixes()),
                                ValuesIn(GetCompressionCodecs())
                                ));

///////////////////////////////////////////////////

class UriCompressionShardsParameterizedTest : public ::testing::TestWithParam<UriCompressionShardsParams> {
protected:
  string base_uri_;
  string test_uri_;
  string compression_codec_;
  int num_shards_;
  std::vector<string> test_uris_;

  void SetTestUri(){
    CHECK(base_uri_.size());
    string uri = base_uri_ + "/test/foobar.pert";
    if (pert::Exists(uri)){
      pert::Remove(uri);
    }
    test_uri_ = uri;
  }

  void InitTestUris(int num){
    CHECK(base_uri_.size());
    for (int i=0; i < num; ++i){
      string uri = StringPrintf("%s/test/foobar_%05d.pert", base_uri_.c_str(), i);
      if (pert::Exists(uri)){
        pert::Remove(uri);
      }
      test_uris_.push_back(uri);
    }
  }
};


///////////////////////////////////////////////////////////////////////////////
// Tests for Unsorted StringTableWriter and StringTableReader
// parameterized by (uri_base, compression, num_shards)
///////////////////////////////////////////////////////////////////////////////

class StringTable : public UriCompressionShardsParameterizedTest {
protected:
};


TEST_P(StringTable, UnsortedWriteAndRead) {
  ::std::tr1::tie(base_uri_, compression_codec_, num_shards_) = GetParam();
  SetTestUri();

  pert::WriterOptions writer_options;
  pert::StringTableWriter writer;

  writer_options.SetUnsorted();
  ASSERT_TRUE(writer.Open(test_uri_, num_shards_, writer_options));

  string key, value;
  set<string> expected_keys;

  for (int i = 0; i < 1e3; ++i) {
    string key = StringPrintf("k%010d", i);
    string value = StringPrintf("v%d", i);
    ASSERT_TRUE(writer.Add(key, value));
    expected_keys.insert(key);
  }
  writer.Close();

  pert::StringTableReader reader;
  ASSERT_TRUE(reader.Open(test_uri_));
  ASSERT_EQ(expected_keys.size(), reader.Entries());

  int num_read = 0;
  while(reader.Next(&key, &value)){
    ASSERT_TRUE(expected_keys.find(key) != expected_keys.end());
    num_read++;
  }

  ASSERT_EQ(num_read, expected_keys.size()) << "failed to read all items";
  reader.Close();
}


TEST_P(StringTable, NextGreaterEqual) {
  ::std::tr1::tie(base_uri_, compression_codec_, num_shards_) = GetParam();
  SetTestUri();
  pert::WriterOptions writer_options;
  pert::StringTableWriter writer;

  writer_options.SetCompressed(compression_codec_);
  writer_options.SetSorted("memcmp");
  ASSERT_TRUE(writer.Open(test_uri_, num_shards_, writer_options));

  string key, value;
  string empty_value;
  empty_value.resize(500);

  for (int i = 0; i < 1e4; ++i) {
    writer.Add(StringPrintf("k%010d", i), StringPrintf("v%d %s", i, empty_value.c_str()));
  }
  writer.Close();

  pert::StringTableReader reader;
  ASSERT_TRUE(reader.Open(test_uri_));

  string match_key, match_value;
  string query_key = "k0000000010";
  string next_query_key = "k0000000011";
  ASSERT_TRUE(reader.NextGreaterEqual(query_key, &match_key, &match_value  ));
  ASSERT_EQ(query_key, match_key);
  ASSERT_TRUE(reader.NextGreaterEqual(query_key, &match_key, &match_value  ));
  ASSERT_NE(query_key, match_key );
  ASSERT_EQ(match_key, next_query_key);

  reader.Close();
}



TEST_P(StringTable, WriteReadMedium) {
  ::std::tr1::tie(base_uri_, compression_codec_, num_shards_) = GetParam();
  SetTestUri();

  pert::WriterOptions writer_options;
  pert::StringTableWriter writer;

  writer_options.SetCompressed(compression_codec_);
  ASSERT_TRUE(writer.Open(test_uri_, num_shards_, writer_options));

  string key, value;
  string empty_value;
  empty_value.resize(50, ' ');

  uint64 num = 2000;

  for (uint64 i = 0; i < num; ++i) {
    key = StringPrintf("k%016llu", i);
    value = StringPrintf("v %s %s", key.c_str(), empty_value.c_str());
    ASSERT_TRUE(writer.Add(key,value));
  }
  writer.Close();

  pert::StringTableReader reader;
  ASSERT_TRUE(reader.Open(test_uri_));

  for (uint64 i = 0; i < num; ++i) {
    string expected_key = StringPrintf("k%016llu", i);
    string expected_value = StringPrintf("v %s %s", expected_key.c_str(), empty_value.c_str());
    bool more = reader.Next(&key, &value);
    ASSERT_TRUE(more);
    ASSERT_EQ(expected_key, key);
    ASSERT_EQ(expected_value, value);
  }

  ASSERT_FALSE(reader.Next(&key, &value)) << "must return false when all items read";
  reader.Close();
}




TEST_P(StringTable, WriteReadBig) {
  ::std::tr1::tie(base_uri_, compression_codec_, num_shards_) = GetParam();
  SetTestUri();

  pert::WriterOptions writer_options;
  pert::StringTableWriter writer;

  writer_options.SetCompressed(compression_codec_);
  ASSERT_TRUE(writer.Open(test_uri_, num_shards_, writer_options));

  string key, value;
  string empty_value("foooooooooooooooooooooooooooo");
  //empty_value.resize(50);

  uint64 num = 1000000;

  for (uint64 i = 0; i < num; ++i) {
    key = StringPrintf("k%016llu", i);
    value = StringPrintf("v %s %s", key.c_str(), empty_value.c_str());
    ASSERT_TRUE(writer.Add(key,value));
  }
  writer.Close();

  pert::StringTableReader reader;
  ASSERT_TRUE(reader.Open(test_uri_));

  for (uint64 i = 0; i < num; ++i) {
    //cout << i << endl;
    string expected_key = StringPrintf("k%016llu", i);
    string expected_value = StringPrintf("v %s %s", expected_key.c_str(), empty_value.c_str());
    reader.Next(&key, &value);
    ASSERT_EQ(expected_key, key);
    ASSERT_EQ(expected_value, value);
  }

  ASSERT_FALSE(reader.Next(&key, &value)) << "must return false when all items read";
  reader.Close();
}




TEST_P(StringTable, Find) {
  ::std::tr1::tie(base_uri_, compression_codec_, num_shards_) = GetParam();
  SetTestUri();

  uint64 num = 100000;
  double design_error_rate = 0.01;
  boost::uuids::random_generator gen;

  pert::WriterOptions writer_options;
  pert::StringTableWriter writer;

  //uint64 num_bits = pert::PredictRequiredBits(num, design_error_rate);

  writer_options.SetCompressed(compression_codec_);
  writer_options.SetBloomFilterPredictBitsPerShard(num, design_error_rate);
  ASSERT_TRUE(writer.Open(test_uri_, num_shards_, writer_options));

  string key, value;
  string empty_value("foooooooooooooooooooooooooooo");

  for (uint64 i = 0; i < num; ++i) {
    //key = to_string(gen());
    key = StringPrintf("k%016llu", i);
    value = StringPrintf("v %s %s", key.c_str(), empty_value.c_str());
    ASSERT_TRUE(writer.Add(key,value));
    //LOG_EVERY_N(INFO, 1000) << "progress: " << double(i)/num;
  }
  writer.Close();

  pert::StringTableReader reader;
  ASSERT_TRUE(reader.Open(test_uri_));


  // check we don't find keys that aren't there
  //TODO(kheath): add check that we don't hit disk too much
  for (uint64 i = 0; i < 1000; ++i) {
    string test_key = StringPrintf("k%016llu", i+num+1);

    bool hit_disk;
    bool found = reader.FindVerbose(test_key, &value, &hit_disk);


    ASSERT_FALSE(found);
  }

  // check we do find keys that are there
  for (uint64 i = 0; i < 100; ++i) {
    //LOG_EVERY_N(INFO, 10) << "progress: " << double(i)/num;
    string expected_key = StringPrintf("k%016llu", i);
    string expected_value = StringPrintf("v %s %s", expected_key.c_str(), empty_value.c_str());
    bool found = reader.Find(expected_key, &value);
    ASSERT_TRUE(found) << "failed to find key: " << expected_key;
    ASSERT_EQ(expected_value, value);
  }
  reader.Close();
}



INSTANTIATE_TEST_CASE_P(ParamSet01,
                        StringTable,
                        Combine(ValuesIn(GetUriPrefixes()),
                                ValuesIn(GetCompressionCodecs()),
                                Values(1,2,21,200)));

///////////////////////////////////////////////////////////////////////////////
// Tests for Unsorted StringTableWriter and StringTableReader
// parameterized by (uri_base, compression, num_shards)
///////////////////////////////////////////////////////////////////////////////


class StringTableShardSetReader : public UriCompressionShardsParameterizedTest {
protected:
};


TEST_P(StringTableShardSetReader, Test1) {
  ::std::tr1::tie(base_uri_, compression_codec_, num_shards_) = GetParam();
  SetTestUri();

  pert::WriterOptions writer_options;
  pert::StringTableWriter writer;

  writer_options.SetUnsorted();
  ASSERT_TRUE(writer.Open(test_uri_, num_shards_, writer_options));

  string key, value;
  set<string> expected_keys;

  for (int i = 0; i < 1e3; ++i) {
    string key = StringPrintf("k%010d", i);
    string value = StringPrintf("v%d", i);
    ASSERT_TRUE(writer.Add(key, value));
    expected_keys.insert(key);
  }
  writer.Close();

  pert::StringTableShardSetReader reader;

  std::vector<string> shard_uris = pert::GetShardUris(test_uri_);

  ASSERT_TRUE(reader.Open(shard_uris));

  int num_read = 0;
  while(reader.Next(&key, &value)){
    ASSERT_TRUE(expected_keys.find(key) != expected_keys.end());
    num_read++;
  }

  ASSERT_EQ(num_read, expected_keys.size()) << "failed to read all items";
  reader.Close();
}


TEST_P(StringTableShardSetReader, Next) {
  ::std::tr1::tie(base_uri_, compression_codec_, num_shards_) = GetParam();
  SetTestUri();

  pert::WriterOptions writer_options;
  writer_options.SetCompressed(compression_codec_);
  string key, value;
  int entries_per_shard = 100;
  uint64 cur_index = 0;
  for (int shard_num=0; shard_num < num_shards_; ++shard_num ){
    string shard_uri = StringPrintf("%s/part-%05d", test_uri_.c_str(), shard_num);
    pert::StringTableShardWriter writer;
    ASSERT_TRUE(writer.Open(shard_uri, writer_options));
    for (uint64 i = 0; i < entries_per_shard; ++i) {
      key = StringPrintf("k%016lld", cur_index);
      value = StringPrintf("v %s", key.c_str());
      ASSERT_TRUE(writer.Add(key,value));
      ++cur_index;
    }
    writer.Close();
  }

  pert::StringTableShardSetReader reader;
  std::vector<string> part_uris = pert::GetShardUris(test_uri_);
  ASSERT_TRUE(reader.Open(part_uris));

  uint64 num_entries = entries_per_shard*num_shards_;
  for (uint64 i = 0; i < num_entries; ++i) {
    string expected_key = StringPrintf("k%016lld", i);
    string expected_value = StringPrintf("v %s", expected_key.c_str());
    bool more = reader.Next(&key, &value);
    if (i != num_entries -1){
      ASSERT_TRUE(more);
    }
    ASSERT_EQ(expected_key, key);
    ASSERT_EQ(expected_value, value);
  }

  ASSERT_FALSE(reader.Next(&key, &value)) << "must return false when all items read";
  reader.Close();
}

TEST_P(StringTableShardSetReader, IndexOrdered) {
  ::std::tr1::tie(base_uri_, compression_codec_, num_shards_) = GetParam();
  SetTestUri();

  pert::WriterOptions writer_options;
  writer_options.SetCompressed(compression_codec_);
  writer_options.SetBlockSize(10);
  string key, value;
  int entries_per_shard = 100;
  uint64 cur_index = 0;
  for (int shard_num=0; shard_num < num_shards_; ++shard_num ){
    string shard_uri = StringPrintf("%s/part-%05d", test_uri_.c_str(), shard_num);
    pert::StringTableShardWriter writer;
    ASSERT_TRUE(writer.Open(shard_uri, writer_options));
    for (uint64 i = 0; i < entries_per_shard; ++i) {
      key = StringPrintf("k%016lld", cur_index);
      value = StringPrintf("v %s", key.c_str());
      ASSERT_TRUE(writer.Add(key,value));
      ++cur_index;
    }
    writer.Close();
  }

  pert::StringTableShardSetReader reader;
  std::vector<string> part_uris = pert::GetShardUris(test_uri_);
  ASSERT_TRUE(reader.Open(part_uris));

  uint64 num_entries = entries_per_shard*num_shards_;
  for (uint64 i = 0; i < num_entries; ++i) {
    string expected_key = StringPrintf("k%016lld", i);
    string expected_value = StringPrintf("v %s", expected_key.c_str());
    ASSERT_TRUE(reader.GetIndex(i, &key, &value));
    ASSERT_EQ(expected_key, key);
    ASSERT_EQ(expected_value, value);
  }

  ASSERT_FALSE(reader.Next(&key, &value)) << "must return false when all items read";
  reader.Close();
}

INSTANTIATE_TEST_CASE_P(ParamSet01,
                        StringTableShardSetReader,
                        Combine(ValuesIn(GetUriPrefixes()),
                                ValuesIn(GetCompressionCodecs()),
                                Values(1,2,21,200)));



///////////////////////////////////////////////////////////////////////////////
// Tests for fingerprint
///////////////////////////////////////////////////////////////////////////////

class Fingerprint : public UriCompressionShardsParameterizedTest {
protected:
  void CreateTestTable(string output_uri, string output_salt){
    pert::StringTableWriter writer;
    pert::WriterOptions writer_options;
    writer_options.SetCompressed(compression_codec_);
    ASSERT_TRUE(writer.Open(output_uri, num_shards_, writer_options));
    string key, value;
    string dummy_value("foooooooooooooooooooooooooooo");
    int num = 10000;
    for (uint64 i = 0; i < num; ++i) {
      key = StringPrintf("k%016llu", i);
      value = StringPrintf("v %s %s", key.c_str(), dummy_value.c_str());
      ASSERT_TRUE(writer.Add(key+output_salt,value+output_salt));
    }
    writer.Close();
  }
};


TEST_P(Fingerprint, basic) {
  ::std::tr1::tie(base_uri_, compression_codec_, num_shards_) = GetParam();
  InitTestUris(3);
  CreateTestTable(test_uris_[0], "foo");
  CreateTestTable(test_uris_[1], "foo");
  CreateTestTable(test_uris_[2], "bar");
  string fp0, fp1, fp2;

  ASSERT_TRUE(pert::GetShardSetFingerprint(test_uris_[0], &fp0));
  ASSERT_TRUE(pert::GetShardSetFingerprint(test_uris_[1], &fp1));
  ASSERT_TRUE(pert::GetShardSetFingerprint(test_uris_[2], &fp2));
  LOG(INFO) << fp0;
  LOG(INFO) << fp1;
  LOG(INFO) << fp2;
  ASSERT_EQ(fp0, fp1); // fp of table with identical data must be identical
  ASSERT_NE(fp0, fp2); // fp of table with different data must be different
}

INSTANTIATE_TEST_CASE_P(ParamSet01,
                        Fingerprint,
                        Combine(ValuesIn(GetUriPrefixes()),
                                ValuesIn(GetCompressionCodecs()),
                                Values(1,2,21,200)));

} // close empty namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  return RUN_ALL_TESTS();
}
