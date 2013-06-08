#include "snap/pert/pert.h"
#include "snap/google/gflags/gflags.h"
//#include "snap/google/strings/split.h"
//#include "snap/google/base/hashutils.h"
//#include <iostream>
//#include "snap/boost/foreach.hpp"
#include "snap/progress/progress.hpp"

using namespace std;
using namespace progress;


DEFINE_string(input, "", "input path");
DEFINE_string(output, "", "output path");
DEFINE_double(new_block_size_mb, 64, "desired block size for output file in megabytes");
DEFINE_int32(num_output_shards, 1, "desired number of shards for output file");

std::string prep_uri(const std::string& input_uri){
  std::string scheme;
  std::string path;
  std::string output_uri = input_uri;
  if(!pert::ParseUri(input_uri, &scheme, &path)){
    LOG(INFO) << "no scheme provided... Assuming local://";
    output_uri = "local://" + input_uri;
  }
  return output_uri;
}


typedef std::pair<std::string, std::string> KeyValuePair;

struct KeyValuePairComparator {
  bool operator()(const KeyValuePair& kvp1, const KeyValuePair& kvp2){
    return kvp1.first < kvp2.first;
  }
};

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  string usage("This program creates a key sorted copy of a PERT file. \nSample usage: pertsort --input=<input path> --output=<output path> \nUse --help for more info.\n");
  google::SetUsageMessage(usage);

  if (FLAGS_input.empty()){
    LOG(INFO) << "must provide flag --input";
    return 1;
  }

  if (FLAGS_output.empty()){
    LOG(INFO) << "must provide flag --output";
    return 1;
  }

  if (FLAGS_new_block_size_mb < 0){
    LOG(INFO) << "must provide flag --new_block_size_mb";
    return 1;
  }

  if (FLAGS_num_output_shards < 0){
    LOG(INFO) << "must provide flag --num_output_shards";
    return 1;
  }

  string input_uri = prep_uri(FLAGS_input);
  string output_uri = prep_uri(FLAGS_output);

  LOG(INFO) << "input_uri: " << input_uri;

  boost::scoped_ptr<pert::StringTableReaderBase> reader;
  reader.reset(new pert::StringTableShardSetReader());

  CHECK(reader->Open(input_uri));



  pert::WriterOptions options;
  uint64 block_size_bytes = FLAGS_new_block_size_mb * (1 << 20);
  options.SetBlockSize(block_size_bytes);
  options.SetSorted("memcmp");
  pert::StringTableWriter writer;
  LOG(INFO) << "output_uri: " << output_uri;
  CHECK(writer.Open(output_uri, FLAGS_num_output_shards, options));

  const char* key_buf;
  uint64 key_buf_len;
  const char* value_buf;
  uint64 value_buf_len;

  std::vector<KeyValuePair> key_value_pairs;
  uint64 num_entries = reader->Entries();
  key_value_pairs.resize(num_entries);
  ProgressBar<uint64> read_progress(reader->Entries());
  for (int i=0; i < num_entries; ++i){
    reader->NextNoCopy(&key_buf, &key_buf_len, &value_buf, &value_buf_len);
    key_value_pairs[i].first.assign(key_buf, key_buf_len);
    key_value_pairs[i].second.assign(value_buf, value_buf_len);
    read_progress.Increment();
  }

  std::sort(key_value_pairs.begin(), key_value_pairs.end(), KeyValuePairComparator());
  ProgressBar<uint64> write_progress(reader->Entries());
  for (int i=0; i < num_entries; ++i){
    writer.Add(key_value_pairs[i].first, key_value_pairs[i].second);
    write_progress.Increment();
  }

  /*
  BOOST_FOREACH(std::string name, reader->ListMetadataNames()){
    std::string data;
    reader->GetMetadata(name, &data);
    writer.AddMetadata(name, data);
  }
  */

  return 0;
}

