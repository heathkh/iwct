#include "snap/pert/pert.h"
#include "snap/google/gflags/gflags.h"
#include "snap/google/base/hashutils.h"
#include <iostream>
#include "snap/boost/foreach.hpp"
#include "snap/progress/progress.hpp"

using namespace std;
using namespace progress;

DEFINE_string(input, "", "input path");
DEFINE_string(output, "", "output path");
DEFINE_bool(sorted, true, "required sorted read and write");
DEFINE_double(new_block_size_mb, -1.0, "desired block size for output file in megabytes");
DEFINE_int32(num_output_shards, -1, "desired number of shards for output file");

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

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  string usage("This program creates a modified copy of a sharded PERT StringTable or ProtoTable file. \nSample usage: pertedit --input=<input path> --output=<output path> \nUse --help for more info.\n");
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

  if (FLAGS_sorted){
    reader.reset(new pert::StringTableReader());
  }
  else{
    reader.reset(new pert::StringTableShardSetReader());
  }

  CHECK(reader->Open(input_uri));
  pert::WriterOptions options;
  uint64 block_size_bytes = FLAGS_new_block_size_mb * (1 << 20);
  options.SetBlockSize(block_size_bytes);

  if (!FLAGS_sorted){
    options.SetUnsorted();
  }
  pert::StringTableWriter writer;
  LOG(INFO) << "output_uri: " << output_uri;
  CHECK(writer.Open(output_uri, FLAGS_num_output_shards, options));
  ProgressBar<uint64> progress(reader->Entries());
  std::string key, value;
  while(reader->Next(&key, &value)){
    writer.Add(key, value);
    progress.Increment();
  }

  //TODO(heathkh):  Generic unordered writer may not support metadata if shards come from different tables... how to deal with this?
  BOOST_FOREACH(std::string name, reader->ListMetadataNames()){
    std::string data;
    reader->GetMetadata(name, &data);
    writer.AddMetadata(name, data);
  }

  return 0;
}

