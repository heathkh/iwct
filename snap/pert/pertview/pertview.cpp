#include "snap/pert/stringtable.h"
#include "snap/pert/prototable.h"
#include "snap/google/gflags/gflags.h"
#include "snap/google/base/hashutils.h"
#include "snap/google/base/stringprintf.h"
#include "snap/boost/filesystem.hpp"
#include "snap/boost/foreach.hpp"
#include "snap/boost/algorithm/string.hpp"
#include <iostream>

using namespace std;

namespace fs =  boost::filesystem;

DEFINE_bool(describe, true, "print description of contents");
DEFINE_bool(keys, false, "print keys");
DEFINE_string(key_format, "", "pretty print using one of several standard key formats (key, keypair, hex, guess");
DEFINE_bool(values, false, "print values");
DEFINE_string(fields, "", "comma seperated list of field names to print (valid for ProtoTable)");
DEFINE_int64(limit, -1, "max number of entries to print");
DEFINE_bool(debug, false, "print debug level description of contents");
DEFINE_bool(metadata, false, "print metadata key value pairs");


int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  string usage("This program prints information about a sharded PERT StringTable or ProtoTable file. \nSample usage: pertview <path to pert file parts dir> \nUse --help for more info.\n");
  google::SetUsageMessage(usage);
  if (argc < 2){
    cout << google::ProgramUsage() << endl;
    return 1;
  }


  string uri(argv[1]);

  std::string scheme;
  std::string path;
  if(!pert::ParseUri(uri, &scheme, &path)){
    LOG(INFO) << "no scheme provided... Assuming local://";
    uri = "local://" + fs::system_complete(uri).string();
  }

  LOG(INFO) << "opening: " << uri;

  pert::StringTableReader reader;
  CHECK(reader.Open(uri));

  pert::ProtoTablePrettyPrinter proto_pretty_printer;

  bool is_proto_table = proto_pretty_printer.Init(reader);

  if (!is_proto_table && !FLAGS_fields.empty()){
    LOG(ERROR) << "This doesn't seem to be a ProtoTable... --fields option can't be used here.";
    return 1;
  }

  if (FLAGS_describe && ! FLAGS_debug){
    cout << "Entries: " << reader.Entries() << endl;
    if (is_proto_table){
      cout << "Proto: " << proto_pretty_printer.ProtoDescription() << endl;
    }
    std::string fingerprint;
    CHECK(pert::GetShardSetFingerprint(uri, &fingerprint));
    cout << "Fingerprint: " << fingerprint << endl;
  }

  if (FLAGS_debug){
    cout << "Sorted: " << reader.IsSorted() << endl;
    cout << "Entries: " << reader.Entries() << endl;
    cout << "MinBlockSize: " << reader.MinBlockSize() << endl;
    cout << "Compression Ratio: " << reader.CompressionRatio() << endl;
    cout << "Bloom Filter: " << reader.HasBloomFilter() << endl;
    cout << reader.ShardInfo() << endl;
    if (is_proto_table){
      cout << "Proto: " << proto_pretty_printer.ProtoDescription() << endl;
    }
  }

  if (FLAGS_metadata) {
    std::string value;
    BOOST_FOREACH(const std::string& key, reader.ListMetadataNames()){
      CHECK(reader.GetMetadata(key, &value));
      cout << "key: " << key << endl;
      cout << "value: " << value << endl;
    }

  }

  if (FLAGS_keys || FLAGS_values){
    string key, value;

    int64 count = 0;
    while (reader.Next(&key, &value)){

      if (FLAGS_keys){
        if (!FLAGS_key_format.empty()){

          if (FLAGS_key_format == "guess"){
            if (key.size() == 8){
              FLAGS_key_format = "key";
            }
            else if (key.size() == 16){
              FLAGS_key_format = "keypair";
            }
            else{
              LOG(FATAL) << "Can't guess the key format for key with length: " << key.size();
            }
          }

          if (FLAGS_key_format == "key"){
            cout << "key: " << KeyToUint64(key) << endl;
          }
          else if (FLAGS_key_format == "keypair"){
            cout << "key: " << KeyToUint64(key.substr(0,8)) << " " << KeyToUint64(key.substr(8,16)) << endl;
          }
          else if (FLAGS_key_format == "hex"){
            cout << "key(" << key.size() << "): " << pert::BytesToHexString(key) << endl;
          }
          else if (FLAGS_key_format == "double"){
            CHECK_EQ(key.size(), 8);
            double value = KeyToDouble(key);
            cout << "key: "  << value << endl;
          }
          else{
            cout << "key: " << key << endl;
          }
        }
        else{
          cout << "key: " << key << endl;
        }

      }

      if (FLAGS_values){

        if (is_proto_table){
          if (FLAGS_fields.empty()){
            cout << proto_pretty_printer.Proto(value) << endl;
          }
          else {
            std::vector<std::string> field_names;
            //SplitStringUsing(FLAGS_fields, ",", &field_names);

            boost::split(field_names, FLAGS_fields, boost::is_any_of(","));

            cout << proto_pretty_printer.ProtoFields(value, field_names) << endl;
          }
        }
        else{
          cout << value << endl;
        }
      }

      count++;
      if (FLAGS_limit > 0 && count >= FLAGS_limit){
        LOG(INFO) << "reached requested limit: " << FLAGS_limit;
        break;
      }
    }
  }

  return 0;
}
