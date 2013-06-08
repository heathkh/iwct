#include "snap/pert/utils.h"
#include "snap/pert/stringtable.h"
#include "snap/boost/foreach.hpp"
#include "snap/progress/progress.hpp"

using namespace std;

namespace pert {

bool MergeTables(const vector<string>& input_uris, const string& output_uri) {
  StringTableReader reader;
  if (!reader.OpenMerge(input_uris)){
    return false;
  }

  StringTableWriter writer;
  if (!writer.Open(output_uri, 1)){
    return false;
  }


  string key, value;
  progress::ProgressBar<int> progress_bar(reader.Entries());
  while (reader.Next(&key, &value)){
    writer.Add(key, value);
    progress_bar.Increment();
  }

  BOOST_FOREACH(const string& name, reader.ListMetadataNames()){
    std::string data;
    reader.GetMetadata(name, &data);
    writer.AddMetadata(name, data);
  }

  return true;
}


} // close namespace

