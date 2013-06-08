#include "snap/pert/stringtable.h"
#include "snap/google/base/stringprintf.h"
#include "snap/google/glog/logging.h"
#include <set>

using namespace std;

// generates input test files for map join test

/// Primary
/// Keys Values
/// K1  primary_1
/// K2  primary_2

/// Secondary
/// Keys Values
/// K1  secondary_1
/// K1  secondary_2
/// K2  secondary_1
/// K2  secondary_2

int main(int argc, char *argv[]) {
  std::string primary_output = "maprfs:///test_mapjoin/input/primary/part-00000";
  std::string secondary_output = "maprfs:///test_mapjoin/input/secondary/part-00000";

  uint64 num_unique_keys = 1e6;

  pert::WriterOptions options;
  options.SetSorted("memcmp");

  {
    pert::StringTableShardWriter writer;
    CHECK(writer.Open(primary_output, options));

    LOG(INFO) << "started writing primary";
    for (int i=0; i < num_unique_keys; ++i){
      writer.Add(StringPrintf("K%d", i), "primary_1");
      writer.Add(StringPrintf("P%d", i), "primary_1");
    }
    writer.Close();
    LOG(INFO) << "done writing primary";
  }

  {
    pert::StringTableShardWriter writer;
    CHECK(writer.Open(secondary_output, options));

    LOG(INFO) << "started writing secondary";
    for (int i=0; i < num_unique_keys; ++i){
      writer.Add(StringPrintf("K%d", i), "secondary_1");
      writer.Add(StringPrintf("K%d", i), "secondary_2");
      writer.Add(StringPrintf("S%d", i), "primary_1");
    }
    writer.Close();
    LOG(INFO) << "done writing secondary";
  }

  return 0;
}

