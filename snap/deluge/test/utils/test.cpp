#include "deluge/deluge.h"
#include "snap/google/glog/logging.h"

using namespace std;
using namespace deluge;

int main(int argc, char *argv[]) {

  /*
  string orig_work_path, new_work_path;

  orig_work_path = "maprfs://10.166.14.11:7222/matchgraph/stage08/output/_temporary/_attempt_201204032001_0019_r_000040_0/matchgraph/stage08/output";
  LOG(INFO) << "orig_work_path: " << orig_work_path;
  new_work_path = CleanupMaprPathFormat(orig_work_path);
  LOG(INFO) << "new_work_path: " << new_work_path;

  orig_work_path = "maprfs://matchgraph/stage08/output/_temporary/_attempt_201204032001_0019_r_000040_0/matchgraph/stage08/output";
  LOG(INFO) << "orig_work_path: " << orig_work_path;
  new_work_path = CleanupMaprPathFormat(orig_work_path);
  LOG(INFO) << "new_work_path: " << new_work_path;
   */

  string input_filename = "maprfs://10.161.38.113:7222/test_mapjoin/reshard_primary/data/part-00012";

  int shard_num;
  CHECK(ParseShardNum(input_filename, &shard_num));
  CHECK_EQ(shard_num, 12);
}

