#include "snap/google/gtest/gtest.h"
#include "snap/deluge/job.h"
#include "snap/google/glog/logging.h"
#include <iostream>
#include <string>

using namespace std;
using namespace deluge;

namespace {

TEST(DistributedCache, HelperFunctions) {
  std::string cached_uri_list("maprfs:///dcache/write/./mr_distributed_cache,maprfs:/test2/secondary.pert#secondary.pert,maprfs:/foo#foo,maprfs:/bar/#");
  std::string local_file_paths("/tmp/mapr-hadoop/mapred/local/taskTracker/distcache/498505952428535075_-339096036_609361331/maprfs/dcache/write/mr_distributed_cache,/tmp/mapr-hadoop/mapred/local/taskTracker/distcache/4550479286643524542_-903507197_609364825/maprfs/test2/secondary.pert,/tmp/mapr-hadoop/mapred/local/taskTracker/distcache/498505952428535075_-339096036_609361331/maprfs/dcache/write/mr_distributed_cache,/tmp/mapr-hadoop/mapred/local/taskTracker/distcache/4550479286643524542_-903507197_609364825/maprfs/test2/secondary.pert");

  DistributedCacheMock cache(cached_uri_list, local_file_paths);

  ASSERT_TRUE(cache.IsCached("maprfs://////test2////secondary.pert/."));
  ASSERT_TRUE(cache.IsCached("maprfs:/test2/secondary.pert"));
  ASSERT_TRUE(cache.IsCached("maprfs:/foo"));
  ASSERT_TRUE(cache.IsCached("maprfs:/bar/"));
  ASSERT_FALSE(cache.IsCached("maprfs:/test5/secondary.pert"));
  ASSERT_EQ(cache.GetLocalPathOrDie("maprfs:///test2/secondary.pert//"), "/tmp/mapr-hadoop/mapred/local/taskTracker/distcache/4550479286643524542_-903507197_609364825/maprfs/test2/secondary.pert");
}

} // close empty namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
