#include "snap/pert/stringtable.h"
#include "snap/google/base/stringprintf.h"
#include "snap/google/base/hashutils.h"
#include "snap/google/gflags/gflags.h"
#include "gtest/gtest.h"
#include <set>
#include <vector>
#include <string>
#include "snap/boost/foreach.hpp"
//#include "libhdfs/hdfs.h"
#include <iostream>

using namespace std;

namespace {

/*
TEST(Chunk, libmaprclient) {

  hdfsFS fs = hdfsConnect("default", 0);
  if (!fs) {
      fprintf(stderr, "Oops! Failed to connect to hdfs!\n");
      exit(-1);
  }

  const char* writeFileName = "/mytest/foo.txt";
  tSize fileTotalSize = 10000;
  tSize bufferSize = 512;

  tSize desired_chunk_size = 1024 * (1<<20);

  tSize nameSizeInBytes;
  //hdfsFile writeFile = hdfsOpenFile2(fs, writeFileName, O_WRONLY, bufferSize, 0, 2147483648, &nameSizeInBytes);
  hdfsFile writeFile = hdfsOpenFile(fs, writeFileName, O_WRONLY, bufferSize, 0, desired_chunk_size);
  ASSERT_TRUE(writeFile) << "Failed to open for writing!" << writeFile;

  // data to be written to the file
  char* buffer = (char*)(malloc(sizeof(char) * bufferSize));
  ASSERT_TRUE(buffer) << "Failed to allocate memory!";

  int i = 0;
  for (i=0; i < bufferSize; ++i) {
      buffer[i] = 'a' + (i%26);
  }

  // write to the file
  tSize nrRemaining;

  for (nrRemaining = fileTotalSize; nrRemaining > 0; ) {
      tSize curSize = ( bufferSize < nrRemaining ) ? bufferSize : (int)nrRemaining;
      tSize bytesWritten = hdfsWrite(fs, writeFile, (void*)buffer, curSize);
      ASSERT_EQ(bytesWritten, curSize) << "Write failed!";
      nrRemaining -= curSize;
  }

  free(buffer);
  hdfsCloseFile(fs, writeFile);
  hdfsDisconnect(fs);
}
*/


TEST(Chunk, basic) {
  pert::StringTableWriter writer;
  string test_file_root_uri = "maprfs://tmp/";
  string test_file_uri = test_file_root_uri + "/test.foo";
  uint64 expected_chunk_size = 1024 * (1<<20);
  uint64 actual_chunk_size = 0;

  ASSERT_TRUE(pert::ChunkSize(test_file_root_uri, &actual_chunk_size));
  ASSERT_EQ(expected_chunk_size, actual_chunk_size);

  ASSERT_TRUE(writer.Open(test_file_uri, 1));

  uint64 num = 10000;
  string key, value;
  for (uint64 i = 0; i < num; ++i) {
    key = StringPrintf("k%016llu", i);
    value = StringPrintf("v %s", key.c_str());
    ASSERT_TRUE(writer.Add(key,value));
  }
  writer.Close();

  ASSERT_TRUE(pert::ChunkSize(test_file_uri, &actual_chunk_size));
  ASSERT_EQ(expected_chunk_size, actual_chunk_size);
}


} // close empty namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  return RUN_ALL_TESTS();
}
