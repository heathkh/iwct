#include "snap/pert/core/ufs.h"
#include "snap/google/base/stringprintf.h"
#include "snap/google/gtest/gtest.h"

using namespace std;
using namespace pert;
namespace {

//TEST(Ufs, TestCopyFileUri) {
//  string test_data_uri = "maprfs://data/test/foo.txt";
//  string copy_uri = "maprfs://data/test/bar.txt";
//  string test_data = "hello world";
//  {
//    boost::scoped_ptr<OutputFile> output_file(OpenOutputFile(test_data_uri, 0, 0));
//    ASSERT_TRUE(output_file.get());
//    ASSERT_TRUE(output_file->Write(test_data.size(), test_data.data()));
//  }
//
//  ASSERT_TRUE(pert::Exists(test_data_uri));
//
//  ASSERT_TRUE(CopyUri(test_data_uri, copy_uri));
//
//
//  boost::scoped_ptr<InputFile> input_file(OpenInputFile(copy_uri));
//  ASSERT_TRUE(input_file.get());
//
//  uint32 read_buffer_size = 1024;
//  char read_buffer[read_buffer_size];
//  uint64 file_size = input_file->Size();
//  CHECK_LE(file_size, read_buffer_size);
//  uint32 num_bytes_read = 0;
//
//  ASSERT_TRUE(input_file->Read(file_size, read_buffer, &num_bytes_read));
//  ASSERT_EQ(file_size, num_bytes_read);
//  string read_value(read_buffer, num_bytes_read);
//  ASSERT_EQ(test_data, read_value);
//}

TEST(Ufs, TestCopyDirectoryMaprUri) {
  string src_dir_uri = "maprfs://data/test/my_dir";
  string test_data_uri = src_dir_uri + "/level1/level2/foo.txt";
  string dst_dir_uri = "maprfs://data/test/copy_of_mydir/";
  string test_data = "hello world";
  {
    boost::scoped_ptr<OutputFile> output_file(OpenOutputFile(test_data_uri, 0, 0));
    ASSERT_TRUE(output_file.get());
    ASSERT_TRUE(output_file->Write(test_data.size(), test_data.data()));
  }

  ASSERT_TRUE(pert::Exists(test_data_uri));
  ASSERT_TRUE(CopyUri(src_dir_uri, dst_dir_uri));

  ASSERT_TRUE(Exists("maprfs://data/test/copy_of_mydir/level1/level2/foo.txt"));

  uint64 t1, t2;
  CHECK(ModifiedTimestamp(test_data_uri, &t1));
  CHECK(ModifiedTimestamp("maprfs://data/test/copy_of_mydir/level1/level2/foo.txt", &t2));
  ASSERT_EQ(t1, t2);
  ASSERT_GT(t1,0);

  CHECK(ModifiedTimestamp(src_dir_uri, &t1));
  CHECK(ModifiedTimestamp(dst_dir_uri, &t2));
  ASSERT_LE(t1, t2);
  ASSERT_GT(t1,0);
}


TEST(Ufs, TestCopyDirectoryLocalUri) {
  string src_dir_uri = "local://tmp/data/test/my_dir";
  string test_data_uri = src_dir_uri + "/level1/level2/foo.txt";
  string dst_dir_uri = "local://tmp/data/test/copy_of_mydir/";
  string test_data = "hello world";
  {
    boost::scoped_ptr<OutputFile> output_file(OpenOutputFile(test_data_uri, 0, 0));
    ASSERT_TRUE(output_file.get());
    ASSERT_TRUE(output_file->Write(test_data.size(), test_data.data()));
  }

  ASSERT_TRUE(pert::Exists(test_data_uri));
  ASSERT_TRUE(CopyUri(src_dir_uri, dst_dir_uri));

  ASSERT_TRUE(Exists("local://tmp/data/test/copy_of_mydir/level1/level2/foo.txt"));

  uint64 t1, t2;
  CHECK(ModifiedTimestamp(test_data_uri, &t1));
  CHECK(ModifiedTimestamp("local://tmp/data/test/copy_of_mydir/level1/level2/foo.txt", &t2));
  ASSERT_EQ(t1, t2);
  ASSERT_GT(t1,0);

  CHECK(ModifiedTimestamp(src_dir_uri, &t1));
  CHECK(ModifiedTimestamp(dst_dir_uri, &t2));
  ASSERT_LE(t1, t2);
  ASSERT_GT(t1,0);
}


bool WriteToTestFile(const std::string& uri, const std::string& data){
  boost::scoped_ptr<OutputFile> output_file(OpenOutputFile(uri, 0, 0));
  CHECK(output_file.get());
  CHECK(output_file->Write(data.size(), data.data()));
  return true;
}

TEST(Ufs, TestComputeMd5DigestForUri) {

  string uri_1 = "local://tmp/data/test/foo1.txt";
  string uri_2 = "local://tmp/data/test/foo2.txt";
  string uri_3 = "local://tmp/data/test/foo3.txt";

  string test_data_a = "hello world";
  string test_data_b = "goodbye world";

  WriteToTestFile(uri_1, test_data_a);
  WriteToTestFile(uri_2, test_data_a);
  WriteToTestFile(uri_3, test_data_b);

  string fp1, fp2, fp3;
  ASSERT_TRUE(ComputeMd5DigestForUri(uri_1, &fp1));
  ASSERT_TRUE(ComputeMd5DigestForUri(uri_2, &fp2));
  ASSERT_TRUE(ComputeMd5DigestForUri(uri_3, &fp3));
  LOG(INFO) << "fp1: " << fp1;
  LOG(INFO) << "fp2: " << fp2;
  LOG(INFO) << "fp3: " << fp3;

  ASSERT_EQ(fp1, fp2);
  ASSERT_NE(fp1, fp3);
}

TEST(Ufs, TestComputeMd5DigestForUris) {

  string uri_1 = "local://tmp/data/test/foo1/";
  string uri_1a = uri_1 + "/a.txt";
  string uri_1b = uri_1 + "/b.txt";
  vector<string> uris_1;
  uris_1.push_back(uri_1a);
  uris_1.push_back(uri_1b);

  string uri_2 = "local://tmp/data/test/foo2/";
  string uri_2a = uri_2 + "/a.txt";
  string uri_2b = uri_2 + "/b.txt";
  vector<string> uris_2;
  uris_2.push_back(uri_2a);
  uris_2.push_back(uri_2b);

  string uri_3 = "local://tmp/data/test/foo3/";
  string uri_3a = uri_3 + "/a.txt";
  string uri_3b = uri_3 + "/b.txt";

  vector<string> uris_3;
  uris_3.push_back(uri_3a);
  uris_3.push_back(uri_3b);

  Remove(uri_1);
  Remove(uri_2);
  Remove(uri_3);

  string test_data_alpha = "hello world";
  string test_data_beta = "goodbye world";

  WriteToTestFile(uri_1a, test_data_alpha);
  WriteToTestFile(uri_1b, test_data_alpha);

  WriteToTestFile(uri_2a, test_data_alpha);
  WriteToTestFile(uri_2b, test_data_alpha);

  WriteToTestFile(uri_3a, test_data_beta);
  WriteToTestFile(uri_3b, test_data_beta);

  string fp1, fp2, fp3;
  ASSERT_TRUE(ComputeMd5DigestForUriSet(uris_1, &fp1));
  ASSERT_TRUE(ComputeMd5DigestForUriSet(uris_2, &fp2));
  ASSERT_TRUE(ComputeMd5DigestForUriSet(uris_3, &fp3));
  LOG(INFO) << "fp1: " << fp1;
  LOG(INFO) << "fp2: " << fp2;
  LOG(INFO) << "fp3: " << fp3;

  ASSERT_EQ(fp1, fp2);
  ASSERT_NE(fp1, fp3);
}

} // close empty namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
