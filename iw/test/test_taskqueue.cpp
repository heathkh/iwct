#include "gtest/gtest.h"
#include "snap/google/glog/logging.h"
#include "iw/taskqueue.hpp"
#include "snap/boost/functional/hash.hpp"
#include "snap/boost/foreach.hpp"
#include <stdlib.h>

std::size_t HashString(const std::string& data){
  boost::hash<std::string> string_hasher;
  return string_hasher(data);
}

std::string GenerateRandomPassword(int length){
  std::string chars(
      "abcdefghijklmnopqrstuvwxyz"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "1234567890"
      "!@#$%^&*()"
      "`~-_=+[{]{\\|;:'\",<.>/? ");

  std::string password;
  for (int i = 0; i < length; ++i) {
    password.push_back(chars[rand() % chars.size()]);
  }
  return password;
}

struct HashStringTaskResult{
  int i;
  std::size_t hash;
};

struct HashStringTask {
  typedef HashStringTaskResult result_type;

  HashStringTask(int i, std::string* data) :
    i_(i),
    data_(data) {
  }

  result_type operator()() {
    result_type result;
    result.i = i_;
    boost::hash<std::string> string_hasher;
    result.hash = string_hasher(*data_);
    return result;
  }

  int i_;
  std::string* data_;
};

TEST(TaskQueue, simple) {
  typedef TaskQueue<HashStringTask> MyTaskQueue;

  // Generate input data
  std::vector<std::string> passwords;
  for (int i=0; i < 100000; ++i){
    passwords.push_back(GenerateRandomPassword(1000));
  }

  std::vector<HashStringTaskResult> results;
  MyTaskQueue queue;
  for (int i=0; i < passwords.size(); ++i){
    HashStringTask task(i, &passwords[i]);
    queue.QueueTask(task);
    while (queue.TasksCompleted()){
      HashStringTaskResult result = queue.GetCompletedTaskResult();
      results.push_back(result);
    }
  }

  while (queue.NumPendingTasks()){
    LOG(INFO) << "waiting...";
    boost::this_thread::sleep(boost::posix_time::milliseconds(10));
  }


  //TODO validate all hashes computed correctly
  BOOST_FOREACH(HashStringTaskResult result, results){
    ASSERT_EQ(result.hash, HashString(passwords[result.i]));
  }

}


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
