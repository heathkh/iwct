#include "gtest/gtest.h"
#include "snap/google/glog/logging.h"
#include "snap/scul/scul.hpp"

#include <vector>
#include <set>
#include <map>

TEST(TestSCUL, vector_for_each) {
  std::vector<int> items;
  for (int i=0; i < 10; ++i){
    items.push_back(i);
  }
  int index = 0;
  scul_for_each(int item, items){
    ASSERT_EQ(items[index], item);
    ++index;
  }
  ASSERT_EQ(index, items.size());
}

TEST(TestSCUL, set_for_each) {
  std::set<int> items;

  items.insert(1);
  items.insert(3);
  items.insert(3);
  items.insert(3);
  items.insert(3);
  items.insert(4);

  int num_items = 0;
  scul_for_each(int item, items){
    ++num_items;
  }
  ASSERT_EQ(num_items, items.size());
}

TEST(TestSCUL, map_for_each) {
  std::vector<int> keys;
  keys.push_back(1);
  keys.push_back(2);
  keys.push_back(3);

  std::vector<int> values;
  values.push_back(20);
  values.push_back(30);
  values.push_back(40);

  std::map<int, int> map;
  map[1] = 10;
  map[1] = 20; // overwrites
  map[2] = 30;
  map[3] = 40;

  ASSERT_EQ(map.size(), 3);

  int index = 0;
  scul_for_each_value(int value, map){
    ASSERT_EQ(values[index], value);
    ++index;
  }
  ASSERT_EQ(index, map.size());

  index = 0;
  scul_for_each_key(int key, map){
    ASSERT_EQ(keys[index], key);
    ++index;
  }
  ASSERT_EQ(index, map.size());

}


TEST(TestSCUL, sum_vector) {
  std::vector<int> items;
  items.push_back(1);
  items.push_back(3);
  items.push_back(3);
  items.push_back(3);
  items.push_back(3);
  items.push_back(4);
  ASSERT_EQ(scul::Sum(items), 17);
}

TEST(TestSCUL, sum_set) {
  std::set<int> items;
  items.insert(1);
  items.insert(3);
  items.insert(3);
  items.insert(3);
  items.insert(3);
  items.insert(4);
  ASSERT_EQ(scul::Sum(items), 8);
}


TEST(TestSCUL, mean_vector_int) {
  std::vector<int> items;
  items.push_back(1);
  items.push_back(3);
  items.push_back(3);
  items.push_back(3);
  items.push_back(3);
  items.push_back(4);
  double true_mean = 2.8333333333333335;
  EXPECT_DOUBLE_EQ(scul::Mean(items), true_mean);
}

TEST(TestSCUL, mean_vector_double) {
  std::vector<int> items;
  items.push_back(1.0);
  items.push_back(3.0);
  items.push_back(3.0);
  items.push_back(3.0);
  items.push_back(3.0);
  items.push_back(4.0);
  double true_mean = 2.8333333333333335;
  EXPECT_DOUBLE_EQ(scul::Mean(items), true_mean);
}



TEST(TestSCUL, contains) {
  std::set<int> items;

  items.insert(1);
  items.insert(3);
  items.insert(3);
  items.insert(3);
  items.insert(3);
  items.insert(4);

  ASSERT_TRUE(scul::Contains(items, 1));
  ASSERT_TRUE(scul::Contains(items, 3));
  ASSERT_TRUE(scul::Contains(items, 4));
  ASSERT_FALSE(scul::Contains(items, 5));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
