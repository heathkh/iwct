#pragma once

#include <string>
#include <vector>

namespace deluge {

std::string CleanupMaprPathFormat(const std::string& orig_work_dir);
bool ParseShardNum(const std::string& path, int* shard_num);
std::vector<std::string> SplitFileList(const std::string& comma_seperated_files);

template <typename C, typename T>
bool contains(const C& container, const T& value){
  if (container.find(value) != container.end()){
    return true;
  }
  return false;
}

} // close namespace deluge
