#include "deluge/util.h"
#include "snap/google/glog/logging.h"
//#include "snap/google/strings/strutil.h"
#include "snap/boost/regex.hpp"
#include "snap/boost/lexical_cast.hpp"
#include "snap/pert/core/ufs.h"

#include "deluge/job.h"
#include "deluge/util.h"
#include "snap/boost/algorithm/string/split.hpp"
#include <map>
#include <string>
#include <iostream>
#include "snap/boost/regex.hpp"
#include "snap/boost/foreach.hpp"
#include "snap/boost/algorithm/string/classification.hpp"
#include "snap/pert/stringtable.h"
#include "snap/boost/filesystem.hpp"

using namespace boost;
using namespace pert;
using namespace std;

namespace fs =  boost::filesystem;

namespace deluge {

// fix work dir
// maprfs:///some_id:some_port/path -> maprfs:///path
std::string CleanupMaprPathFormat(const std::string& orig_work_dir){
  boost::regex re("maprfs://[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\:[0-9]{1,5}(/.*)");

  std::string new_path;
  boost::smatch matches;
  if ( boost::regex_search(orig_work_dir, matches, re)) {
    new_path = std::string("maprfs://") + std::string(matches[1].first, matches[1].second);
  }
  else{
    new_path = orig_work_dir;
  }

  return new_path;
}

std::string StripLeadingZeros(const std::string& in){
  string::size_type nz_pos = in.find_first_not_of("0");
   if (nz_pos != string::npos){
    return in.substr(nz_pos);
   }
   return in;
}

// maprfs:///path/part-00000
// Return true if shard_num can be parsed
bool ParseShardNum(const std::string& path, int* shard_num){
  bool success = false;
  boost::regex re(".*part-([0-9]{5})");

  std::string new_path;
  boost::smatch matches;
  if ( boost::regex_search(path, matches, re)) {
    std::string match(matches[1].first, matches[1].second);
    //LOG(INFO) << "match: " << match;
    try {
      *shard_num = boost::lexical_cast<int>(StripLeadingZeros(match));
      success = true;
    }
    catch(bad_lexical_cast &){
    }
  }

  return success;
}


std::vector<std::string> SplitFileList(const std::string& comma_seperated_files){
  std::vector<std::string> files;
  boost::split(files, comma_seperated_files, boost::is_any_of(","), boost::token_compress_on);

  // filter out strange # characters used as metadata
  regex re("#.*[^,]*");
  BOOST_FOREACH(std::string& file, files){
    file = regex_replace(file, re, "");
  }
  return files;
}

} // close namespace deluge
