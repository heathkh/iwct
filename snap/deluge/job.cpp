#include "snap/base64/decode.h"
#include "snap/boost/algorithm/string/classification.hpp"
#include "snap/boost/algorithm/string/split.hpp"
#include "snap/boost/asio.hpp"
#include "snap/boost/filesystem.hpp"
#include "snap/boost/foreach.hpp"
#include "snap/boost/regex.hpp"
#include "snap/boost/regex.hpp"
#include "snap/deluge/job.h"
#include "snap/deluge/util.h"
#include "snap/google/base/stringprintf.h"
#include "snap/google/glog/logging.h"
#include "snap/pert/core/ufs.h"
#include "snap/pert/stringtable.h"
#include <google/protobuf/message.h>
#include <iostream>
#include <map>
#include <sstream>
#include <sstream>
#include <string>

using namespace boost;
using namespace pert;
using namespace std;

namespace fs =  boost::filesystem;

using namespace std;

namespace deluge {

JobInfo::JobInfo(HadoopPipes::TaskContext& context) :
  jobconf_(context.getJobConf()) {
  CHECK(jobconf_);
}

bool JobInfo::Get(const std::string& key, std::string* value) const {
  if (!jobconf_->hasKey(key)){
    return false;
  }
  value->assign(jobconf_->get(key));
  return true;
}


bool JobInfo::Get(const std::string& key, int* value) const {
  if (!jobconf_->hasKey(key)){
    return false;
  }
  *value = jobconf_->getInt(key);
  return true;
}

bool JobInfo::Get(const std::string& key, double* value) const {
  if (!jobconf_->hasKey(key)){
    return false;
  }
  *value = jobconf_->getFloat(key);
  return true;
}

bool JobInfo::Get(const std::string& key, google::protobuf::Message* msg) const {
  std::string base64_encoded_data = GetStringOrDie(key);
  std::istringstream in_stream(base64_encoded_data);
  std::ostringstream out_stream;
  base64::decoder decoder;
  decoder.decode(in_stream, out_stream);
  if (!msg->ParseFromString(out_stream.str())){
    LOG(FATAL) << "got proto of wrong type";
    return false;
  }
  if (!msg->IsInitialized()){
    LOG(FATAL) << "proto not initialized";
    return false;
  }
  return true;
}

int JobInfo::GetTaskIdOrDie() const {
    CHECK(jobconf_->hasKey("mapred.tip.id"));
    string task_string = jobconf_->get("mapred.tip.id");

    int task_id = -1;

    using namespace boost;
    regex expression("task_(.*)_(.*)_(.*)_([0-9]*)");

    boost::smatch what;
    if(regex_match(task_string, what, expression)){
      task_id = atoi(what[4].str().c_str());
    }

    CHECK_GE(task_id, 0) << "failed to parse task id string: " << task_string;

    return task_id;
}


void DistributedCacheBase::PrettyPrint(){

  BOOST_FOREACH(StringStringTable::value_type t, uri_to_local_path_){
    LOG(INFO) << t.first << " -> " << t.second;
  }
}

void DistributedCacheBase::InitFromStrings(std::string uri_list, std::string local_path_list){
  std::vector<std::string> uris = SplitFileList(uri_list);
  std::vector<std::string> local_paths = SplitFileList(local_path_list);

  CHECK_EQ(uris.size(), local_paths.size());
  int num_files = local_paths.size();
  for(int i=0; i < num_files; ++i){
    std::string canonical_uri = CanonicalizeUri(uris[i]);
    uri_to_local_path_[canonical_uri] = local_paths[i];
  }

  PrettyPrint();
}

// Returns true if this uri is available by local cache
bool DistributedCacheBase::IsCached(const std::string& uri){
  return contains(uri_to_local_path_, CanonicalizeUri(uri));
}

// Returns local path the cached uri
std::string DistributedCacheBase::GetLocalPathOrDie(const std::string& uri){
  std::string canonical_uri = CanonicalizeUri(uri);
  StringStringTable::iterator iter = uri_to_local_path_.find(canonical_uri);
  if (iter == uri_to_local_path_.end()){
    PrettyPrint();
    LOG(FATAL) << "There is no cache entry for requested uri: " << uri << " (cannoical: " << canonical_uri << ")";
  }
  return iter->second;
}

/*
DistributedCache::DistributedCache(const JobInfo& info){
  std::string mapred_cache_files = info.GetStringOrDie("mapred.cache.files");
  LOG(INFO) << "mapred.cache.files: " << mapred_cache_files;
  std::string mapred_cache_localFiles = info.GetStringOrDie("mapred.cache.localFiles");
  LOG(INFO) << "mapred.cache.localFiles: " << mapred_cache_localFiles;
  InitFromStrings(mapred_cache_files, mapred_cache_localFiles);
}
*/

///////////////////////////////////////////////////////////////////////////////

std::string GetMyIp() {
  boost::asio::io_service io_service;
  boost::asio::ip::tcp::resolver resolver(io_service);
  boost::asio::ip::tcp::resolver::query query(boost::asio::ip::host_name(), "");
  boost::asio::ip::tcp::resolver::iterator it = resolver.resolve(query);
  boost::asio::ip::tcp::endpoint endpoint = *it;
  return endpoint.address().to_string();
}



MaprDistributedCache::MaprDistributedCache(const JobInfo& info) : info_(info), is_rack_manager_(false){
  cache_job_ = info.GetProtoOrDie<MaprDistributedCacheJob>("mapr_distributed_cache_job");
  std::string my_ip = GetMyIp();

  BOOST_FOREACH(const MaprDistributedCacheJob_Rack& rack, cache_job_.racks()){
    BOOST_FOREACH(const std::string& ip, rack.member_ips()){
      if (ip == my_ip){
        rack_topology_ = rack.topology();
        break;
      }
    }
    if (!rack_topology_.empty()){
      break;
    }
  }
  CHECK(!rack_topology_.empty());
  LOG(INFO) << "rack_topology_: " << rack_topology_;

  string manager_working_uri = StringPrintf("maprfs://data/deluge/cache/config/topo/%s/%s/_MANAGER_WORKING", rack_topology_.c_str(), cache_job_.name().c_str());
  string manager_done_uri = StringPrintf("maprfs://data/deluge/cache/config/topo/%s/%s/_MANAGER_DONE", rack_topology_.c_str(), cache_job_.name().c_str());

  string task_string = info.GetStringOrDie("mapred.tip.id");
  LOG(INFO) << "task_string: " << task_string;
  if (!Exists(manager_working_uri)){
    {
      boost::scoped_ptr<OutputFile> manager_working_file(OpenOutputFile(manager_working_uri));
      CHECK(manager_working_file->Write(task_string.size(), task_string.data()));
      uint64 backoff = ((info.GetTaskIdOrDie() % 10) / 10.0)* 10.0e6;
      LOG(INFO) << "backoff: " << backoff;
      usleep(backoff); // give other contenders a chance to write their values
      sleep(1);
    }

    boost::scoped_ptr<InputFile> manager_working_file(OpenInputFile(manager_working_uri));
    CHECK(manager_working_file.get());
    string winner_task_string;
    CHECK(manager_working_file->ReadToString(&winner_task_string));
    if (task_string == winner_task_string){
      is_rack_manager_ = true;
    }
  }

  LOG(INFO) << "is_rack_manager_: " << is_rack_manager_;

  if (is_rack_manager_){
    vector<string> stale_uris = GetStaleUris();
    LOG(INFO) << "num_stale_uris: " << stale_uris.size();
    BOOST_FOREACH(const std::string& stale_uri, stale_uris) {
      UpdateCache(stale_uri);
    }

    // finally, create flag file to signal to others that we are done refreshing the cache and they can proceed
    boost::scoped_ptr<OutputFile> manager_done_file(OpenOutputFile(manager_done_uri));
  }

  // wait for rack manager to indicate it is done
  while (!Exists(manager_done_uri)){
    LOG(INFO) << "waiting for cache manager to finish...";
    sleep(10);
  }
}


bool MaprDistributedCache::ResourceIsCached(const std::string& resource_name){
  string param_name = "deluge_cache_"+resource_name;
  string src_uri;
  if (!info_.Get(param_name, &src_uri)){
    LOG(INFO) << "No resource was cached with name: " << resource_name;
    return false;
  }

  return IsUriCached(src_uri);
}

std::string MaprDistributedCache::GetResourceCacheUriOrDie(const std::string& resource_name){
  string param_name = "deluge_cache_"+resource_name;
  string src_uri;
  if (!info_.Get(param_name, &src_uri)){
    LOG(FATAL) << "No resource was cached with name: " << resource_name;
  }
  return GetCacheUriOrDie(src_uri);
}

bool MaprDistributedCache::UpdateCache(const std::string& uri){
  string cache_uri = GetCacheUriOrDie(uri);
  LOG(INFO) << "updating cache: " << uri << " -> " << cache_uri;
  return CopyUri(uri, cache_uri);
}


vector<string> MaprDistributedCache::GetStaleUris(){
  vector<string> stale_uris;

  BOOST_FOREACH(const std::string& src_uri, cache_job_.uris()){
    string cache_uri = GetCacheUriOrDie(src_uri);

    if (!Exists(cache_uri)){
      LOG(INFO) << "not yet cached: " << src_uri;
      LOG(INFO) << "expected at location: " << cache_uri;
      stale_uris.push_back(src_uri);
      continue;
    }

    //TODO(heathkh): this is a hack... instead you should properly recurse on directories to check for stale files here or in an outer loop
    if (IsDirectory(cache_uri)){
      stale_uris.push_back(src_uri);
      continue;
    }

    // Check if the src file has different size than cached file
    uint64 src_size, cache_size;
    CHECK(FileSize(src_uri, &src_size));
    CHECK(FileSize(cache_uri, &cache_size));
    if (cache_size != src_size || cache_size == 0){
      LOG(INFO) << "cache is stale: " << src_uri;
      LOG(INFO) << "src_size:   " << src_size;
      LOG(INFO) << "cache_size: " << cache_size;
      stale_uris.push_back(src_uri);
      continue;
    }


    // Check if the src file is newer than the cached file
    uint64 src_mod_time, cache_mod_time;
    CHECK(ModifiedTimestamp(src_uri, &src_mod_time));
    CHECK(ModifiedTimestamp(cache_uri, &cache_mod_time));
    if (src_mod_time > cache_mod_time){
      LOG(INFO) << "cache is stale: " << src_uri;
      LOG(INFO) << "src_mod_time:   " << src_mod_time;
      LOG(INFO) << "cache_mod_time: " << cache_mod_time;
      stale_uris.push_back(src_uri);
    }
  }
  return stale_uris;
}

// Returns true if this uri is available by cache
bool MaprDistributedCache::IsUriCached(const std::string& query_uri){
  string canonical_query_uri = CanonicalizeUri(query_uri);
  BOOST_FOREACH(const std::string& cached_uri, cache_job_.uris()){
    if (CanonicalizeUri(cached_uri) == canonical_query_uri){
      return true;
    }
  }
  return false;
}

// Returns cache uri path
std::string MaprDistributedCache::GetCacheUriOrDie(const std::string& uri){
  string scheme, path;
  CHECK(ParseUri(uri, &scheme, &path));
  CHECK_EQ(scheme, "maprfs");
  string cache_uri = StringPrintf("maprfs://data/deluge/cache/%s/%s", rack_topology_.c_str(), path.c_str());
  return cache_uri;
}

void MaprDistributedCache::PrettyPrint(){

}



UriLocalCache::UriLocalCache(std::string uri){
  string scheme, path, error;
  CHECK(ParseUri(uri, &scheme, &path, &error));
  //TODO(heathkh): change instances to put ephemeral local under /tmp so we don't need a mysterious magic path
  std::string local_scratch_path = "/opt/mapr/logs/cache/tmp/";  // magic path to a dir mounted with a ephemeral disc with ~200 GB storage
  local_cache_path_ = local_scratch_path + fs::unique_path().string();
  LOG(INFO) << "Downloading data from " << uri;
  CHECK(CopyUri(uri, "local://" + local_cache_path_));
  LOG(INFO) << "Done downloading index data";
}

UriLocalCache::~UriLocalCache(){
  fs::remove_all(local_cache_path_);
}

std::string UriLocalCache::GetLocalPath(){
  return local_cache_path_;
}



} // close namespace deluge
