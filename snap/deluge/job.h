#pragma once

#include "deluge/pipes/Pipes.hh"
//#include "deluge/pipes/TemplateFactory.hh"
//#include "deluge/std.h"
#include <map>
#include <string>
#include <vector>
#include "snap/google/glog/logging.h"
#include "snap/deluge/deluge.pb.h"


// forward declare pointer types
namespace google {
  namespace protobuf {
    class Message;
  }
}

namespace deluge {

// Wrapper around the HadoopPipes::JobConf class to provide cleaner API.
class JobInfo {
public:
  JobInfo(HadoopPipes::TaskContext& context);

  int GetTaskIdOrDie() const;

  bool Get(const std::string& key, std::string* value) const;
  bool Get(const std::string& key, int* value) const;
  bool Get(const std::string& key, double* value) const;
  bool Get(const std::string& key, google::protobuf::Message* msg) const;

  std::string GetStringOrDie(const std::string& key) const{
    return GetOrDie<std::string>(key);
  }

  int GetIntOrDie(const std::string& key) const {
    return GetOrDie<int>(key);
  }

  double GetDoubleOrDie(const std::string& key) const {
    return GetOrDie<double>(key);
  }

  template <typename Proto>
  Proto GetProtoOrDie(const std::string& key) const {
    return GetOrDie<Proto>(key);
  }

private:

  template <typename T>
  T GetOrDie(const std::string& key) const {
    T value;
    CHECK(Get(key, &value)) << "Key is missing: " << key;
    return value;
  }

  const HadoopPipes::JobConf* jobconf_;
};


class DistributedCacheBase {
public:
  // Returns true if this uri is available by local cache
  bool IsCached(const std::string& uri);

  // Returns local path the cached uri
  std::string GetLocalPathOrDie(const std::string& uri);

  void PrettyPrint();
protected:
  void InitFromStrings(std::string uri_list, std::string local_path_list);
  void RegisterCachedFiles(std::string uri, std::string local_path);
  typedef std::map<std::string, std::string> StringStringTable;
  StringStringTable uri_to_local_path_;
};


class DistributedCacheMock : public DistributedCacheBase {
public:
  DistributedCacheMock(std::string uri_list, std::string local_path_list) {
    InitFromStrings(uri_list, local_path_list);
  }
};

/*
class DistributedCache : public DistributedCacheBase {
public:
  DistributedCache(const JobInfo& info);
};
*/

class MaprDistributedCache {
public:
  MaprDistributedCache(const JobInfo& info);
  bool ResourceIsCached(const std::string& resource_name);
  std::string GetResourceCacheUriOrDie(const std::string& resource_name);

  // Deprecated... prefer above which is more convenient
  bool IsUriCached(const std::string& uri);   // Returns true if this uri is available by cache
  std::string GetCacheUriOrDie(const std::string& uri); // Returns cache uri path

  void PrettyPrint();
protected:

  std::vector<std::string> GetStaleUris();
  bool UpdateCache(const std::string& uri);

  const JobInfo& info_;
  MaprDistributedCacheJob cache_job_;
  std::string rack_topology_;
  bool is_rack_manager_;
};


// If you have data at a uri and a legacy api that only reads local files,
// this will download the uri to the local fs, provide the local path, and
// clean up the tmp copy when destructed.
class UriLocalCache {
public:
  UriLocalCache(std::string uri);
  ~UriLocalCache();
  std::string GetLocalPath();

private:
  std::string uri_;
  std::string local_cache_path_;
};


} // close namespace deluge
