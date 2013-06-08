#pragma once

#include <vector>
#include <string>
#include "snap/pert/core/cbfile.h"
#include "snap/google/base/macros.h"
#include "snap/google/glog/logging.h"

namespace pert {

// Abstract base class for accessing filesystems with a URI scheme.
class FileSystem {
public:
  FileSystem() {};
  virtual ~FileSystem() {};
  virtual bool GetUriPath(const std::string& uri, std::string* path) = 0;
  virtual std::string GetUriPathOrDie(const std::string& uri) {
    std::string path;
    CHECK(GetUriPath(uri, &path)) << "Invalid uri format: " << uri;
    return path;
  }
  virtual bool Exists(const std::string& uri) = 0;
  virtual bool IsFile(const std::string& uri) = 0;
  virtual bool IsDirectory(const std::string& uri) = 0;
  virtual bool MakeDirectory(const std::string& uri) = 0;
  virtual bool ListDirectory(const std::string& uri, std::vector<std::string>* contents) = 0;
  virtual bool Remove(const std::string& uri) = 0;
  virtual bool FileSize(const std::string& uri, uint64* size) = 0;
  virtual bool ModifiedTimestamp(const std::string& uri, uint64* timestamp) = 0;
  virtual bool SetModifiedTimestamp(const std::string& uri, uint64 timestamp) = 0;
  virtual bool ChunkSize(const std::string& uri, uint64* chunk_size_bytes) = 0;
  virtual InputCodedBlockFile* OpenInput(const std::string& uri) = 0;
  virtual OutputCodedBlockFile* OpenOutput(const std::string& uri, short replication, uint64 chunk_size) = 0;
  virtual bool DownloadUri(const std::string& source_uri, const std::string& local_path) = 0;
  virtual bool CopyLocalToUri(const std::string& local_path, const std::string& destintation_uri) = 0;
  virtual InputFile* OpenInputFile(const std::string& uri) = 0;
  virtual OutputFile* OpenOutputFile(const std::string& uri, short replication, uint64 chunk_size, uint64 desired_modification_time = 0) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(FileSystem);
};


}
