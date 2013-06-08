#pragma once
#include "snap/pert/core/cbfile.h"
#include "snap/pert/core/fs_base.h"
#include <google/protobuf/io/zero_copy_stream_impl.h>

namespace pert {

class LocalInputFile: public InputFile {
public:
  LocalInputFile();
  virtual ~LocalInputFile();
  virtual bool Open(std::string uri);
  virtual uint64 Size();
  virtual uint64 ModificationTime();
  virtual bool Read(uint32 buffer_size, char* buffer, uint32* bytes_read);

protected:
  int fd_;
  bool is_open_;
  uint64 size_;
  uint64 modification_time_;
  std::string path_;
};


class LocalOutputFile: public OutputFile {
public:
  LocalOutputFile();
  virtual ~LocalOutputFile();
  virtual bool Open(std::string uri, short replication, uint64 chunk_size, uint64 desired_mod_time = 0);
  virtual bool Write(uint32 buffer_size, const char* buffer);
protected:
  int fd_;
  bool is_open_;
  std::string path_;
  std::string uri_;
  uint64 desired_mod_time_;
};


// An InputCodedBlockFile for the local file system.
class LocalInputCodedBlockFile: public InputCodedBlockFile {
public:
  LocalInputCodedBlockFile();
  virtual ~LocalInputCodedBlockFile();
  virtual bool Open(std::string uri);
  virtual uint64 Size();

protected:
  virtual google::protobuf::io::CodedInputStream*
  CreateCodedStream(uint64 position, uint64 length);

  int fd_;
  uint64 size_;
  boost::scoped_ptr<google::protobuf::io::FileInputStream> file_stream_;
  boost::scoped_ptr<google::protobuf::io::LimitingInputStream> limiting_stream_;
};


// An OutputCodedBlockFile for the local file system.
class LocalOutputCodedBlockFile: public OutputCodedBlockFile {
public:
  LocalOutputCodedBlockFile();
  virtual ~LocalOutputCodedBlockFile();
  virtual bool Open(std::string uri, short replication, uint64 chunk_size);

protected:
  virtual google::protobuf::io::CodedOutputStream* CreateCodedStream();
  int fd_;
  boost::scoped_ptr<google::protobuf::io::FileOutputStream> file_stream_;
};


// An implementation of the FileSystem interface for the local filesystem.
class LocalFileSystem : virtual public FileSystem {
public:
  LocalFileSystem();
  virtual ~LocalFileSystem();
  virtual bool GetUriPath(const std::string& uri, std::string* path);
  virtual bool Exists(const std::string& uri);
  virtual bool IsFile(const std::string& uri);
  virtual bool IsDirectory(const std::string& uri);
  virtual bool MakeDirectory(const std::string& uri);
  virtual bool ListDirectory(const std::string& uri, std::vector<std::string>* contents);
  virtual bool Remove(const std::string& uri);
  virtual bool FileSize(const std::string& uri, uint64* size);
  virtual bool ModifiedTimestamp(const std::string& uri, uint64* timestamp);
  virtual bool SetModifiedTimestamp(const std::string& uri, uint64 timestamp);
  virtual bool ChunkSize(const std::string& uri, uint64* chunk_size_bytes);
  virtual InputCodedBlockFile* OpenInput(const std::string& uri);
  virtual OutputCodedBlockFile* OpenOutput(const std::string& uri, short replication, uint64 chunk_size);
  virtual bool DownloadUri(const std::string& source_uri, const std::string& local_path);
  virtual bool CopyLocalToUri(const std::string& local_path, const std::string& destintation_uri);

  virtual InputFile* OpenInputFile(const std::string& uri);
  virtual OutputFile* OpenOutputFile(const std::string& uri, short replication, uint64 chunk_size, uint64 desired_modification_time = 0);

private:
  DISALLOW_COPY_AND_ASSIGN(LocalFileSystem);
};


}
