#pragma once

#ifdef PERT_USE_MAPR_FS

#include "snap/pert/core/fs_base.h"
#include "snap/pert/core/cbfile.h"
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "libhdfs/hdfs.h"

namespace pert {



std::vector<std::string> MaprGetShardUris(std::string uri);

class MaprInputFile: public InputFile {
public:
  MaprInputFile();
  virtual ~MaprInputFile();
  virtual bool Open(std::string uri);
  virtual uint64 Size();
  virtual uint64 ModificationTime();
  virtual bool Read(uint32 buffer_size, char* buffer, uint32* bytes_read);

protected:
  bool is_open_;
  uint64 size_;
  uint64 modification_time_;
  hdfsFS fs_;
  hdfsFile file_;
  std::string path_;
  uint32 hdfs_buffer_size_;
};

class MaprOutputFile: public OutputFile {
public:
  MaprOutputFile();
  virtual ~MaprOutputFile();
  virtual bool Open(std::string uri, short replication, uint64 chunk_size, uint64 desired_mod_time = 0);
  virtual bool Write(uint32 buffer_size, const char* buffer);
protected:
  bool is_open_;
  hdfsFS fs_;
  hdfsFile file_;
  std::string path_;
  std::string uri_;
  uint32 hdfs_buffer_size_;
  uint64 desired_mod_time_;
};


class MaprCopyingInputStream;

class MaprInputCodedBlockFile: public InputCodedBlockFile {
public:
  MaprInputCodedBlockFile();
  virtual ~MaprInputCodedBlockFile();
  virtual bool Open(std::string uri);
  virtual uint64 Size();

protected:
  virtual google::protobuf::io::CodedInputStream*
  CreateCodedStream(uint64 position, uint64 length);

  uint64 size_;

  hdfsFS fs_;
  hdfsFile file_;
  std::string path_;

  boost::scoped_ptr<MaprCopyingInputStream> copying_input_stream_;
  boost::scoped_ptr<google::protobuf::io::CopyingInputStreamAdaptor> copying_stream_;
  boost::scoped_ptr<google::protobuf::io::LimitingInputStream> limiting_stream_;

  friend class MaprCopyingInputStream;
};

class MaprCopyingInputStream : public google::protobuf::io::CopyingInputStream  {
public:
  MaprCopyingInputStream(MaprInputCodedBlockFile* input );
  virtual ~MaprCopyingInputStream();

  virtual int Read(void * buffer, int size);
  virtual int Skip(int count);

private:
  MaprInputCodedBlockFile* input_;
};


class MaprCopyingOutputStream;

class MaprOutputCodedBlockFile: public OutputCodedBlockFile {
public:
  MaprOutputCodedBlockFile();
  virtual ~MaprOutputCodedBlockFile();
  virtual bool Open(std::string uri, short replication, uint64 chunk_size);

protected:
  virtual google::protobuf::io::CodedOutputStream* CreateCodedStream();

  std::string path_;
  hdfsFS fs_;
  hdfsFile file_;
  boost::scoped_ptr<MaprCopyingOutputStream> copying_output_stream_;
  boost::scoped_ptr<google::protobuf::io::CopyingOutputStreamAdaptor> output_stream_;

  friend class MaprCopyingOutputStream;
};


class MaprCopyingOutputStream : public google::protobuf::io::CopyingOutputStream {
public:
  MaprCopyingOutputStream(MaprOutputCodedBlockFile* output);
  virtual ~MaprCopyingOutputStream();
  virtual bool Write(const void * buffer, int size);

private:
  MaprOutputCodedBlockFile* output_; // does not own this object
};



// An implementation of the FileSystem interface for the MapR filesystem.
class MaprFileSystem : virtual public FileSystem {
public:
  MaprFileSystem();
  virtual ~MaprFileSystem();
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
  DISALLOW_COPY_AND_ASSIGN(MaprFileSystem);
};



}


#endif
