#pragma once

#include "snap/google/base/integral_types.h"
#include "snap/boost/scoped_ptr.hpp"
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/message.h>
#include "snap/google/base/macros.h"

namespace pert {

class InputFile {
public:
  InputFile() {}
  virtual ~InputFile() {}
  virtual bool Open(std::string uri) = 0;
  virtual uint64 Size() = 0;
  virtual uint64 ModificationTime() = 0;
  virtual bool Read(uint32 buffer_size, char* buffer, uint32* bytes_read) = 0;
  virtual bool ReadToString(std::string* data);

private:
  DISALLOW_COPY_AND_ASSIGN(InputFile);
};

class OutputFile {
public:
  OutputFile() {}
  virtual ~OutputFile() {}
  virtual bool Open(std::string uri, short replication, uint64 chunk_size, uint64 desired_mod_time = 0) = 0;
  virtual bool Write(uint32 buffer_size, const char* buffer) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(OutputFile);
};

class InputCodedBlockFile {
public:
  InputCodedBlockFile();
  virtual ~InputCodedBlockFile();

  virtual bool Open(std::string uri) = 0;
  virtual uint64 Size() = 0;

  bool OpenBlock(uint64 position, uint64 length);
  bool Read(std::string* out, int size);
  bool Read(google::protobuf::Message* proto);
  void CloseBlock();

protected:

  virtual google::protobuf::io::CodedInputStream* CreateCodedStream(
      uint64 position, uint64 length) = 0;
  boost::scoped_ptr<google::protobuf::io::CodedInputStream> coded_stream_;
  bool is_open_;

private:
  DISALLOW_COPY_AND_ASSIGN(InputCodedBlockFile);
};

class OutputCodedBlockFile {
public:
  OutputCodedBlockFile();
  virtual ~OutputCodedBlockFile();

  virtual bool Open(std::string uri, short replication, uint64 chunk_size) = 0;

  uint64 OpenBlock();
  void Write(const std::string& out);
  void Write(uint64 out);
  void Write(const google::protobuf::Message& proto);
  uint64 CloseBlock();

  std::string GetUri();

protected:
  virtual google::protobuf::io::CodedOutputStream* CreateCodedStream() = 0;
  boost::scoped_ptr<google::protobuf::io::CodedOutputStream> coded_stream_;
  uint64 bytes_written_;
  bool is_open_;
  std::string uri_;

private:
  DISALLOW_COPY_AND_ASSIGN(OutputCodedBlockFile);
};

}
