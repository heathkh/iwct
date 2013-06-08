#pragma once

#include <string>
#include "snap/google/base/basictypes.h"

namespace pert {

class CompressionCodec {
public:
  virtual ~CompressionCodec() {};
  virtual uint64 Compress(const char* uncompressed_data,
      uint64 uncompressed_data_size, std::string* compressed_data) = 0;
  virtual uint64 Decompress(const char* compressed_data,
      uint64 compressed_data_size, std::string* uncompressed_data) = 0;
};

class NoCompressionCodec: public CompressionCodec {
public:
  virtual ~NoCompressionCodec() {};
  virtual uint64 Compress(const char* uncompressed_data,
      uint64 uncompressed_data_size, std::string* compressed_data);
  virtual uint64 Decompress(const char* compressed_data,
      uint64 compressed_data_size, std::string* uncompressed_data);
};

class SnappyCompressionCodec: public CompressionCodec {
public:
  virtual ~SnappyCompressionCodec() {};
  virtual uint64 Compress(const char* uncompressed_data,
      uint64 uncompressed_data_size, std::string* compressed_data);
  virtual uint64 Decompress(const char* compressed_data,
      uint64 compressed_data_size, std::string* uncompressed_data);
};

CompressionCodec* CreateCompressionCodec(const std::string& codec_name);

bool ValidCompressionCodecName(const std::string& compression_codec_name);

}

