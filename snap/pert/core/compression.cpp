#include "snap/pert/core/compression.h"
#include "snap/snappy/snappy.h"
#include "snap/google/glog/logging.h"
#include "snap/google/base/basictypes.h"
#include "snap/boost/scoped_ptr.hpp"


namespace pert {

uint64 NoCompressionCodec::Compress(const char* uncompressed_data,
    uint64 uncompressed_data_size, std::string* compressed_data) {
  compressed_data->assign(uncompressed_data, uncompressed_data_size);
  return uncompressed_data_size;
}

uint64 NoCompressionCodec::Decompress(const char* compressed_data,
    uint64 compressed_data_size, std::string* uncompressed_data) {
  uncompressed_data->assign(compressed_data, compressed_data_size);
  return compressed_data_size;
}

uint64 SnappyCompressionCodec::Compress(const char* uncompressed_data,
    uint64 uncompressed_data_size, std::string* compressed_data) {
  return snappy::Compress(uncompressed_data, uncompressed_data_size,
      compressed_data);
}

uint64 SnappyCompressionCodec::Decompress(const char* compressed_data,
    uint64 compressed_data_size, std::string* uncompressed_data) {
  return snappy::Uncompress(compressed_data, compressed_data_size,
      uncompressed_data);
}

CompressionCodec* CreateCompressionCodec(const std::string& codec_name) {
  CompressionCodec* codec = NULL;
  if (codec_name == "none") {
      codec = new NoCompressionCodec;
  }
  else if (codec_name == "snappy") {
    codec = new SnappyCompressionCodec;
  } else {
    LOG(ERROR) << "unknown codec name: " << codec_name;
  }
  return codec;
}


bool ValidCompressionCodecName(const std::string& compression_codec_name) {
  boost::scoped_ptr<CompressionCodec> codec;
  codec.reset(CreateCompressionCodec(compression_codec_name));
  return codec.get() != NULL;
}



} // close namespace
