#include "snap/pert/core/md5.h"

namespace pert {

Md5Digest::Md5Digest(){
  MD5_Init(&md5_context_);
}

void Md5Digest::Update(const char* data, uint32 length){
  MD5_Update(&md5_context_, data, length);
}

std::string to_hex(const unsigned char* data, size_t len){
  static const char* const lut = "0123456789abcdef";
  std::string output;
  output.reserve(2 * len);
  for (size_t i = 0; i < len; ++i){
    const unsigned char c = data[i];
    output.push_back(lut[c >> 4]);
    output.push_back(lut[c & 15]);
  }
  return output;
}

std::string Md5Digest::GetAsHexString(){
  unsigned char digest[MD5_DIGEST_LENGTH];
  MD5_Final(digest, &md5_context_);
  return to_hex(digest, MD5_DIGEST_LENGTH);
}

} // close namespace pert
