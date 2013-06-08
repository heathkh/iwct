#pragma once

#include <string>
#include "snap/google/base/basictypes.h"
#include "openssl/md5.h"

namespace pert {

class Md5Digest {
public:
  Md5Digest();
  void Update(const char* data, uint32 length);
  std::string GetAsHexString();
private:
  MD5_CTX md5_context_;
};

} // close namespace pert
