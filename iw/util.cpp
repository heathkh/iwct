#include "iw/util.h"
#include "snap/google/base/hashutils.h"
#include <fstream>

namespace iw {

void ParseImagePairKey(const std::string& image_pair_key, uint64* image_id_1, uint64* image_id_2){
  CHECK(image_pair_key.size() == 16);
  *image_id_1 = KeyToUint64(image_pair_key.substr(0,8));
  *image_id_2 = KeyToUint64(image_pair_key.substr(8,16));
}

// Create 16 byte key so that it represents and "undirected edge" between two
// vertices a and b.  The idea is that A->B or B->A map to same key and will be
// adjacent when processing sorted keys
std::string UndirectedEdgeKey(uint64 a, uint64 b){
  std::string edge_key;
  if (a < b){
    edge_key = Uint64ToKey(a) +  Uint64ToKey(b);
  }
  else{
    edge_key = Uint64ToKey(b) +  Uint64ToKey(a);
  }
  return edge_key;
}

bool WriteStringToFile(const std::string& filename, const std::string& str){
  std::ofstream ofs(filename.c_str(), std::ios::binary);
  if (!ofs){
    LOG(WARNING) << "Can't open file: " << filename;
    return false;
  }
  ofs.write((char*)str.data(), str.size());
  return true;
}

} // close namespace iw
