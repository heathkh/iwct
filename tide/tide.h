#pragma once

#include <string>
#include <vector>
#include <map>
#include <set>
#include "snap/google/base/integral_types.h"
#include "snap/boost/unordered_map.hpp"
#include "snap/boost/unordered_set.hpp"
#include "tide/tide.pb.h"

namespace tide {

#define UNKNOWN_OBJECT kuint32max

// Store tide dataset to a pert file... Used mainly for unit testing.
bool CreateTideDataset(const ObjectList& list, const std::string& output_uri);

// Represents a set of tide object classes
class TideDataset {
public:
  struct ObjectData {
  public:
    uint32 id;
    std::string name;
    std::vector<uint64> primary;  // images known to have object label
    std::vector<uint64> aux; // images for which we don't know if the object label is there
    std::vector<uint64> neg; // images which are known not to contain a sufficiently good example of the object
  };

  TideDataset(const std::string& tide_uri);
  std::vector<uint32> GetObjectIds() const;
  std::vector<uint64> GetImageIds() const;
  const TideDataset::ObjectData& GetObject(uint32 object_id) const;
  uint32 GetObjectId(uint64 image_id) const;
  bool GenerateTrial(int num_training_images,  float frac_aux_images, ExperimentTrial* trial);
  bool ImageIsPrimary(uint64 image_id);
private:
  typedef boost::unordered_map<uint32, ObjectData> ObjectIdToObjectData;
  ObjectIdToObjectData objectid_to_objectdata_;
  typedef boost::unordered_map<uint64, uint32> ImageIdToObjectId;
  ImageIdToObjectId imageid_to_objectid_;
  typedef boost::unordered_set<uint64> ImageSet;
  ImageSet primary_images_;
};

} // close namespace tide


