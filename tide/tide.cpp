#include "tide/tide.h"
#include "snap/pert/pert.h"
#include "tide/tide.pb.h"
#include "snap/scul/scul.hpp"


using namespace std;
using namespace pert;
using namespace scul;

namespace tide {

bool CreateTideDataset(const ObjectList& objects, const std::string& output_uri){
  pert::ProtoTableWriter<Object> writer;
  if (not writer.Open(output_uri)){
    return false;
  }
  BOOST_FOREACH(const Object& object, objects.entries()){
    writer.Add("", object.SerializeAsString());
  }
  return true;
}

TideDataset::TideDataset(const std::string& tide_uri) {
  pert::ProtoTableReader<Object> tide_reader;
  CHECK(tide_reader.Open(tide_uri)) << "failed to open uri: " << tide_uri;
  string key;
  Object object;

  while (tide_reader.NextProto(&key, &object)){
    uint32 object_id = object.id();
    ObjectData& object_data = objectid_to_objectdata_[object_id];
    object_data.id = object_id;
    object_data.name = object.name();

    for (int i=0; i < object.photos_size(); ++i){
      const tide::Photo& photo = object.photos(i);
      imageid_to_objectid_[photo.id()] = object_id;
      if (photo.label() == tide::POSITIVE){
        object_data.primary.push_back(photo.id());
        primary_images_.insert(photo.id());
      }
      else if (photo.label() == tide::NEGATIVE){
        object_data.neg.push_back(photo.id());
      }
      else if (photo.label() == tide::NONE){
        object_data.aux.push_back(photo.id());
      }
    }
  }
}


std::vector<uint32> TideDataset::GetObjectIds() const {
  std::vector<uint32> object_ids;
  BOOST_FOREACH(const ObjectData& object_data, boost::adaptors::values(objectid_to_objectdata_)){
    object_ids.push_back(object_data.id);
  }
  return object_ids;
}


std::vector<uint64> TideDataset::GetImageIds() const {
  std::vector<uint64> image_ids;
  BOOST_FOREACH(const ObjectData& object_data,  boost::adaptors::values(objectid_to_objectdata_)){
    scul::Append(object_data.primary, &image_ids);
    scul::Append(object_data.aux, &image_ids);
    scul::Append(object_data.neg, &image_ids);
  }
  return image_ids;
}


const TideDataset::ObjectData& TideDataset::GetObject(uint32 object_id) const{
  CHECK_NE(object_id, UNKNOWN_OBJECT);
  return scul::FindOrDie(objectid_to_objectdata_, object_id);
}

uint32 TideDataset::GetObjectId(uint64 image_id) const{
  return scul::FindOrDie(imageid_to_objectid_, image_id);
}


// Samples the tide dataset to generate a random experiment suitable for
// computing benchmark statistics.
bool TideDataset::GenerateTrial(int num_training_images, float frac_aux_images, ExperimentTrial* trial){
  CHECK(trial);
  trial->Clear();
  BOOST_FOREACH(const ObjectData& object_data,  boost::adaptors::values(objectid_to_objectdata_)){
    // shuffle a copy of set of the primary images
    std::vector<uint64> primary = object_data.primary;
    std::vector<uint64> aux = object_data.aux;
    random_shuffle(primary.begin(), primary.end());
    int num_testing = primary.size() / 2.0;
    // sanity check... each label should have enough test cases
    if (num_testing < 100){
      LOG(FATAL) << "results are invalid... object " << object_data.id << " has too few primary labels.";
    }
    // select first half as testing_set
    CHECK_LT(num_testing, primary.size());
    std::vector<uint64>::iterator image_iter = primary.begin();
    for (int i=0; i < num_testing; ++i){
      ExperimentTrial_ImageLabelPair* p = trial->add_testing();
      p->set_object_id(object_data.id);
      p->set_image_id(*image_iter);
      ++image_iter;
    }

    // select training as prefix of second half
    CHECK_LT(num_testing + num_training_images, primary.size());
    image_iter = primary.begin() + num_testing;
    for (int i=0; i < num_training_images; ++i){
      ExperimentTrial_ImageLabelPair* p = trial->add_training();
      p->set_object_id(object_data.id);
      p->set_image_id(*image_iter);
      ++image_iter;
    }


    // Sample some aux images to be held out
    random_shuffle(aux.begin(), aux.end());
    int num_aux_images = aux.size() * frac_aux_images;
    //CHECK_GT(num_aux_images, 10); // sanity check... each label should have at least 100 aux images
    int num_blacklist_images = aux.size() - num_aux_images;
    CHECK_GE(num_blacklist_images, 0);
    std::vector<uint64>::iterator aux_iter = aux.begin();
    for (int i=0; i < num_blacklist_images; ++i){
      trial->add_heldout_aux_images(*aux_iter);
      aux_iter++;
    }
  }
  return true;
}

bool TideDataset::ImageIsPrimary(uint64 image_id){
  return scul::Contains(primary_images_, image_id);
}

} // close namespace tide
