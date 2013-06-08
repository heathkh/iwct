#include "tide/tide.h"
#include "snap/google/glog/logging.h"
#include "snap/google/gtest/gtest.h"

using namespace tide;
using namespace std;

namespace {

Object* AddObject(ObjectList* object_list, uint32 id, std::string name){
  Object* new_object = object_list->add_entries();
  new_object->set_id(id);
  new_object->set_name(name);
  new_object->set_description("description of " + name);
  new_object->set_purity(0.5);
  return new_object;
}

void AddPhoto(Object* object, uint64 id, LabelType label){
  Photo* p = object->add_photos();

  p->set_id(id);
  p->set_label(label);
  //BoundingBox* bb = p->add_regions();
}

TEST(TideDataset, simple) {
  ObjectList object_list;
  uint64 cur_image_index = 0;

  Object* obj;
  obj = AddObject(&object_list, 1, "sunflower");
  for (int i=0; i < 200; ++i){
    AddPhoto(obj, cur_image_index++, POSITIVE);
    AddPhoto(obj, cur_image_index++, NEGATIVE);
    AddPhoto(obj, cur_image_index++, NONE);
  }
  ASSERT_EQ(cur_image_index, 600);

  obj = AddObject(&object_list, 2, "rose");
  for (int i=0; i < 200; ++i){
    AddPhoto(obj, cur_image_index++, POSITIVE);
    AddPhoto(obj, cur_image_index++, NEGATIVE);
    AddPhoto(obj, cur_image_index++, NONE);
  }

  // TODO(kheath): get temp file and clean up properly
  string test_dataset_uri = "local://tmp/test_tide.pert";
  CreateTideDataset(object_list, test_dataset_uri);

  TideDataset tide(test_dataset_uri);

  ASSERT_EQ(tide.GetObjectId(10), 1);
  ASSERT_EQ(tide.GetObjectId(610), 2);

  int num_objects = 2;
  ASSERT_EQ(tide.GetObjectIds().size(), num_objects);
  ASSERT_EQ(tide.GetObject(1).aux.size(), 200);
  ASSERT_EQ(tide.GetObject(1).primary.size(), 200);

  ExperimentTrial trial;
  int num_training_images_per_object = 50;
  ASSERT_TRUE(tide.GenerateTrial(num_training_images_per_object, 1.0, &trial));
  ASSERT_EQ(100*num_objects, trial.testing_size());
  ASSERT_EQ(num_training_images_per_object*num_objects, trial.training_size());
  LOG(INFO) << trial.DebugString();
}

} // close anononymous namespace


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


