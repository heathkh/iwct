#include "gtest/gtest.h"
#include "iw/matching/cbir/full/full.h"
#include "snap/pert/pert.h"
#include <string>
#include "iw/util.h"
#include "snap/google/base/hashutils.h"
#include "snap/boost/timer/timer.hpp"
#include "snap/boost/filesystem/path.hpp"
#include "snap/boost/filesystem/operations.hpp"
using namespace std;

namespace fs = boost::filesystem;

namespace {

cbir::FeatureIndex* CreateFeatureIndex(string features_uri){
  pert::StringTableShardSetReader reader;
  CHECK(reader.Open(features_uri));
  iw::ImageFeatures features;
  bool include_keypoints = true;
  cbir::FeatureIndex* index = new cbir::FeatureIndex();
  string k,v;
  while (reader.Next(&k, &v)){
    uint64 image_id = KeyToUint64(k);
    CHECK(features.ParseFromString(v));
    index->Add(image_id, features);
  }
  cbir::FlannParams params;
  index->Build(params);
  return index;
}


TEST(ConcurrentFeatureIndex, test_speedup) {
  string test_data_path =  fs::absolute( fs::current_path()).string();
  string features_uri =  "local://" + test_data_path + "/features.pert";
  boost::scoped_ptr<cbir::FeatureIndex> inner_index(CreateFeatureIndex(features_uri));

  pert::StringTableShardSetReader reader;
  CHECK(reader.Open(features_uri));
  string key, value;
  int num_reps = 5;
  int k = 10;
  uint64 image_id;
  iw::ImageFeatures features;
  cbir::ImageFeaturesNeighbors neighbors;

  LOG(INFO) << "computing nonthreaded results";
  {
    boost::timer::auto_cpu_timer t;
    for (int i=0; i < num_reps; ++i){
      reader.SeekToStart();
      while (reader.Next(&key, &value)){
        image_id = KeyToUint64(key);
        CHECK(features.ParseFromString(value));
        inner_index->Search(image_id, k, false, features, &neighbors);
      }
    }
  }

  LOG(INFO) << "computing threaded results";
  cbir::ConcurrentFeatureIndex index(*inner_index.get());
  {
    boost::timer::auto_cpu_timer t;
    for (int i=0; i < num_reps; ++i){
      reader.SeekToStart();
      while (reader.Next(&key, &value)){
        image_id = KeyToUint64(key);
        boost::shared_ptr<iw::ImageFeatures> features(new iw::ImageFeatures);
        CHECK(features->ParseFromString(value));
        index.AddJob(image_id, k, false, features);
        while (index.JobsDone()){
          index.GetJob(&image_id, &neighbors);
        }
      }
    }

    while (index.NumJobsPending()){
      LOG(INFO) << "Waiting...";
      sleep(1);
    }

    while (index.JobsDone()){
      index.GetJob(&image_id, &neighbors);
    }
  }
}

/*
TEST(ConcurrentFeatureIndex, test_correct_output) {
  string test_data_path = "/tmp/TestConcurrentQuery/";
  string features_uri =  "local://tmp/TestSmoothFilter/features.pert";
  boost::scoped_ptr<cbir::FeatureIndex> inner_index(CreateFeatureIndex(features_uri));

  pert::StringTableShardSetReader reader;
  CHECK(reader.Open(features_uri));
  string key, value;

  int num_threads = 16;
  int k = 100;

  typedef std::map<uint64, cbir::ImageFeaturesNeighbors> ImageToResults;
  ImageToResults nonthreaded_index_results;

  LOG(INFO) << "computing nonthreaded results";
  {
    boost::timer::auto_cpu_timer t;
    while (reader.Next(&key, &value)){
      uint64 image_id = KeyToUint64(key);
      iw::ImageFeatures features;
      CHECK(features.ParseFromString(value));
      cbir::ImageFeaturesNeighbors& neighbors = nonthreaded_index_results[image_id];
      inner_index->Search(image_id, k, features, &neighbors);
    }
  }

  LOG(INFO) << "computing threaded results";
  ImageToResults threaded_index_results;
  cbir::ConcurrentFeatureIndex index(*inner_index.get(), num_threads);
  {
    boost::timer::auto_cpu_timer t;
    reader.SeekToStart();
    while (reader.Next(&key, &value)){
      uint64 image_id = KeyToUint64(key);
      boost::shared_ptr<iw::ImageFeatures> features(new iw::ImageFeatures);
      CHECK(features->ParseFromString(value));
      index.AddJob(image_id, k, features);

      while (index.JobsDone()){
        uint64 image_id;
        index.GetJob(&image_id, &threaded_index_results[image_id]);
      }
    }

    while (index.NumJobsPending()){
      LOG(INFO) << "Waiting...";
      sleep(1);
    }

    while (index.JobsDone()){
      uint64 image_id;
      index.GetJob(&image_id, &threaded_index_results[image_id]);
    }
  }


  BOOST_FOREACH(ImageToResults::value_type v, threaded_index_results){
    ASSERT_EQ(threaded_index_results[v.first].DebugString(), v.second.DebugString());
  }

}
*/


} // close empty namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
