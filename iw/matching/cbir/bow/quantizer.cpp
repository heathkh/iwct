#include "iw/matching/cbir/bow/quantizer.h"
#include "snap/flann/flann.h"
#include "snap/google/glog/logging.h"
#include "snap/boost/foreach.hpp"
#include "snap/pert/prototable.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <algorithm>

using namespace std;
using namespace iw;

namespace cbir {

Quantizer::Quantizer() {
}

Quantizer::~Quantizer() {
  delete [] dataset_.ptr();
}

bool Quantizer::Init(std::string visual_vocab_uri, bool exact) {
  LOG(INFO) << "attempting to load vv from file: " << visual_vocab_uri;

  pert::ProtoTableReader<VisualWord> reader;

  if (!reader.Open(visual_vocab_uri)){
    return false;
  }

  num_words_ = reader.Entries();

  // read first VW to get num_dims
  std::string key;
  VisualWord visual_word;
  CHECK(reader.NextProto(&key, &visual_word));
  num_dims_ = visual_word.entries_size();
  LOG(INFO) << "num_dims_: " << num_dims_;
  dataset_ = flann::Matrix<float>(new float[num_words_*num_dims_], num_words_, num_dims_);


  reader.SeekToStart();
  for (int i=0; i < num_words_; ++i){
    //LOG_EVERY_N(INFO, 10000) << "loading: " << i << " of " << num_words_;
    CHECK(reader.NextProto(&key, &visual_word));
    CHECK_EQ(visual_word.entries_size(), num_dims_);
    memcpy(dataset_[i], visual_word.entries().data(), sizeof(float)*num_dims_);
  }

  if (exact){
    index_params_["algorithm"] = FLANN_INDEX_LINEAR;
  }
  else{
    #ifdef AUTOTUNE
    index_params_ = AutotunedIndexParams(0.95, // target_precision
                               0.01, //  build_weight
                               0, // memory_weight
                               0.25); // sample_fraction

    flann::Logger::setLevel(FLANN_LOG_INFO);
    #else

    index_params_["algorithm"] = FLANN_INDEX_KMEANS;
    index_params_["branching"] = 16;
    index_params_["iterations"] = 5;
    index_params_["centers_init"] = FLANN_CENTERS_RANDOM;
    index_params_["cb_index"] = 0.8f;

    search_params_.checks = 320;
    search_params_.eps = 0;
    search_params_.sorted = 1;
    search_params_.max_neighbors = -1;

    #endif
  }

  LOG(INFO) << "staring to build index ";
  index_.reset(new Index(dataset_, index_params_));
  index_->buildIndex();
  LOG(INFO) << "done building index ";

  #ifdef AUTOTUNE
  flann::print_params(index_->getParameters());
  exit(1);
  #endif

  LOG(INFO) << "done loading vv from file: " << visual_vocab_uri;

  return true;
}


/*
bool Quantizer::Init(std::string visual_vocab_filename) {

  VisualVocabulary visual_vocab;
  LOG(INFO) << "attempting to load proto from file: " << visual_vocab_filename;
  if (!ReadProtoFromFile(visual_vocab_filename, &visual_vocab)){
    LOG(INFO) << "failed to load proto from file: " << visual_vocab_filename;
    return false;
  }

  num_words_ = visual_vocab.words_size();
  num_dims_ = visual_vocab.num_dimensions();
  dataset_ = flann::Matrix<float>(new float[num_words_*num_dims_], num_words_, num_dims_);
  for (int i=0; i < num_words_; ++i){
    CHECK_EQ(visual_vocab.words(i).entries_size(), num_dims_);
    memcpy(dataset_[i], visual_vocab.words(i).entries().data(), sizeof(float)*num_dims_);
  }

  #ifdef AUTOTUNE
  index_params_ = AutotunedIndexParams(0.95, // target_precision
                             0.01, //  build_weight
                             0, // memory_weight
                             0.25); // sample_fraction

  flann::Logger::setLevel(FLANN_LOG_INFO);
  #else

  index_params_["algorithm"] = FLANN_INDEX_KMEANS;
  index_params_["branching"] = 16;
  index_params_["iterations"] = 5;
  index_params_["centers_init"] = FLANN_CENTERS_RANDOM;
  index_params_["cb_index"] = 0.8f;

  search_params_.checks = 320;
  search_params_.eps = 0;
  search_params_.sorted = 1;
  search_params_.max_neighbors = -1;

  #endif

  index_.reset(new Index(dataset_, index_params_));
  index_->buildIndex();

  #ifdef AUTOTUNE
  flann::print_params(index_->getParameters());
  exit(1);
  #endif

  return true;
}
*/


bool Quantizer::Quantize(const ImageFeatures& features, BagOfWords* bag_of_words){
  CHECK(bag_of_words);

  int num_features = features.descriptors_size();

  flann::Matrix<int> indices(new int[num_features], num_features, 1);
  flann::Matrix<float> dists(new float[num_features], num_features, 1);
  flann::Matrix<float> queries(new float[num_features*num_dims_], num_features, num_dims_);

  for (int i=0; i < num_features; ++i){
    CHECK_EQ(features.descriptors(i).data().size() , num_dims_);
    float* row_dst = queries[i];
    const std::string& row_src = features.descriptors(i).data();
    for (int j=0; j < num_dims_; ++j){
      row_dst[j] = (float)row_src[j];
    }
  }

  index_->knnSearch(queries, indices, dists, 1, search_params_);

  bag_of_words->Clear();
  for (int i=0; i < num_features; ++i){
    bag_of_words->add_word_id(indices[i][0]);
  }

  delete [] indices.ptr();
  delete [] dists.ptr();
  delete [] queries.ptr();

  return true;
}


} // close namespace
