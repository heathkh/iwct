#include "iw/matching/cbir/full/feature_index.h"
#include "iw/util.h"
#include "snap/boost/filesystem.hpp"
#include "snap/boost/foreach.hpp"
#include "snap/boost/scoped_array.hpp"
#include "snap/google/base/stringprintf.h"
#include "snap/google/glog/logging.h"
#include "snap/pert/pert.h"
#include "snap/progress/progress.hpp"
#include "snap/progress/progress.hpp"
#include "snap/scul/scul.hpp"
#include <algorithm>
#include <boost/timer/timer.hpp>
#include <cfloat>

namespace fs = boost::filesystem;

using namespace std;
using namespace pert;
using namespace iw;
using namespace boost;
using namespace scul;
using namespace progress;
//#define AUTOTUNE

namespace cbir {

// Truncate list of features to given length.
void ReduceImageFeatures(int max_features, iw::ImageFeatures* features){
  int num_to_remove = 0;
  if (features->descriptors_size() > max_features){
    num_to_remove = features->descriptors_size() - max_features;
  }

  for (int i=0; i < num_to_remove; ++i){
    features->mutable_descriptors()->RemoveLast();
    features->mutable_keypoints()->RemoveLast();
  }
}


///////////////////////////////////////////////////
// ImageFeatureCountTable
////////////////////////////////////////////////////////////////////////////////

ImageFeatureCountTable::ImageFeatureCountTable() :
ready_(false) {
}

bool ImageFeatureCountTable::Create(const std::string& features_uri){
  CHECK(!ready_);
  pert::StringTableReader reader;
  CHECK(reader.Open(features_uri));
  string key, value;
  iw::ImageFeatures features;
  progress::ProgressBar<uint64> progress_bar(reader.Entries());
  while (reader.Next(&key, &value)){
    uint64 image_id = KeyToUint64(key);
    features.ParseFromString(value);
    imageid_to_numfeatures_[image_id] = features.descriptors_size();
    progress_bar.Increment();
  }

  ready_ = true;
  return true;
}

bool ImageFeatureCountTable::Create(const std::vector<cbir::FeatureIndexImageMetadata>& feature_index_metadata){
  CHECK(!ready_);
  uint32 prev_num_features_cumulative = 0;
  for (int i=0; i < feature_index_metadata.size(); ++i){
    const FeatureIndexImageMetadata& metadata = feature_index_metadata[i];
    CHECK_GE(metadata.num_features_cumulative(), prev_num_features_cumulative);
    uint32 num_features = metadata.num_features_cumulative() - prev_num_features_cumulative;
    prev_num_features_cumulative = metadata.num_features_cumulative();
    imageid_to_numfeatures_[metadata.image_id()] = num_features;
  }
  ready_ = true;
  return true;
}



bool ImageFeatureCountTable::Save(const std::string& feature_counts_uri){
  CHECK(ready_);
  pert::ProtoTableWriter<cbir::Count> writer;
  CHECK(writer.Open(feature_counts_uri));
  ProgressBar<uint64> progress_bar(imageid_to_numfeatures_.size());
  cbir::Count count;
  for(ImageToFeatureCount::iterator iter = imageid_to_numfeatures_.begin(); iter != imageid_to_numfeatures_.end(); ++iter ){
    //writer.Add(Uint64ToKey(iter->first), Uint64ToKey(iter->second)); // TODO(kheath): Use smaller int type for count
    count.set_value(iter->second);
    writer.Add(Uint64ToKey(iter->first), count); // TODO(kheath): Use smaller int type for count
    progress_bar.Increment();
  }

  return true;
}


bool ImageFeatureCountTable::Load(const std::string& feature_counts_uri){
  CHECK(!ready_);
  LOG(INFO) << "loading: " << feature_counts_uri;
  pert::ProtoTableReader<cbir::Count> reader;
  CHECK(reader.Open(feature_counts_uri));
  string key;
  cbir::Count count;
  while (reader.NextProto(&key, &count)){
    imageid_to_numfeatures_[KeyToUint64(key)] = count.value();
  }
  LOG(INFO) << "done loading: " << feature_counts_uri;
  ready_ = true;
  return true;
}


int ImageFeatureCountTable::GetCountOrDie(uint64 image_id) const{
  return FindOrDie(imageid_to_numfeatures_, image_id);
}

/*
    // Tuned for precision = 0.95
    index_params_["algorithm"] = flann::FLANN_INDEX_KMEANS;
    index_params_["branching"] = 32;
    index_params_["iterations"] = 5;
    index_params_["centers_init"] = flann::FLANN_CENTERS_RANDOM;
    index_params_["cb_index"] = 0.4f;
    search_params_.checks = 352;
    search_params_.cores = -1;
    */

/*
    // Tuned for precision = 0.90
    index_params_["algorithm"] = flann::FLANN_INDEX_KMEANS;
    index_params_["branching"] = 16;
    index_params_["iterations"] = 10;
    index_params_["centers_init"] = flann::FLANN_CENTERS_RANDOM;
    index_params_["cb_index"] = 0.2f;
    search_params_.checks = 98;
    search_params_.cores = -1;
*/

/*
    // Tuned for precision = 0.5
    index_params_["algorithm"] = flann::FLANN_INDEX_KDTREE;
    index_params_["trees"] = 4;
    search_params_.checks = 30;
    search_params_.cores = -1;
*/

/*
    // Tuned for precision = 0.1
    index_params_["algorithm"] = flann::FLANN_INDEX_KDTREE;
    index_params_["trees"] = 1;
    search_params_.checks = 2;
    search_params_.cores = 1;
*/

// force_exact useful for testing
FeatureIndex::FeatureIndex() :
  num_preallocated_features_(0),
  num_dims_(128),
  num_features_(0),
  feature_data_ptr_(NULL)
{

}


FeatureIndex::~FeatureIndex() {
}


void FeatureIndex::StartPreallocate(uint64 num_preallocated_features){
  CHECK_EQ(num_preallocated_features_, 0);
  num_preallocated_features_ = num_preallocated_features;
  feature_data_.resize(num_preallocated_features_ * num_dims_);
  feature_data_ptr_ = feature_data_.data();
  num_features_ = 0;
}


bool FeatureIndex::AddPreallocated(uint64 image_id, const iw::ImageFeatures& features){
  CHECK_GT(num_preallocated_features_, 0) << "Must call StartPreallocate first";
  CHECK(!index_) << "Can't add to index after it has been built or loaded";
  CHECK_EQ( features.descriptors_size(),  features.keypoints_size());
  CHECK_NOTNULL(feature_data_ptr_);

  BOOST_FOREACH(const iw::FeatureDescriptor& descriptor, features.descriptors()){
    CHECK_EQ(descriptor.data().size(),  num_dims_); // verify each added feature is expected length
    memcpy(feature_data_ptr_, &descriptor.data()[0], num_dims_);
    feature_data_ptr_ += num_dims_;
    ++num_features_;
    CHECK_LE(num_features_, num_preallocated_features_);
  }

  cbir::Keypoint new_keypoint_metadata;
  BOOST_FOREACH(const FeatureKeypoint& keypoint, features.keypoints()){
    new_keypoint_metadata.set_x(keypoint.pos().x());
    new_keypoint_metadata.set_y(keypoint.pos().y());
    new_keypoint_metadata.set_radius_100(keypoint.radius()*100);
    new_keypoint_metadata.CheckInitialized();
    keypoint_metadata_.push_back(new_keypoint_metadata);
  }

  cbir::FeatureIndexImageMetadata metadata;
  metadata.set_image_id(image_id);
  metadata.set_num_features_cumulative(num_features_);
  index_metadata_.push_back(metadata);
  return true;
}

void FeatureIndex::EndPreallocate(){
  CHECK_GT(num_preallocated_features_, 0);
  feature_data_.resize(num_features_ * num_dims_);
}

bool FeatureIndex::Add(uint64 image_id, const iw::ImageFeatures& features){
  CHECK_EQ(num_preallocated_features_, 0) << "can't use both preallocated and non-preallocated apis";
  CHECK(!index_) << "Can't add to index after it has been built or loaded";
  CHECK_EQ( features.descriptors_size(),  features.keypoints_size());

  BOOST_FOREACH(const FeatureDescriptor& descriptor, features.descriptors()){
    feature_data_.insert(feature_data_.end(), descriptor.data().begin(), descriptor.data().end());
    ++num_features_;
  }
  CHECK_EQ(feature_data_.size(), num_features_ * num_dims_); // verify each added feature is expected length

  cbir::Keypoint new_keypoint_metadata;
  BOOST_FOREACH(const FeatureKeypoint& keypoint, features.keypoints()){
    new_keypoint_metadata.set_x(keypoint.pos().x());
    new_keypoint_metadata.set_y(keypoint.pos().y());
    new_keypoint_metadata.set_radius_100(keypoint.radius()*100);
    new_keypoint_metadata.CheckInitialized();
    keypoint_metadata_.push_back(new_keypoint_metadata);
  }

  cbir::FeatureIndexImageMetadata metadata;
  metadata.set_image_id(image_id);
  metadata.set_num_features_cumulative(num_features_);
  index_metadata_.push_back(metadata);
  return true;
}

void FeatureIndex::Build(const cbir::FlannParams& params){
  params_ = params;
  if (params_.algorithm() == params_.FLANN_INDEX_KDTREE){
    index_params_["algorithm"] = flann::FLANN_INDEX_KDTREE;
    index_params_["trees"] = int(params_.kdtree_params().trees()); // the flann "any" type requires a very specific type of integer or it fails in a cryptic way... make sure it is happy.
    search_params_.checks = int(params_.kdtree_params().checks());
  }

  if (num_preallocated_features_){
    EndPreallocate();
  }

  feature_matrix_ = flann::Matrix<ElementType>(feature_data_.data(), num_features_, num_dims_);
  CHECK_EQ(feature_data_.size(), num_features_*num_dims_); // sanity check

  #ifdef AUTOTUNE
  index_params_ = flann::AutotunedIndexParams(0.1, // target_precision
                             0.01, //  build_weight
                             0, // memory_weight
                             0.01); // sample_fraction

  flann::Logger::setLevel(flann::FLANN_LOG_INFO);
  #endif

  LOG(INFO) << "Starting to build index ";
  index_.reset(new Index(feature_matrix_, index_params_));
  index_->buildIndex();
  LOG(INFO) << "Done building index ";

  #ifdef AUTOTUNE
  flann::print_params(index_->getParameters());
  exit(1);
  #endif
}


std::string DataUri(const string& uri){
  return uri + "/data.pert";
}

std::string FlannUri(const string& uri){
  return uri + "/flannindex.bin";
}

std::string MetadataUri(const string& uri){
  return uri + "/metadata.pert";
}

std::string KeypointMetadataUri(const string& uri){
  return uri + "/keypoint_metadata.pert";
}

bool FeatureIndex::Save(const std::string& uri) {
  CHECK(IsValidUri(uri)) << "Expected a uri: " << uri;
  // Write the data
  ProtoTableShardWriter<iw::FeatureDescriptor> data_writer;
  CHECK(data_writer.Open(DataUri(uri)));
  iw::FeatureDescriptor descriptor;
  vector<ElementType>::const_iterator iter = feature_data_.begin();
  for (size_t i = 0; i < num_features_; ++i){
    descriptor.mutable_data()->assign(iter, iter+num_dims_);
    data_writer.Add("", descriptor);
    iter += num_dims_;
  }
  data_writer.AddMetadata("flann_index_params", params_.SerializeAsString());
  data_writer.Close();

  // Write the index to a tmp file
  string tmp_filename = fs::unique_path().string();
  LOG(INFO) << "attempting to save index to tmp file: " << tmp_filename;
  index_->save(tmp_filename);
  // copy to maprfs
  LOG(INFO) << "attempting to copy tmp index to uri: " << FlannUri(uri);
  bool copy_ok = CopyLocalToUri(tmp_filename, FlannUri(uri));
  fs::remove(tmp_filename);
  CHECK(copy_ok);

  // Write the image metadata
  ProtoTableShardWriter<cbir::FeatureIndexImageMetadata> metadata_writer;
  CHECK(metadata_writer.Open(MetadataUri(uri)));
  BOOST_FOREACH(const cbir::FeatureIndexImageMetadata& m, index_metadata_){
    metadata_writer.Add(" ", m);
  }
  metadata_writer.Close();

  // Write the keypoint metadata
  ProtoTableShardWriter<cbir::Keypoint> keypoint_metadata_writer;
  CHECK(keypoint_metadata_writer.Open(KeypointMetadataUri(uri)));
  BOOST_FOREACH(const cbir::Keypoint& k, keypoint_metadata_){
    keypoint_metadata_writer.Add("", k);
  }
  keypoint_metadata_writer.Close();

  return true;
}

bool FeatureIndex::Load(const std::string& uri) {
  CHECK(IsValidUri(uri)) << "Expected a uri: " << uri;
  CHECK(!index_) << "Index already present... can't load a new one";
  CHECK(Exists(DataUri(uri))) << "Can't find path: " <<  DataUri(uri);
  CHECK(Exists(FlannUri(uri))) << "Can't find path: " <<  FlannUri(uri);
  CHECK(Exists(MetadataUri(uri))) << "Can't find path: " <<  MetadataUri(uri);
  CHECK(Exists(KeypointMetadataUri(uri))) << "Can't find path: " <<  KeypointMetadataUri(uri);

  // Load the data (and the params proto from metadata section of pert)
  ProtoTableShardReader<iw::FeatureDescriptor> data_reader;
  iw::FeatureDescriptor descriptor;
  string key;
  num_features_ = 0;
  {
    LOG(INFO) << "Loading feature data...";
    boost::timer::cpu_timer timer;
    CHECK(data_reader.Open(DataUri(uri)));
    num_features_ = data_reader.Entries();
    feature_data_.resize(num_dims_*num_features_);
    unsigned char* data_ptr = feature_data_.data();
    CHECK_EQ(sizeof(ElementType), 1);
    while (data_reader.NextProto(&key, &descriptor)){
      CHECK_EQ(descriptor.data().size(), num_dims_);
      //feature_data_.insert(feature_data_.end(), descriptor.data().begin(), descriptor.data().end());
      memcpy(data_ptr, &descriptor.data()[0], num_dims_);
      data_ptr += num_dims_;
    }
    string tmp;
    CHECK(data_reader.GetMetadata("flann_index_params", &tmp));
    CHECK(params_.ParseFromString(tmp));
    data_reader.Close();
    LOG(INFO) << "DONE: Loading feature data: " << timer.format();
  }

  feature_matrix_ = flann::Matrix<ElementType>(feature_data_.data(), num_features_, num_dims_);
  CHECK_EQ(feature_data_.size(), num_features_*num_dims_); // sanity check

  string scheme, local_index_path, error;
  CHECK(pert::ParseUri(FlannUri(uri), &scheme, &local_index_path, &error));

  string tmp_filename;
  if (scheme != "local"){
    tmp_filename = "/opt/mapr/logs/cache/tmp/" + fs::unique_path().string(); // magic path to a dir mounted with a ephemeral disc with ~200 GB storage
    LOG(INFO) << "Downloading index data";
    LOG(INFO) << "from: " << FlannUri(uri);
    LOG(INFO) << "to: " << tmp_filename;
    boost::timer::cpu_timer timer;
    // Load the index (must copy from remote FS to local FS for flann load to work)
    //CHECK(DownloadUri(FlannUri(uri), tmp_filename));
    CHECK(CopyUri(FlannUri(uri), "local://" + tmp_filename));
    LOG(INFO) << "Done downloading index data: " << timer.format();
    local_index_path = tmp_filename;
  }

  {
    LOG(INFO) << "Loading index data from : " << local_index_path;
    boost::timer::cpu_timer timer;
    index_params_["algorithm"] = flann::FLANN_INDEX_SAVED;
    index_params_["filename"] = local_index_path;
    index_.reset(new Index(feature_matrix_, index_params_));
    LOG(INFO) << "Done loading index data: " << timer.format();

    // Checks that the flann serialized params match the copy we have in proto
    //LOG(INFO) << index_params_["algorithm"].type().name();
    //CHECK_EQ(index_params_["algorithm"].cast<int>(), params_.algorithm());

    // The "checks" param is not part of the flann build in serialization...
    // restore it for the proto previously loaded from the data.pert
    if (params_.has_kdtree_params()){
      search_params_.checks = params_.kdtree_params().checks();
    }
  }

  if (!tmp_filename.empty()){
    fs::remove(tmp_filename);
  }

  // Load the image metadata
  {
    boost::timer::auto_cpu_timer timer;
    LOG(INFO) << "Loading metadata...";
    ProtoTableShardReader<cbir::FeatureIndexImageMetadata> metadata_reader;
    cbir::FeatureIndexImageMetadata m;
    CHECK(metadata_reader.Open(MetadataUri(uri)));
    while (metadata_reader.NextProto(&key, &m)){
      index_metadata_.push_back(m);
    }
    CHECK_EQ(num_features_, index_metadata_.back().num_features_cumulative());
    LOG(INFO) << "DONE: Loading metadata: " << timer.format();
  }

  // Load the keypoint metadata
  {
    LOG(INFO) << "Loading keypoint metadata...";
    boost::timer::auto_cpu_timer timer;
    ProtoTableShardReader<cbir::Keypoint> keypoint_metadata_reader;
    cbir::Keypoint keypoint;
    CHECK(keypoint_metadata_reader.Open(KeypointMetadataUri(uri)));
    CHECK_EQ(keypoint_metadata_reader.Entries(), num_features_);
    keypoint_metadata_.resize(num_features_);
    for (int i=0; i < num_features_; ++i){
      CHECK(keypoint_metadata_reader.NextProto(&key, &keypoint));
      keypoint_metadata_[i] = keypoint;
    }
    LOG(INFO) << "DONE: Loading keypoint metadata: " << timer.format();
  }
  LOG(INFO) << "done loading index";
  return true;
}


struct IndexMetadataComparator {
  bool operator()(int64 feature_index, const cbir::FeatureIndexImageMetadata& m){
    return feature_index < m.num_features_cumulative();
  }
};

uint64 FeatureIndex::GetImageContainingFeature(int64 feature_index) const {
  MetadataVector::const_iterator iter = std::upper_bound(index_metadata_.begin(), index_metadata_.end(), feature_index, IndexMetadataComparator());
  CHECK(iter != index_metadata_.end());
  return iter->image_id();
}


void FeatureIndex::ResultIndexToImageFeatureIndices(int64 feature_index, uint64* result_image_id, uint32* result_image_feature_id) const {
  MetadataVector::const_iterator iter = std::upper_bound(index_metadata_.begin(), index_metadata_.end(), feature_index, IndexMetadataComparator());
  CHECK(iter != index_metadata_.end());
  *result_image_id = iter->image_id();

  // calculate the offset of this feature in the list of features of this image
  // if feature is in the first image, the cumulative index of the first feature in this image is 0
  int64 cur_image_first_feature_index = 0;
  // otherwise, we need to subtract the cumulative feature index of the prev image
  if (iter != index_metadata_.begin()){
    MetadataVector::const_iterator prev_iter = iter - 1; // get prev image
    CHECK_GE(feature_index, prev_iter->num_features_cumulative());
    cur_image_first_feature_index =  prev_iter->num_features_cumulative();
  }
  CHECK_GE((uint64)feature_index, cur_image_first_feature_index);
  *result_image_feature_id = feature_index - cur_image_first_feature_index;
}


bool FeatureIndex::Search(uint64 ignore_image_id, int k, bool include_keypoints, const iw::ImageFeatures& features, cbir::ImageFeaturesNeighbors* neighbors) const {
  std::set<uint64> ignore_image_ids;
  ignore_image_ids.insert(ignore_image_id);
  return Search(ignore_image_ids, k, include_keypoints, features, neighbors);
}

// Find the k nearest neighbors for each feature
// Only returns features that are not from images in the ignore set.  This is
// useful if the query features come from an image already in the database, and
// we want to get the query results as if image A was not in the database since
// we already know A is similar to itself.
bool FeatureIndex::Search(const std::set<uint64>& ignore_image_set, int num_neighbors, bool include_keypoints, const iw::ImageFeatures& features, cbir::ImageFeaturesNeighbors* neighbors) const {
  CHECK(index_) << "Must call Build() or Load() before calling Search()";
  CHECK_GE(num_neighbors, 1);
  int num_features = features.descriptors_size();
  if (!num_features){
    return false;
  }

  // Convert query to FLANN data type
  boost::scoped_array<ElementType> data(new ElementType[num_features*num_dims_]); // Ensure buffer gets deleted
  flann::Matrix<ElementType> queries(data.get(), num_features, num_dims_);
  for (int i=0; i < num_features; ++i){
    const string& d = features.descriptors(i).data();
    CHECK_EQ(d.size(), num_dims_);
    memcpy(queries[i], d.data(), sizeof(ElementType)*num_dims_);
  }
  float search_expansion_rate = 1.5; // how fast to grow k
  int knn_search_depth = num_neighbors;
  bool success = false;
  for (int i=0; i < 20; ++i){
    if (SearchWithDepth(ignore_image_set, knn_search_depth, num_neighbors, queries, neighbors)){
      success = true;
      break;
    }
    knn_search_depth *= search_expansion_rate;
  }
  if (success && include_keypoints){
    CHECK_EQ(num_features, neighbors->features_size());
    for (int i=0; i < num_features; ++i){
      const iw::FeatureKeypoint& k = features.keypoints(i);
      cbir::Keypoint* nk = neighbors->mutable_features(i)->mutable_keypoint();
      nk->set_x(k.pos().x());
      nk->set_y(k.pos().y());
      nk->set_radius_100(k.radius()*100);
    }
  }

  return success;
}


bool FeatureIndex::SearchWithDepth(const std::set<uint64>& blacklist_image_ids, int knn_search_depth, int num_neighbors, const flann::Matrix<ElementType>& queries, cbir::ImageFeaturesNeighbors* image_features_neighbors) const {
  CHECK(index_) << "Must call Build() or Load() before calling Search()";
  CHECK(image_features_neighbors);
  CHECK_GE(knn_search_depth, 1);
  CHECK_GE(num_neighbors, 1);
  CHECK_GE(knn_search_depth, num_neighbors);
  int num_queries = queries.rows;
  CHECK_GE(num_queries, 1);

  // Use scoped_array to ensure result buffers always get cleaned-up.
  boost::scoped_array<int> indices_data(new int[num_queries*knn_search_depth]);
  boost::scoped_array<float> dists_data(new float[num_queries*knn_search_depth]);
  flann::Matrix<int> indices(indices_data.get(), num_queries, knn_search_depth);
  flann::Matrix<float> dists(dists_data.get(), num_queries, knn_search_depth);
  int total_returned = index_->knnSearch(queries, indices, dists, knn_search_depth, search_params_);

  // TODO(kheath): This is a lazy bail condition that could be eliminated if
  // you add a check below for -1 flags returned to indicate not all requested
  // neighbors were found.
  if (total_returned != num_queries * knn_search_depth){
    return false;
  }

  // Find valid num_neighbors results features for each query feature
  // (or abort if not enough valid features for current search depth)
  // Result is valid when it is not from the black listed images.
  std::vector<std::vector<int> > queryid_to_valid_resultids(num_queries);
  for (int query_id=0; query_id < num_queries; ++query_id){
    int* query_indices = indices[query_id];
    std::vector<int>& valid_resultids = queryid_to_valid_resultids[query_id];
    for (int result_id=0; result_id < knn_search_depth; ++result_id) {
      uint64 image_id = GetImageContainingFeature(query_indices[result_id]);
      // Ignore if resulting feature is from a blacklisted image.
      if (scul::Contains(blacklist_image_ids, image_id)){
        continue;
      }
      valid_resultids.push_back(result_id);
      // Move to next query as soon as we have the requested number of neighbors
      if (valid_resultids.size() == num_neighbors){
        break;
      }
    }
    if (valid_resultids.size() != num_neighbors){
      return false;
    }
  }

  // If we get here, we have found enough non-blacklisted neighbors for each query feature
  // Copy results to output.
  image_features_neighbors->Clear();
  for (int query_id=0; query_id < num_queries; ++query_id){
    FeatureNeighbors* feature_neighbors = image_features_neighbors->mutable_features()->Add();
    int* query_indices = indices[query_id];
    float* query_dists = dists[query_id];
    float prev_result_dist = 0;
    BOOST_FOREACH(int result_id, queryid_to_valid_resultids[query_id]){
      int result_index = query_indices[result_id];
      float result_distance = query_dists[result_id];
      CHECK_GE(result_distance, prev_result_dist); // check invariant: sorted increasing by distance
      prev_result_dist = result_distance;
      FeatureNeighbor* n = feature_neighbors->mutable_entries()->Add();
      n->set_image_id(GetImageContainingFeature(result_index));
      n->set_distance(result_distance);
      n->mutable_keypoint()->CopyFrom(keypoint_metadata_[result_index]);
    }
    CHECK_EQ(feature_neighbors->entries_size(), num_neighbors);
  }
  return true;
}

////////////////////////////////////////////////////////////////////////////////
// NearestNeighborsAccumulator
////////////////////////////////////////////////////////////////////////////////

bool FeatureNeighborSortComparator(const cbir::FeatureNeighbor& n1, const cbir::FeatureNeighbor& n2){
  return n1.distance() < n2.distance();
}

NearestNeighborsAccumulator::NearestNeighborsAccumulator() {
}

void NearestNeighborsAccumulator::Add(const cbir::ImageFeaturesNeighbors& new_neighbors){
  // If accumulator is empty, just copy first batch of features and return
  if (!accumulated_features_knn_.features_size()){
    accumulated_features_knn_.CopyFrom(new_neighbors);
    return;
  }
  // Since at this point, we expect to be accumulating more knns for same image... the num of features must be the same for each call.
  CHECK_EQ(new_neighbors.features_size(), accumulated_features_knn_.features_size());
  for (int i=0; i < new_neighbors.features_size(); ++i){
    cbir::FeatureNeighbors* feature_neighbors = accumulated_features_knn_.mutable_features(i);
    const cbir::FeatureNeighbors& new_feature_neighbors = new_neighbors.features(i);
    // exactly one of the new neighbors must contain the keypoints to be transfered to the accumulated one
    if (new_feature_neighbors.has_keypoint()){
      CHECK(!feature_neighbors->has_keypoint());
      feature_neighbors->mutable_keypoint()->CopyFrom(new_feature_neighbors.keypoint());
    }
    // concatenate the knns for each feature, and sort increasing by distance
    feature_neighbors->mutable_entries()->MergeFrom(new_feature_neighbors.entries());
  }
}

bool NearestNeighborsAccumulator::GetResult(cbir::ImageFeaturesNeighbors* feature_neighbors){
  CHECK(feature_neighbors);

  // after we have merged all inputs, verify invariant that keypoints got transfered to accumulated results
  // and that neighbors are sorted in order of increasing distance
  for (int i=0; i < accumulated_features_knn_.features_size(); ++i){
    cbir::FeatureNeighbors* feature_neighbors = accumulated_features_knn_.mutable_features(i);
    CHECK(feature_neighbors->has_keypoint());
    Sort(feature_neighbors->mutable_entries(), FeatureNeighborSortComparator);
  }
  feature_neighbors->CopyFrom(accumulated_features_knn_);
  return true;
}

/////

ConcurrentFeatureIndex::ConcurrentFeatureIndex(const FeatureIndex& index) :
  task_queue_(256),
  index_(index) {
  LOG(INFO) << "ConcurrentFeatureIndex::ConcurrentFeatureIndex()";
}

void ConcurrentFeatureIndex::AddJob(uint64 ignore_image_id, int k, bool include_keypoints, boost::shared_ptr<iw::ImageFeatures> features){
  task_queue_.QueueTask(IndexSearchTask(index_, ignore_image_id, k, include_keypoints, features));
}
bool ConcurrentFeatureIndex::JobsDone(){
  return task_queue_.TasksCompleted();
}

void ConcurrentFeatureIndex::GetJob(uint64* image_id, cbir::ImageFeaturesNeighbors* features_neighbors){
  IndexSearchTaskResult result = task_queue_.GetCompletedTaskResult();
  *image_id = result.image_id;
  features_neighbors->CopyFrom(*result.neighbors.get());
}

int  ConcurrentFeatureIndex::NumJobsPending(){
  return task_queue_.NumPendingTasks();
}


} // close namespace cbir
