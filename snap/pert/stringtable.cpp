#include "snap/pert/stringtable.h"
#include "snap/pert/core/ufs.h"
#include "snap/google/base/integral_types.h"
#include "snap/boost/foreach.hpp"
#include "snap/google/glog/logging.h"
#include "snap/google/base/stringprintf.h"
#include "snap/boost/filesystem.hpp"
#include <algorithm>
#include <string>

#include "snap/boost/regex.hpp"

namespace fs =  boost::filesystem;

namespace pert {

std::string BytesToHexString(const std::string& in){
  CHECK_EQ(in.size() % 4, 0) << "Current implementation requires input string length be a multiple of 4 - size was: " << in.size();
  std::string out;
  for (int i=0; i < in.size(); i += 4){
   const uint32* d = (const uint32*)(in.data()+i);
   uint32 tmp = *d;
   out += StringPrintf("%02x", tmp);
  }
  return out;
}


// Helper functions
int GetNumShards(const std::string& uri){
  std::vector<std::string> uris = GetShardUris(uri);
  return uris.size();
}

// returns shard uris sorted in increasing order
std::vector<std::string> GetShardUris(const std::string& uri){
  boost::scoped_ptr<FileSystem> fs(GetFilesystemForUri(uri));
  std::vector<std::string> matching_uris;
  std::string scheme, uri_path;
  if (!fs || !ParseUri(uri, &scheme, &uri_path)){
    return matching_uris;
  }

  fs::path p(uri_path);

  // METHOD 1: Check if provided a directory containing part files
  std::string dirpath = p.parent_path().string();
  std::string filepath = p.string();
  std::string filename = p.filename().string();

  std::string regex = ".*part-.[[:digit:]]+";
  boost::regex re(regex);
  std::string filepath_uri = Uri(scheme, filepath);

  //LOG(INFO) << "filepath_uri: " << filepath_uri;
  if (fs->IsDirectory(filepath_uri)){
    //LOG(INFO) << "is dir";
    std::vector<std::string> entries;
    CHECK(fs->ListDirectory(filepath_uri, &entries));
    BOOST_FOREACH(std::string& entry_uri, entries) {
      //LOG(INFO) << "entry_uri: " << entry_uri;
      boost::smatch what;
      if(boost::regex_match(entry_uri, what, re)){
        matching_uris.push_back(entry_uri);
        //VLOG(2) << "Matching input: " << candidate_path;
        //LOG(INFO) << "Matching input: " << entry_uri;
      }
    }
  }
  // METHOD 2: Check if provided a full path to a single part file
  else if (fs->IsFile(filepath_uri)) {
    //LOG(INFO) << "is file";
    matching_uris.push_back(filepath_uri);
  }
  else{
    LOG(FATAL) << "uri doesn't exist: " << filepath_uri;
  }

  sort(matching_uris.begin(), matching_uris.end());

  return matching_uris;
}

bool ShardSetIsValid(const std::vector<std::string>& shard_uris){
  // Make sure shard URIS are valid and complete
  int part_id = 0;
  BOOST_FOREACH(std::string shard_uri, shard_uris){
    std::string shard_name = fs::path(shard_uri).filename().string();
    std::string expected_shard_name = StringPrintf("part-%05d", part_id);
    if (shard_name != expected_shard_name){
      LOG(INFO) << "Shard set is not valid.  Expected " << expected_shard_name << " but next shard was " << shard_name;
      return false;
    }
    part_id++;
  }
  return true;
}

// Computes the fingerprint of a shard set as follows:
// if uri is a single shard -> return fingerprint of shard
// if uri is a table with with multiple shards -> concatinate the fingerprints and fingerprint that
bool GetShardSetFingerprint(const std::string& uri, std::string* hex_fingerprint){
  vector<string> shard_uris = pert::GetShardUris(uri);
  if (shard_uris.empty()){
    return false;
  }

  // sort so fingerprint is independent of the input uri ordering
  std::sort(shard_uris.begin(), shard_uris.end());

  BOOST_FOREACH(std::string uri, shard_uris){
    if (!Exists(uri)){
      LOG(INFO) << "uri does not exist: " << uri;
      return false;
    }
  }

  pert::FileInfo info;
  if (shard_uris.size() == 1){
    string uri = shard_uris[0];
    if (!StringTableShardReader::GetShardInfo(uri, &info)){
      LOG(INFO) << "Error getting info for shard: " << uri;
      return false;
    }
    hex_fingerprint->assign(info.fingerprint());
  }
  else{
    Md5Digest md5_digest;
    BOOST_FOREACH(std::string uri, shard_uris){
      if (!StringTableShardReader::GetShardInfo(uri, &info)){
        LOG(INFO) << "Error getting info for shard: " << uri;
        return false;
      }
      const std::string& shard_fingerprint = info.fingerprint();
      CHECK_EQ(shard_fingerprint.size(), 32);
      md5_digest.Update(shard_fingerprint.data(), shard_fingerprint.size());
    }
    hex_fingerprint->assign(md5_digest.GetAsHexString());
  }
  return true;
}

// Normally splits have many entries for efficient indexing.  Use this config
// when you need a split to contain a single entry.
WriterOptions TinyWriter() {
  WriterOptions options;
  options.SetUnsorted();
  options.SetCompressed();
  options.SetBlockSize(0);
  return options;
}


///////////////////////////////////////////////////////////////////////////////
// StringTableWriter
///////////////////////////////////////////////////////////////////////////////

StringTableShardWriter::StringTableShardWriter() :
    is_open_(false),
    uncompressed_data_stream_(NULL),
    coded_uncompressed_data_stream_(NULL),
    num_entries_(0),
    cur_block_initial_entry_index_(0),
    num_opens_(0)
     {
}

StringTableShardWriter::~StringTableShardWriter() {
  //LOG(INFO) << "StringTableShardWriter::~StringTableShardWriter() - " << this;
  Close(); // in case user forgets to call Close
}


bool StringTableShardWriter::Open(const std::string& uri, WriterOptions options) {
  // TODO(kheath): Currently, Close() doesn't leave writer in a state where it
  // can be re-used (that is Open() can't be called again)...
  CHECK_EQ(num_opens_, 0) << "Can't reuse a Writer.  Please, create a new one.";
  ++num_opens_;

  CHECK(!is_open_) << "Can only call Open once on same Writer.";
  options_ = options;
  output_.reset(pert::OpenOutput(uri));
  if(!output_.get()){
    LOG(ERROR) << "failed to open uri: " << uri;
    return false;
  }

  is_open_ = true;
  is_sorted_ = false;
  if (!options_.comparator_name_.empty() && options_.comparator_name_ != "none"){
    is_sorted_ = true;
    comparator_.reset(pert::CreateComparator(options_.comparator_name_));
  }

  num_entries_ = 0;
  compression_codec_name_ = options_.compression_codec_name_;
  compression_codec_.reset(pert::CreateCompressionCodec(compression_codec_name_));

  // create data block compression buffers
  desired_uncompressed_data_block_size_ = options_.desired_uncompressed_data_block_size_;
  ResetDataBlockBuffers();

  // create bloom filter generator
  if (options.create_bloom_filter_){
    //bloom_filter_generator_.reset(new pert::KeyBloomFilterGenerator(options.bloom_filter_capacity_, options.bloom_error_rate_));
    bloom_filter_generator_.reset(new pert::KeyBloomFilterGenerator(options.bloom_filter_num_bits_));
  }

  // Write magic value to start file
  output_->OpenBlock();
  output_->Write(PERT_FILE_MAGIC);
  output_->CloseBlock();
  return true;
}

bool StringTableShardWriter::Add(const std::string& key, const std::string& value){
  CHECK(is_open_) << "Must call Open() before Add().";

  uint64 combined_size = key.size() + value.size();
  const int max_entry_size = 512*(2<<20);
  CHECK_LT(combined_size, max_entry_size) << "Combined size of key and value exceeds limit: (" << combined_size << " > " << max_entry_size << ").";

  if (num_entries_ == 0){
    first_key_ = key;
  }
  else {
    if (is_sorted_){
      int result = comparator_->CompareKey(last_key_.data(), last_key_.size(), key.data(), key.size());
      CHECK(result <= 0) << "Because configured to be sorted, keys must be added in non-descending order. The prev added key was: " << last_key_ << " and next added was: " << key;
    }
  }

  last_key_ = key;

  CHECK(coded_uncompressed_data_stream_);

  coded_uncompressed_data_stream_->WriteVarint64(key.size());
  coded_uncompressed_data_stream_->WriteVarint64(value.size());
  coded_uncompressed_data_stream_->WriteRaw(key.data(), key.size());
  coded_uncompressed_data_stream_->WriteRaw(value.data(), value.size());
  num_entries_++;

  if (coded_uncompressed_data_stream_->ByteCount() >= desired_uncompressed_data_block_size_){
    FlushCurrentDataBlock();
  }

  if (options_.create_bloom_filter_){
    bloom_filter_generator_->Add(key);
  }

  return true;
}

void StringTableShardWriter::FlushCurrentDataBlock() {
  int uncompressed_data_size = coded_uncompressed_data_stream_->ByteCount();

  // If no data to flush, return immediately... Don't create a block with no data
  if (!uncompressed_data_size){
    return;
  }

  //this flushes all data to the internal string
  delete coded_uncompressed_data_stream_;
  coded_uncompressed_data_stream_ = NULL;
  delete uncompressed_data_stream_;
  uncompressed_data_stream_ = NULL;

  compressed_data_.clear();
  compression_codec_->Compress(uncompressed_data_.data(), uncompressed_data_.size(), &compressed_data_);

  CompressedBlockHeader header;
  header.set_block_magic(PERT_DATA_BLOCK_MAGIC);
  header.set_uncompressed_size(uncompressed_data_size);

  uint64 block_start = output_->OpenBlock();
  output_->Write(header);
  uint64 header_end = output_->CloseBlock();

  CHECK_EQ(header_end-block_start, PERT_COMPRESSED_BLOCK_HEADER_SIZE) << "Write failed to file: " << output_->GetUri() << " (This may happen if file system is full.)";

  // write compressed block to output,
  output_->OpenBlock();
  output_->Write(compressed_data_);
  md5_digest_.Update(compressed_data_.data(), compressed_data_.size());
  uint64 block_end = output_->CloseBlock();
  uint64 block_size = block_end - block_start;

  // update data index
  DataIndexRecord* new_data_record = data_index_.add_blocks();
  new_data_record->set_offset(block_start);
  new_data_record->set_size(block_size);
  new_data_record->set_initial_entry_index(cur_block_initial_entry_index_);

  if (is_sorted_){
    new_data_record->mutable_last_key()->set_data(last_key_);
  }

  data_compressed_bytes_ += compressed_data_.size();
  data_uncompressed_bytes_ += uncompressed_data_size;

  // record the index of the first entry in the next block to be flushed
  cur_block_initial_entry_index_ = num_entries_;

  // Reset buffer for next data block
  ResetDataBlockBuffers();

  //LOG(INFO) << "flushed data block";
}

void StringTableShardWriter::ResetDataBlockBuffers(){
  if (coded_uncompressed_data_stream_) delete coded_uncompressed_data_stream_;
  if (uncompressed_data_stream_) delete uncompressed_data_stream_;

  uncompressed_data_.clear();
  uncompressed_data_stream_ = new google::protobuf::io::StringOutputStream(&uncompressed_data_);
  coded_uncompressed_data_stream_ = new google::protobuf::io::CodedOutputStream(uncompressed_data_stream_);
}


void StringTableShardWriter::AddMetadata(const std::string& name, const std::string& data){
  metadata_[name] = data;
}

void StringTableShardWriter::WriteMetadataBlock(const std::string& name, const std::string& data){
  CompressedBlockHeader header;
  header.set_block_magic(PERT_META_BLOCK_MAGIC);
  header.set_uncompressed_size(data.size());

  uint64 block_start = output_->OpenBlock();
  output_->Write(header);
  uint64 header_end = output_->CloseBlock();;
  CHECK_EQ(header_end-block_start, PERT_COMPRESSED_BLOCK_HEADER_SIZE);

  // write compressed block to output,
  output_->OpenBlock();
  output_->Write(data);
  uint64 block_end = output_->CloseBlock();
  uint64 block_size = block_end - block_start;

  // update data index
  MetadataIndexRecord* new_data_record = metadata_index_.add_blocks();
  new_data_record->set_name(name);
  new_data_record->set_offset(block_start);
  new_data_record->set_size(block_size);
}

void StringTableShardWriter::Close() {
  //LOG(INFO) << "StringTableShardWriter::Close()" << this;
  if (!is_open_){
    return;
  }
  //LOG(INFO) << "StringTableShardWriter::Close() - " << this;

  // finish writing current data block
  FlushCurrentDataBlock();

  // Add metadata block for bloom filter
  if (options_.create_bloom_filter_){
    BloomFilterData bloom_filter_data;
    bloom_filter_generator_->Save(&bloom_filter_data);
    AddMetadata(PERT_BLOOM_FILTER_METADATA_NAME, bloom_filter_data.SerializeAsString());
  }

  // write all metadata blocks
  for (MetadataMap::iterator iter = metadata_.begin(); iter != metadata_.end(); ++iter){
    WriteMetadataBlock(iter->first, iter->second);
  }

  Tail tail;
  tail.set_file_version(PERT_FILE_VERSION);
  tail.set_file_magic(PERT_FILE_MAGIC);

  // Write data index
  uint64 data_index_size = data_index_.ByteSize();
  uint64 data_index_offset = output_->OpenBlock();
  output_->Write(data_index_);
  uint64 data_index_end = output_->CloseBlock();
  CHECK_EQ(data_index_end - data_index_offset, data_index_size);

  tail.mutable_data_index_location()->set_offset(data_index_offset);
  tail.mutable_data_index_location()->set_size(data_index_size);

  // Write metadata index
  uint64 metadata_index_size = metadata_index_.ByteSize();
  uint64 metadata_index_offset = output_->OpenBlock();
  output_->Write(metadata_index_);
  uint64 metadata_index_end = output_->CloseBlock();
  CHECK_EQ(metadata_index_end - metadata_index_offset, metadata_index_size);

  tail.mutable_metadata_index_location()->set_offset(metadata_index_offset);
  tail.mutable_metadata_index_location()->set_size(metadata_index_size);

  // Write file info
  FileInfo file_info;
  file_info.set_num_entries(num_entries_);
  file_info.set_compression_codec(compression_codec_name_);

  if (is_sorted_){
    OrderedKeyInfo ordered_key_info;
    ordered_key_info.set_comparator(options_.comparator_name_);
    ordered_key_info.mutable_first_key()->set_data(first_key_);
    ordered_key_info.mutable_last_key()->set_data(last_key_);
    ordered_key_info.CheckInitialized();
    file_info.mutable_ordered_key_info()->CopyFrom(ordered_key_info);
  }

  file_info.set_compression_ratio(double(data_uncompressed_bytes_)/data_compressed_bytes_);
  file_info.set_fingerprint(md5_digest_.GetAsHexString()); // fingerprint useful as a UUID for this immutable data set (e.g. for provenance tracking purposes)

  //TODO: populate key and value stats and add to file info
  //Stats key_stats;
  //Stats value_stats;
  //file_info.mutable_key_stats()->CopyFrom(key_stats);
  //file_info.mutable_value_stats()->CopyFrom(value_stats);

  file_info.CheckInitialized();

  uint64 file_info_size = file_info.ByteSize();
  uint64 file_info_offset = output_->OpenBlock();
  output_->Write(file_info);
  uint64 file_info_end = output_->CloseBlock();

  CHECK_EQ(file_info_end - file_info_offset, file_info_size);

  tail.mutable_file_info_location()->set_offset(file_info_offset);
  tail.mutable_file_info_location()->set_size(file_info_size);

  tail.CheckInitialized();
  CHECK_EQ(PERT_TAIL_SIZE, tail.ByteSize());
  uint64 tail_offset = output_->OpenBlock();
  output_->Write(tail);
  uint64 tail_end = output_->CloseBlock();
  CHECK_EQ(tail_end - tail_offset, PERT_TAIL_SIZE);

  is_open_ = false;
  output_.reset(NULL);

  /*
  metadata_index_.Clear();
  metadata_.clear();
  uncompressed_data_.clear();
  compressed_data_.clear();
  data_index_.Clear();
  first_key_.clear();
  last_key_.clear();
  num_entries_ = 0;
  cur_block_initial_entry_index_ = 0;
  data_compressed_bytes_ = 0;
  data_uncompressed_bytes_ = 0;
  bloom_filter_generator_.reset();
*/

}


///////////////////////////////////////////////////////////////////////////////
// StringTableReader
///////////////////////////////////////////////////////////////////////////////

StringTableShardReader::StringTableShardReader() :
    is_open_(false),
    is_sorted_(false)
{

}

StringTableShardReader::~StringTableShardReader() {

}

void StringTableShardReader::Close(){
  is_open_ = false;
  input_.reset(NULL);
}

bool StringTableShardReader::Open(const std::string& uri){
  return OpenSplit(uri, 0ULL, 0ULL);
}

// Reads essential shard info without fully opening the shard for reading.
// static method
bool StringTableShardReader::GetShardInfo(const std::string& uri, pert::FileInfo* info ){
  CHECK(info);
  boost::scoped_ptr<pert::InputCodedBlockFile> input;

  input.reset(pert::OpenInput(uri));
  if(!input.get()){
    LOG(ERROR) << "failed to open uri: " << uri;
    return false;
  }
  CHECK_GT(input->Size(), 0) << "File is empty: " << uri;

  // Read tail
  Tail tail;
  CHECK_GE(input->Size(), PERT_TAIL_SIZE) << "File is too small to be a valid PERT file.";
  uint64 tail_offset = input->Size()-PERT_TAIL_SIZE;

  CHECK(input->OpenBlock(tail_offset, PERT_TAIL_SIZE));
  if (!input->Read(&tail)){
    LOG(ERROR) << "failed parsing tail of file: " << uri;
    LOG(ERROR) << "tail_offset: " << tail_offset;
    return false;
  }

  // Sanity check tail


  // Read file info
  CHECK(input->OpenBlock(tail.file_info_location().offset(), tail.file_info_location().size()));
  if (!input->Read(info)){
    LOG(ERROR) << "failed parsing file info: " << uri;
    return false;
  }
  return true;
}

bool StringTableShardReader::OpenSplit(const std::string& uri, uint64 split_start, uint64 split_end){
  CHECK(!is_open_) << "Reader is already open.";
  uri_ = uri;
  CHECK_GE(split_end, split_start) << "Make sure you pass end of split, not length of split";

  input_.reset(pert::OpenInput(uri));
  if(!input_.get()){
    LOG(ERROR) << "Failed to open uri: " << uri;
    return false;
  }

  if (input_->Size() == 0){
    LOG(WARNING) << "File is empty: " << uri;
    return false;
  }

  // Read tail
  Tail tail;
  CHECK_GT(input_->Size(), PERT_TAIL_SIZE);
  uint64 tail_offset = input_->Size()-PERT_TAIL_SIZE;

  CHECK(input_->OpenBlock(tail_offset, PERT_TAIL_SIZE));
  if (!input_->Read(&tail)){
    LOG(ERROR) << "failed parsing tail of file: " << uri;
    LOG(ERROR) << "tail_offset: " << tail_offset;
    return false;
  }

  // Read file info
  CHECK(input_->OpenBlock(tail.file_info_location().offset(), tail.file_info_location().size()));
  if (!input_->Read(&file_info_)){
    LOG(ERROR) << "failed parsing file info: " << uri;
    return false;
  }

  if (file_info_.has_compression_codec()){
    compression_codec_.reset(pert::CreateCompressionCodec(file_info_.compression_codec()));
  }

  if (file_info_.has_ordered_key_info()){
    comparator_.reset(pert::CreateComparator(file_info_.ordered_key_info().comparator()));
    CHECK(comparator_.get());
    is_sorted_ = true;
  }

  // Read metadata index
  CHECK(input_->OpenBlock(tail.metadata_index_location().offset(), tail.metadata_index_location().size()));
  if (!input_->Read(&metadata_index_)){
    LOG(ERROR) << "failed parsing metadata index: " << uri;
    return false;
  }

  // Read data index
  CHECK(input_->OpenBlock(tail.data_index_location().offset(), tail.data_index_location().size()));
  if (!input_->Read(&data_index_)){
    LOG(ERROR) << "failed parsing data index: " << uri;
    return false;
  }

  is_open_=true;

  // if there is no split, read all blocks
  if (split_start == 0 && split_end == 0){
    min_block_id_ = 0;

    // TODO: this is a hack to work around a design flaw
    // because max_block_id_ is unsigned and uses inclusive semantics, we can't indicate when there are zero blocks aka max_block_id_ < min_block_id_
    // consider changing to iterator semantics where end is "past the end" and fix all the off-by-one errors this introduces
    max_block_id_ = 0;
    if (data_index_.blocks_size() > 0) { // make sure we don't wrap the unsigned max_block_id_ by subtracting one
      max_block_id_ = data_index_.blocks_size()-1;
    }

    min_entry_index_ = 0;
    max_entry_index_ = Entries();
  }
  else{ // compute the first and last data blocks to decode from split restrictions
    // find the first data block that started at or before the start of the split position
    min_block_id_ = DataBlockStartingBeforePosition(split_start);

    // find the last data block that ends before end of this split
    max_block_id_ = DataBlockEndingBeforePosition(split_end);

    CHECK_GE(min_block_id_, 0);
    CHECK_LE(max_block_id_, data_index_.blocks_size());

    // handle case that split contains the tail section of file with no data blocks
    if (min_block_id_ == data_index_.blocks_size()){
      max_entry_index_ = 0;
      min_entry_index_ = 0;
    }
    else{
    // invariant: at least one block was selected (i.e. split size > data block size)
      LOG(INFO) << "data_index_.blocks_size(): " << data_index_.blocks_size();
      LOG(INFO) << "split_start: " << split_start;
      LOG(INFO) << "split_end: " << split_end;
      LOG(INFO) << "min_block_id_: " << min_block_id_ << "size: " << data_index_.blocks(min_block_id_).size();
      LOG(INFO) << "max_block_id_: " << max_block_id_ << "size: " << data_index_.blocks(max_block_id_).size();

      CHECK_GE(max_block_id_, min_block_id_) << "You can't request a split size smaller than the data block size.  Either increase the split size or recreate the input with a smaller data block size.";

      min_entry_index_ = data_index_.blocks(min_block_id_).initial_entry_index();
      if (max_block_id_+1 < data_index_.blocks_size()){
        max_entry_index_ = data_index_.blocks(max_block_id_+1).initial_entry_index()-1;
      }
      else{
        max_entry_index_ = Entries()-1;
      }
    }
  }

  entries_in_split_ = max_entry_index_ - min_entry_index_;

  // Load bloom filter if present
  std::string bloom_data_value;
  if (GetMetadata(PERT_BLOOM_FILTER_METADATA_NAME, &bloom_data_value)){
    BloomFilterData bloom_filter_data;
    CHECK(bloom_filter_data.ParseFromString(bloom_data_value));
    bloom_filter_.reset(new KeyBloomFilter(bloom_filter_data));
  }

  SeekToStart();
  return true;
}

uint64 StringTableShardReader::Entries(){
  CHECK(is_open_) << "Must call Open() before Entries().";
  return file_info_.num_entries();
}


struct IndexComparator {
  bool operator()(uint64 index, const DataIndexRecord& r) const {
    return index < r.initial_entry_index();
  }
};

bool StringTableShardReader::GetIndex(uint64 index, std::string* key, std::string* value){
  CHECK_NOTNULL(key);
  CHECK_NOTNULL(value);

  const char* key_buf;
  uint64 key_buf_len;
  const char* value_buf;
  uint64 value_buf_len;
  bool success = GetIndexNoCopy(index, &key_buf, &key_buf_len, &value_buf, &value_buf_len);

  if (success){
    key->assign(key_buf, key_buf_len);
    value->assign(value_buf, value_buf_len);
  }

  return success;
}

bool StringTableShardReader::GetIndexNoCopy(uint64 index, const char** key_buf, uint64* key_buf_len, const char** value_buf, uint64* value_buf_len){

  // Check that the index is in correct range
  // Because it is possible to open a subset of a shard, the range may not be the same as [0, num_entries)
  if(index > max_entry_index_ || index < min_entry_index_){
    LOG(INFO) << "requested index not in the currently open shard split.";
    return false;
  }

  // find which block contains the requested index
  typedef ::google::protobuf::RepeatedPtrField< DataIndexRecord > RepeatedDataIndexRecord;
  RepeatedDataIndexRecord::const_iterator upper = std::upper_bound(data_index_.blocks().begin(), data_index_.blocks().end(), index, IndexComparator());
  int desired_block_id = upper - data_index_.blocks().begin() - 1;

  CHECK_GE(desired_block_id, 0);
  CHECK_LE(desired_block_id, max_block_id_);

  if (desired_block_id != cur_block_id_){
    cur_block_id_ = desired_block_id;
    LoadDataBlock(cur_block_id_);
  }


  if (index < cur_entry_index_){
    //LOG(INFO) << "rewinding inside current block: " << cur_block_id_ << " cur_entry_index_: " << cur_entry_index_ << " index: " << index;
    LoadDataBlock(cur_block_id_);
  }

  CHECK_GE(index, cur_entry_index_);
  uint64 block_index = index - cur_entry_index_;

  // advance to the correct offset inside the active block
  for (int skipped=0; skipped <= block_index; ++skipped){
    CHECK(NextNoCopy(key_buf, key_buf_len, value_buf, value_buf_len));
  }

  return true;
}


// moves the cursor back to the first key in the scan range
void StringTableShardReader::SeekToStart(){
  CHECK(is_open_) << "Must call Open() before SeekToStart().";
  cur_block_id_ = min_block_id_;
  LoadDataBlock(cur_block_id_);
}


// returns false if at end, otherwise returns true and advances the cursor
bool StringTableShardReader::Next(std::string* key, std::string* value){
  const char* key_buf;
  uint64 key_buf_len;
  const char* value_buf;
  uint64 value_buf_len;
  bool success = NextNoCopy(&key_buf, &key_buf_len, &value_buf, &value_buf_len);

  if (success){
    key->assign(key_buf, key_buf_len);
    value->assign(value_buf, value_buf_len);
  }

  return success;
}

bool StringTableShardReader::NextNoCopy(const char** key_buf, uint64* key_buf_len, const char** value_buf, uint64* value_buf_len){
  CHECK(is_open_) << "Must call Open() before Next().";

  if (!cur_block_has_more_data_){
    if (!LoadNextDataBlock()){
      return false;
    }
  }

  CHECK(data_block_coded_stream_.get());

  CHECK(data_block_coded_stream_->ReadVarint64(key_buf_len));
  CHECK(data_block_coded_stream_->ReadVarint64(value_buf_len));
  int remaining_buf_size;
  CHECK(data_block_coded_stream_->GetDirectBufferPointer((const void**)key_buf, &remaining_buf_size));

  CHECK_GE(remaining_buf_size, 0);
  CHECK_LT(*key_buf_len, kint32max);
  CHECK_GE(*key_buf_len, 0);
  CHECK(data_block_coded_stream_->Skip(*key_buf_len)) << "failed to skip key_buf_len: " << *key_buf_len;
  if (data_block_coded_stream_->GetDirectBufferPointer((const void**)value_buf, &remaining_buf_size)){
    CHECK_GT(remaining_buf_size, 0);
    CHECK_LT(*value_buf_len, kint32max);
    CHECK(data_block_coded_stream_->Skip(*value_buf_len));
    const void* tmp;
    if (!data_block_coded_stream_->GetDirectBufferPointer(&tmp, &remaining_buf_size)){
      cur_block_has_more_data_ = false;
    }
  }
  else{
    cur_block_has_more_data_ = false;
  }
  cur_entry_index_++;
  return true;
}



struct CustomComparator {
  CustomComparator(const pert::KeyComparator* key_comparator) : key_comparator_(key_comparator) { }

  bool operator()(const DataIndexRecord& r, const std::string& k) const {
    //LOG(INFO) << "comparing block record with last key: " << r.last_key().data() << " k: " << k;
    if (key_comparator_->CompareKey(r.last_key().data().data(), r.last_key().data().size(), k.data(), k.size()) < 0){
      return true;
    }
    return false;
  }
  const pert::KeyComparator* key_comparator_;
};


bool StringTableShardReader::Find(const std::string& key, std::string* value){
  bool hit_disk;
  return FindVerbose(key, value, &hit_disk);
}

/*
bool StringTableShardReader::FindVerbose(const std::string& key, std::string* value, bool* hit_disk){
  CHECK(is_open_) << "Must call Open() before Find().";
  CHECK(is_sorted_) << "File must be sorted to use Find() method";
  CHECK(hit_disk);

  // TODO(kheath): Perhaps this is too aggresive to make it FATAL?
  CHECK(bloom_filter_) << "No bloom filter is present, using Find is inefficient in this case.  You have been warned.";

  // If bloom filter is available, check if key can not be in this shard and if so, abort early
  if (bloom_filter_){
    if (!bloom_filter_->ProbablyContains(key)){
      *hit_disk = false;
      return false;
    }
  }

  // If possible that key is in shard, check for it.
  // TODO(kheath): Seeking back to start makes implementation simple, but inefficient for seeking to keys in sorted order.  If this is a hotspot, consider checking if we need to seek to start or not.
  // TODO(kheath): Add test case!
  //LOG(INFO) << "hitting disk!";
  *hit_disk = true;
  SeekToStart();
  std::string found_key;
  NextGreaterEqual(key, &found_key, value);
  return key == found_key;
}
*/

bool StringTableShardReader::FindVerbose(const std::string& key, std::string* value, bool* hit_disk){
  CHECK(is_open_) << "Must call Open() before Find().";
  CHECK(is_sorted_) << "File must be sorted to use Find() method";
  CHECK(hit_disk);

  // TODO(kheath): Perhaps this is too aggresive to make it FATAL?
  //CHECK(bloom_filter_) << "No bloom filter is present, using Find is inefficient in this case.  You have been warned.";

  // If bloom filter is available, check if key can not be in this shard and if so, abort early
  if (bloom_filter_){
    if (!bloom_filter_->ProbablyContains(key)){
      *hit_disk = false;
      return false;
    }
  }
  /*
  else{
    LOG(INFO) << "No bloom filter is present, using Find on a multi-sharded table is inefficient in this case.  You have been warned.";
  }
  */

  // If possible that key is in shard, check for it.
  // TODO(kheath): Seeking back to start makes implementation simple, but inefficient for seeking to keys in sorted order.  If this is a hotspot, consider checking if we need to seek to start or not.
  // TODO(kheath): Add test case!
  //LOG(INFO) << "hitting disk!";
  *hit_disk = true;

  std::string found_key;

  // Try to seek from our current position
  NextGreaterEqual(key, &found_key, value);

  // If we didn't find key, wrap around and check if it was before our initial position
  if (found_key != key){
    SeekToStart();
    NextGreaterEqual(key, &found_key, value);
  }

  return key == found_key;
}


// Advances from cur entry to the next entry with key greater than or equal to the query key.  (always advances forward at least one entry)
// Returns true if such a key is found, false otherwise (we reached end of table and found no key greater or equal to query)
// More efficient than calling Next repeatedly because the index may allow us to skip decompressing some data blocks.
// If there are n entries with key K, calling with query K will iterate through these n entries.

bool StringTableShardReader::NextGreaterEqualNoCopy(const std::string& query_key, const char** key_buf, uint64* key_buf_len, const char** value_buf, uint64* value_buf_len) {
  CHECK(is_open_) << "Must call Open() before NextGreaterEqualNoCopy().";
  CHECK(is_sorted_) << "File must be sorted to use NextGreaterEqualNoCopy() method";
  CHECK(comparator_.get());
  CHECK(key_buf);
  CHECK(key_buf_len);
  CHECK(value_buf);
  CHECK(value_buf_len);
  int cmp;

  // handle case when shard has no entries
  if (!data_index_.blocks_size()){
    return false;
  }

  // If the largest key in current data block is strictly smaller than query key, then cur block can not contain a suitable key and we need to skip ahead at least one block
  //CHECK(data_index_.blocks(cur_block_id_).has_last_key());
  //CHECK(data_index_.blocks(cur_block_id_).last_key().has_data());
  const std::string& cur_block_last_key = data_index_.blocks(cur_block_id_).last_key().data();
  cmp = comparator_->CompareKey(cur_block_last_key.data(), cur_block_last_key.size(), query_key.data(), query_key.size());
  if (cmp < 0) { // cur block can not contain any matches, so we need to skip ahead at least one block
    // find how far we need to skip ahead by examine the index
    typedef ::google::protobuf::RepeatedPtrField< DataIndexRecord > RepatedDataIndexRecord;

    RepatedDataIndexRecord::const_iterator low = std::lower_bound(data_index_.blocks().begin(), data_index_.blocks().end(), query_key, CustomComparator(comparator_.get()));

    // if the query key is larger than the largest key in the table, move cursor past end and return false to indicate no suitable key can be found
    if (low == data_index_.blocks().end()){
      cur_block_id_ = max_block_id_;
      cur_block_has_more_data_ = false;
      return false; // all keys in table are stricktly smaller than the query key
    }

    int block_id = low - data_index_.blocks().begin();

    CHECK_NE(block_id, cur_block_id_); // invariant: if we get here, we already ruled out the possibility that the key could be in the current block

    CHECK_GT(block_id, cur_block_id_) << "Your query key must be greater or equal to the previous query key on each call.  It appears this constraint was violated.";

    cur_block_id_ = block_id;
    LoadDataBlock(block_id);
  }


  // If a matching key is in the table, it must be in the current block
  // Step through current block to try to find it
  while(NextNoCopy(key_buf, key_buf_len, value_buf, value_buf_len)){
    // If next key is a match, return true
    cmp = comparator_->CompareKey(*key_buf, *key_buf_len, query_key.data(), query_key.size());
    if (cmp >= 0 ){ // since we have found an entry with key >= than query key, we can return true
      return true;
    }
  }
  return false;
}


bool StringTableShardReader::NextGreaterEqual(const std::string& query_key, std::string* key, std::string* value){
  const char* key_buf;
  uint64 key_buf_len;
  const char* value_buf;
  uint64 value_buf_len;
  bool result = NextGreaterEqualNoCopy(query_key, &key_buf, &key_buf_len, &value_buf, &value_buf_len);
  if (result){
    key->assign(key_buf, key_buf_len);
    value->assign(value_buf, value_buf_len);
  }
  return result;
}

/*
// moves the cursor to the first key greater than or equal to the input key, returns true if the key equals the input key
//TODO: This is inefficient to do frequently because it reloads the datablock each time in order to back up...
bool StringTableShardReader::SeekTo(const std::string& key) {
  CHECK(is_open_) << "Must call Open() before SeekTo().";
  CHECK(is_sorted_) << "File must be sorted to use SeekTo() method";

  //for (int i=0; i < data_index_.blocks_size(); ++i){
    //const string last_key = data_index_.blocks(i).last_key().data();
    //LOG(INFO) << "block: " << i << " last_key: " << last_key;
  //}

  // examine the index to find which block may contain the key
  typedef ::google::protobuf::RepeatedPtrField< DataIndexRecord > RepatedDataIndexRecord;

  RepatedDataIndexRecord::const_iterator low = std::lower_bound(data_index_.blocks().begin(), data_index_.blocks().end(), key, CustomComparator(comparator_.get()));

  // if the key is larger than the largest key in the table, move cursor past end and return false to indicate key not found
  if (low == data_index_.blocks().end()){
    cur_block_id_ = max_block_id_;
    cur_block_has_more_data_ = false;
    return false;
  }

  // otherwise, examine the candidate block
  int block_id = low - data_index_.blocks().begin();

  //LOG(INFO) << "block_id: " << block_id;
  //LOG(INFO) << data_index_.blocks(block_id).DebugString();

  LoadDataBlock(block_id);

  const char* key_buf;
  uint64 key_buf_len;
  const char* value_buf;
  uint64 value_buf_len;
  bool found_match = false;
  uint64 rows_inspected = 0;
  while ( NextNoCopy(&key_buf, &key_buf_len, &value_buf, &value_buf_len) ){
    int cmp = comparator_->CompareKey(key_buf, key_buf_len, key.data(), key.size());
    if (cmp >= 0){
      if (cmp == 0){
        found_match = true;
      }
      break;
    }
    rows_inspected++;
  }

  // if we found a match, we need to rewind so that next will return found match
  if (found_match) {
    // reload the block id
    LoadDataBlock(block_id);

    // advance cursor to just before the matching position
    for (uint64 i=0; i < rows_inspected; ++i){
      NextNoCopy(&key_buf, &key_buf_len, &value_buf, &value_buf_len);
    }
  }

  return found_match;
}
*/

std::vector<std::string> StringTableShardReader::ListMetadataNames() const{
  std::vector<std::string> metadata_names;
  for(uint32 block_id=0; block_id < metadata_index_.blocks_size(); block_id++){
    const MetadataIndexRecord* metadata_block_record = &(metadata_index_.blocks(block_id));
    metadata_names.push_back(metadata_block_record->name());
  }
  return metadata_names;
}

bool StringTableShardReader::HasMetadata(const std::string& name) const{
  uint32 block_id = 0;
  bool found_block = false;
  for(block_id=0; block_id < metadata_index_.blocks_size(); block_id++){
    const MetadataIndexRecord* metadata_block_record = &metadata_index_.blocks(block_id);
    if (metadata_block_record->name() == name){
      found_block = true;
      break;
    }
  }
  return found_block;
}

bool StringTableShardReader::GetMetadata(const std::string& name, std::string* data) const{
  uint32 block_id = 0;
  bool found_block = false;
  for(block_id=0; block_id < metadata_index_.blocks_size(); block_id++){
    const MetadataIndexRecord* metadata_block_record = &metadata_index_.blocks(block_id);
    if (metadata_block_record->name() == name){
      CHECK(LoadMetadataBlock(metadata_block_record, data));
      found_block = true;
      break;
    }
  }

  return found_block;
}


float StringTableShardReader::SplitProgress(){
  double frac;
  if (cur_block_id_ == data_index_.blocks_size()){ // handle the case where split doesn't contain any data blocks... cur_block is past end of valid blocks
    frac = 1.0;
  }
  else{
    frac = (cur_entry_index_ - min_entry_index_)/(double)entries_in_split_;
  }
  return frac;
}

uint64 StringTableShardReader::Size(){
  CHECK(is_open_);
  return input_->Size();
}

double StringTableShardReader::CompressionRatio(){
  CHECK(is_open_);
  return file_info_.compression_ratio();
}

std::string StringTableShardReader::CompressionCodecName(){
  CHECK(is_open_);
  return file_info_.compression_codec();
}

std::string StringTableShardReader::ComparatorName(){
  CHECK(is_open_);
  return file_info_.ordered_key_info().comparator();
}

uint64 StringTableShardReader::MinBlockSize(){
  CHECK(is_open_);
  // TODO(kheath): this can be stored in the file info proto so it doesn need to be computed... might be slow if there is a large index...
  uint64 min_block_size = kuint64max;

  for(int i=0; i < data_index_.blocks_size(); ++i){
    //LOG(INFO) << "block " << i << " size: " << data_index_.blocks(i).size();
    min_block_size = std::min(data_index_.blocks(i).size(), min_block_size);
  }

  //LOG(INFO) << "min_block_size: " << min_block_size;

  return min_block_size;
}

uint64 StringTableShardReader::MaxBlockSize(){
  CHECK(is_open_);
  // TODO(kheath): this can be stored in the file info proto so it doesn need to be computed... might be slow if there is a large index...
  uint64 max_block_size = 0;

  for(int i=0; i < data_index_.blocks_size(); ++i){
    //LOG(INFO) << "block " << i << " size: " << data_index_.blocks(i).size();
    max_block_size = std::max(data_index_.blocks(i).size(), max_block_size);
  }

  return max_block_size;
}

std::string StringTableShardReader::GetBlockTableInfo(){
  CHECK(is_open_);

  std::string info;
  for(int i=0; i < data_index_.blocks_size(); ++i){
    const pert::DataIndexRecord& block = data_index_.blocks(i);
    info += StringPrintf("block %04d  start: %016u  end: %016u  first_index: %u\n", i, block.offset(), block.offset() + block.size(), block.initial_entry_index());
  }
  return info;
}

bool StringTableShardReader::LoadMetadataBlock(const MetadataIndexRecord* metadata_block_record, std::string* data) const {

  CompressedBlockHeader block_header;
  uint64 header_start = metadata_block_record->offset();
  uint64 header_size = PERT_COMPRESSED_BLOCK_HEADER_SIZE;
  CHECK(input_->OpenBlock(header_start, header_size));
  CHECK(input_->Read(&block_header));

  // verify magic
  CHECK_EQ(block_header.block_magic(), PERT_META_BLOCK_MAGIC);

  // Load rest of datablock
  uint64 payload_start = metadata_block_record->offset() + PERT_COMPRESSED_BLOCK_HEADER_SIZE;
  uint64 payload_size = metadata_block_record->size() - PERT_COMPRESSED_BLOCK_HEADER_SIZE;
  CHECK(input_->OpenBlock(payload_start, payload_size));
  CHECK(input_->Read(data, payload_size));
  CHECK_EQ(data->size(), payload_size);
  input_->CloseBlock();
  return true;
}

//TODO: check that this doesn't leak memory
bool StringTableShardReader::LoadDataBlock(int block_id){
  // Check for case of empty file
  if (data_index_.blocks_size() == 0 || block_id == data_index_.blocks_size()){
    cur_block_has_more_data_ = false;
    return false;
  }

  CHECK_LT(block_id, data_index_.blocks_size());
  CHECK_LE(block_id, max_block_id_);
  CHECK_GE(block_id, min_block_id_);

  const DataIndexRecord* data_block_record = data_index_.mutable_blocks(block_id);

  // Load CompressedBlockHeader
  uint64 data_block_header_start = data_block_record->offset();
  uint64 data_block_header_size = PERT_COMPRESSED_BLOCK_HEADER_SIZE;
  CompressedBlockHeader block_header;
  CHECK(input_->OpenBlock(data_block_header_start, data_block_header_size));
  CHECK(input_->Read(&block_header));

  // verify magic
  CHECK_EQ(block_header.block_magic(), PERT_DATA_BLOCK_MAGIC);

  // Load rest of datablock
  std::string compressed_data_block_;
  uint64 data_block_payload_start = data_block_record->offset() + PERT_COMPRESSED_BLOCK_HEADER_SIZE;
  uint64 data_block_payload_size = data_block_record->size() - PERT_COMPRESSED_BLOCK_HEADER_SIZE;
  CHECK(input_->OpenBlock(data_block_payload_start, data_block_payload_size));
  CHECK(input_->Read(&compressed_data_block_, data_block_payload_size));
  input_->CloseBlock();
  CHECK_GT(compressed_data_block_.size(), 0);

  // decompress
  // TODO(kheath): try the swap trick to allow decompress vector to shrink
  compression_codec_->Decompress(compressed_data_block_.data(), compressed_data_block_.size(), &decompressed_data_block_);
  CHECK_EQ(decompressed_data_block_.size(), block_header.uncompressed_size());

  // create a coded input stream on decompressed string
  data_block_coded_stream_.reset(new google::protobuf::io::CodedInputStream((const uint8*)decompressed_data_block_.data(), decompressed_data_block_.size()));
  data_block_coded_stream_->SetTotalBytesLimit(512*(2<<20),-1); // a coded stream can deal with sizes up to 500MB (see doc in coded_stream.h)
  cur_block_has_more_data_ = (decompressed_data_block_.size() > 0);

  /*
  if (!cur_block_has_more_data_){
    LOG(INFO) << "uri_: " << uri_;
    LOG(INFO) << "block_id: " << block_id;
    LOG(INFO) << "decompressed_data_block_.size(): " << decompressed_data_block_.size();
    LOG(INFO) << "block_header.uncompressed_size(): " << block_header.uncompressed_size();
    LOG(INFO) << "data_index_: " << data_index_.DebugString();
  }
  */

  cur_entry_index_ = data_block_record->initial_entry_index();

  return cur_block_has_more_data_;
}

bool StringTableShardReader::LoadNextDataBlock(){
  bool next_block_loaded;
  if (cur_block_id_ < max_block_id_){
    cur_block_id_++;
    CHECK(LoadDataBlock(cur_block_id_)) << "failed loading block " << cur_block_id_ << " of " <<  max_block_id_;
    next_block_loaded = true;
  }
  else{
    next_block_loaded = false;
  }
  return next_block_loaded;
}


typedef ::google::protobuf::RepeatedPtrField< ::pert::DataIndexRecord > RepatedDataIndexRecord;

struct BlockStartComparator {
  bool operator()(const DataIndexRecord& r, uint64 pos) const {
    if (r.offset() < pos){
      return true;
    }
    return false;
  }
};

struct BlockEndComparator {
  bool operator()(const ::pert::DataIndexRecord& r, uint64 pos) const {
    if (r.offset()+r.size() < pos){
      return true;
    }
    return false;
  }
};

// returns the index of the data block that started just before the given disk byte offset, or 0 if there are no blocks that started before the given disk byte offset
int StringTableShardReader::DataBlockStartingBeforePosition(uint64 pos) {
  RepatedDataIndexRecord::const_iterator low = std::lower_bound(data_index_.blocks().begin(), data_index_.blocks().end(), pos, BlockStartComparator());
  int block_id = low - data_index_.blocks().begin();
  return block_id;
}

// returns the index of the data block that ends just before the given disk byte offset
int StringTableShardReader::DataBlockEndingBeforePosition(uint64 pos) {
  if (!data_index_.blocks().size()){
    return 0;
  }

  RepatedDataIndexRecord::const_iterator low = std::lower_bound(data_index_.blocks().begin(), data_index_.blocks().end(), pos, BlockEndComparator());
  int block_id = low - data_index_.blocks().begin();

  if (block_id == data_index_.blocks_size()){
    block_id = data_index_.blocks_size()-1;
  }

  return block_id;
}


///////////////////////////////////////////////////////////////////////////////
// StringTableShardSetReader
///////////////////////////////////////////////////////////////////////////////

StringTableShardSetReader::StringTableShardSetReader() :
    is_open_(false)
{

}

StringTableShardSetReader::~StringTableShardSetReader(){

}

bool StringTableShardSetReader::Open(const std::string& uri){
  vector<string> shard_uris = pert::GetShardUris(uri);
  return Open(shard_uris);
}

bool StringTableShardSetReader::Open(const std::vector<std::string>& uris){
  uris_ = uris;
  std::sort(uris_.begin(),uris_.end()); // sort so part ordering is stable so the index ordering will be independent of the input uri ordering
  if (uris_.empty()){
    return false;
  }

  BOOST_FOREACH(std::string uri, uris_){
    if (!Exists(uri)){
      LOG(INFO) << "uri does not exist: " << uri;
      return false;
    }
  }

  // Count the total number of entries and total size
  shard_last_index_.clear();
  num_entries_ = 0;

  BOOST_FOREACH(std::string uri, uris_){
    pert::FileInfo info;
    if (!StringTableShardReader::GetShardInfo(uri, &info)){
      LOG(INFO) << "Error getting info for shard: " << uri;
      return false;
    }
    num_entries_ += info.num_entries();
    shard_last_index_.push_back(num_entries_-1);
    //TODO(kheath): is it helpful to verify the shards are compatible or not needed?
  }

  is_open_ = true;
  SeekToStart();
  return true;
}

uint64 StringTableShardSetReader::Entries(){
  CHECK(is_open_) << "must call open first";
  return num_entries_;
}

void StringTableShardSetReader::SeekToStart(){
  CHECK(is_open_) << "must call open first";
  cur_uri_index_ = 0;
  cur_reader_.reset(new StringTableShardReader());
  CHECK(cur_reader_->Open(uris_[cur_uri_index_]));
}


bool StringTableShardSetReader::GetIndex(uint64 index, std::string* key, std::string* value){
  if (index >= num_entries_){
    LOG(INFO) << "Requested index larger than number of entries: " << index;
    return false;
  }

  uint64 desired_shard_num = shard_last_index_.size() - 1;

  std::vector<uint64>::const_iterator low = std::lower_bound(shard_last_index_.begin(), shard_last_index_.end(), index);

  if (low != shard_last_index_.end()){
    desired_shard_num = low - shard_last_index_.begin();
  }

  CHECK_LT(desired_shard_num, uris_.size());

  // If requested index is not in current shard, switch to that shard
  if (cur_uri_index_ != desired_shard_num){
    cur_uri_index_ = desired_shard_num;
    cur_reader_.reset(new StringTableShardReader());
    CHECK(cur_reader_->Open(uris_[cur_uri_index_]));
    //LOG(INFO) << "switching to shard " << cur_uri_index_;
  }

  // Advance to the offset index within the shard
  uint64 shard_relative_index = index;
  if (cur_uri_index_ > 0){
    shard_relative_index -= shard_last_index_[cur_uri_index_-1] + 1;;
  }
  //LOG(INFO) << "index: " << index << " shard_num: " << cur_uri_index_ << " shard_relative_index: " << shard_relative_index;
  return cur_reader_->GetIndex(shard_relative_index, key, value);
}

void StringTableShardSetReader::Close(){
  is_open_ = false;
}

bool StringTableShardSetReader::Next(std::string* key, std::string* value){
  CHECK(is_open_);
  CHECK(key);
  CHECK(value);

  const char* key_buf = NULL;
  uint64 key_buf_len = 0;
  const char* value_buf = NULL;
  uint64 value_buf_len = 0;

  bool success = NextNoCopy(&key_buf, &key_buf_len, &value_buf, &value_buf_len);

  if (success){
    key->assign(key_buf, key_buf_len);
    value->assign(value_buf, value_buf_len);
  }

  return success;
}



bool StringTableShardSetReader::NextNoCopy(const char** key_buf, uint64* key_buf_len,
    const char** value_buf, uint64* value_buf_len){

  bool success = true;
  while(true){
    success = cur_reader_->NextNoCopy(key_buf, key_buf_len, value_buf, value_buf_len);
    if (success){
      break; // done if we found a next pair
    }
    if (cur_uri_index_ < uris_.size()-1){
      cur_uri_index_++;
      cur_reader_.reset(new StringTableShardReader());
      CHECK(cur_reader_->Open(uris_[cur_uri_index_]));
    }
    else{
     break; // done if we have reached end of last pert
    }
  }

  return success;
}

bool StringTableShardSetReader::HasMetadata(const std::string& name) const {LOG(FATAL) << "not yet implemented"; return false;};
bool StringTableShardSetReader::GetMetadata(const std::string& name, std::string* data) const {LOG(FATAL) << "not yet implemented"; return false;};
std::vector<std::string> StringTableShardSetReader::ListMetadataNames() const {LOG(FATAL) << "not yet implemented"; return std::vector<std::string>();};

///////////////////////////////////////////////////////////////////////////////
// StringTableReader
///////////////////////////////////////////////////////////////////////////////

StringTableReader::StringTableReader() :
  is_open_(false),
  is_sorted_(false),
  has_bloom_filters_(false),
  num_entries_(0),
  size_(0),
  cur_reader_index_(-1),
  active_ordered_reader_(NULL){
}

StringTableReader::~StringTableReader(){
  if (is_open_){
    Close();
  }
}


bool StringTableReader::OpenMerge(const std::vector<std::string>& uris){
  CHECK(!is_open_);

  std::vector<std::string> shard_uris;
  BOOST_FOREACH(const std::string& uri, uris){
    std::string scheme, path;
    if (!pert::ParseUri(uri, &scheme, &path)){
      LOG(ERROR) << "uri is missing schema prefix <scheme>:// - provided uri: " << uri;
      return false;
    }

    // find matching shard filenames
    std::vector<std::string> cur_shard_uris = pert::GetShardUris(uri);
    CHECK(!cur_shard_uris.empty()) << "failed to find part files here: " << path;

    bool is_shard_set = (cur_shard_uris.size() > 1);

    if (is_shard_set && !ShardSetIsValid(cur_shard_uris)){
      LOG(INFO) << "This uri does not contain a valid shard set: " << uri;
      return false;
    }
    shard_uris.insert(shard_uris.end(), cur_shard_uris.begin(), cur_shard_uris.end());
  }

  num_shards_ = shard_uris.size();
  partitioner_.reset(new pert::ModKeyPartitioner(num_shards_));

  // open each file
  is_sorted_ = true;
  has_bloom_filters_ = true;
  num_entries_ = 0;
  size_ = 0;
  avg_compression_ratio_ = 0;
  min_block_size_ = kuint64max;
  max_block_size_ = 0;

  BOOST_FOREACH(std::string shard_uri, shard_uris){
    StringTableShardReader* new_reader = new StringTableShardReader();
    if (!new_reader->Open(shard_uri)){
      LOG(ERROR) << "Failed to open shard uri: " << shard_uri;
      return false;
    }
    num_entries_ += new_reader->Entries();
    size_ += new_reader->Size();
    shard_readers_.push_back(new_reader);

    is_sorted_ = is_sorted_ & new_reader->IsSorted();
    avg_compression_ratio_ += new_reader->CompressionRatio();
    min_block_size_ = std::min(min_block_size_, new_reader->MinBlockSize());
    max_block_size_ = std::max(max_block_size_, new_reader->MaxBlockSize());

    has_bloom_filters_ = has_bloom_filters_ & new_reader->HasBloomFilter();
  }

  // validate preconditions for sorted shards
  if (is_sorted_){
    comparator_name_ = shard_readers_[0]->ComparatorName();
    comparator_.reset(pert::CreateComparator(comparator_name_));
    CHECK_NOTNULL(comparator_.get());
    BOOST_FOREACH(StringTableShardReader* reader, shard_readers_){
      CHECK_EQ(reader->ComparatorName(), comparator_name_) << "all shards must use same comparator";
    }
    shard_queue_.reset(new ShardReaderQueue(CompareShardReaderPtrs(comparator_.get())));
  }

  avg_compression_ratio_ = avg_compression_ratio_ / shard_uris.size();
  is_open_ = true;
  SeekToStart();
  return true;
}


bool StringTableReader::Open(const std::string& uri){
  std::vector<std::string> uris;
  uris.push_back(uri);
  return OpenMerge(uris);
}

uint64 StringTableReader::Size(){
  return size_;
}

bool StringTableReader::IsOpen(){
  return is_open_;
}

uint64 StringTableReader::Entries(){
  CHECK(is_open_);
  return num_entries_;
}

bool StringTableReader::IsSorted(){
  CHECK(is_open_);
  return is_sorted_;
}

double StringTableReader::CompressionRatio(){
  CHECK(is_open_);
  return avg_compression_ratio_;
}


void StringTableReader::FlushQueue(){
  CHECK_NOTNULL(shard_queue_.get());
  // flush the existing queue state
  while (!shard_queue_->empty()){
    OrderedShardReader* shard_reader = shard_queue_->top();
    CHECK_NOTNULL(shard_reader);
    shard_queue_->pop();
    delete shard_reader;
  }

  active_ordered_reader_ = NULL;
}

void StringTableReader::FillQueue(){
  CHECK_NOTNULL(shard_queue_.get());
  // refill the queue with readers set at start
  BOOST_FOREACH(StringTableShardReader* reader, shard_readers_){
    OrderedShardReader* ordered_reader = new OrderedShardReader(reader);
    CHECK_NOTNULL(ordered_reader);
    if (ordered_reader->Next()){ // if reader has something, add it
      shard_queue_->push(ordered_reader);
    }
    else{
      delete ordered_reader;
    }
  }
}

void StringTableReader::SeekToStart(){
  CHECK(is_open_);
  CHECK(shard_readers_.size());
  // reset all readers to start
  BOOST_FOREACH(StringTableShardReader* reader, shard_readers_){
    reader->SeekToStart();
  }

  if (is_sorted_){
    FlushQueue();
    FillQueue();
  }
  else {
    cur_reader_index_ = 0;
  }
}

bool StringTableReader::Next(std::string* key, std::string* value){
  CHECK(is_open_);
  CHECK(key);
  CHECK(value);

  const char* key_buf = NULL;
  uint64 key_buf_len = 0;
  const char* value_buf = NULL;
  uint64 value_buf_len = 0;

  bool success = NextNoCopy(&key_buf, &key_buf_len, &value_buf, &value_buf_len);

  if (success){
    key->assign(key_buf, key_buf_len);
    value->assign(value_buf, value_buf_len);
  }

  return success;
}


bool StringTableReader::NextNoCopy(const char** key_buf, uint64* key_buf_len, const char** value_buf, uint64* value_buf_len){
  CHECK(is_open_);

  if (is_sorted_){
    // if there was an active reader, we need to update and return it to queue
    // this must be done here because of the no-copy implementation (can't call next on coded stream that has yet to be used)
    if (active_ordered_reader_){
      // try to advance it and add it back to queue if not empty
      if (active_ordered_reader_->Next()){
        shard_queue_->push(active_ordered_reader_);
      }
      // otherwise, we are done with it
      else{
        delete active_ordered_reader_;
      }
    }

    if (shard_queue_->empty()){
      return false;
    }

    active_ordered_reader_ = shard_queue_->top();
    shard_queue_->pop();
    active_ordered_reader_->Get(key_buf, key_buf_len, value_buf, value_buf_len);
  }
  else{
    while(!shard_readers_[cur_reader_index_]->NextNoCopy(key_buf, key_buf_len, value_buf, value_buf_len )){
      if (cur_reader_index_ < shard_readers_.size()-1){
        cur_reader_index_++;
      }
      else{
        return false;
      }
    }
  }

  return true;
}

bool StringTableReader::Find(const std::string& key, std::string* value){
  bool hit_disk;
  return FindVerbose(key, value, &hit_disk);
}


bool StringTableReader::FindVerbose(const std::string& key, std::string* value, bool* hit_disk){
  CHECK(is_open_) << "Must call Open() before Find().";
  CHECK(is_sorted_) << "File must be sorted to use Find() method";

  // Compute shard that should contain key
  uint32 shard = partitioner_->Partition(key);
  //LOG(INFO) << "shard: " << shard << " of " << num_shards_;
  return shard_readers_[shard]->Find(key, value);
}

bool StringTableReader::FindNaive(const std::string& key, std::string* value){
  SeekToStart();
  std::string found_key;
  NextGreaterEqual(key, &found_key, value);
  return key == found_key;
}


// Advances reader to the first key, value pair with key greater or equal to the requested key.  Returns True if the returned key equals requested key.
bool StringTableReader::SeekTo(const std::string& key, std::string* value){
  // TODO(kheath): Seeking back to start makes implementation simple, but inefficient for seeking to keys in sorted order.  If this is a hotspot, consider checking if we need to seek to start or not.
  // TODO(kheath): Add test case!
  SeekToStart();
  std::string found_key;
  NextGreaterEqual(key, &found_key, value);
  return key == found_key;
}


bool StringTableReader::NextGreaterEqualNoCopy(const std::string& query_key,
      const char** key_buf, uint64* key_buf_len, const char** value_buf,
      uint64* value_buf_len){

  CHECK(is_sorted_) << "this method can only be used with sorted tables";
  CHECK(shard_queue_);

  bool found_valid_key = false;
  while (!shard_queue_->empty() && !found_valid_key){
    OrderedShardReader* reader = shard_queue_->top();
    shard_queue_->pop();
    reader->Get(key_buf, key_buf_len, value_buf, value_buf_len);
    int cmp_result = comparator_->CompareKey(*key_buf, *key_buf_len, query_key.data(), query_key.size());
    // if current smallest entry is greater than or equal to query, we have already copied desired values to output

    if (cmp_result >= 0) {
      found_valid_key = true;
    }

    // re-add the reader if it has any others greater or equal to query for next call
    if (reader->NextGreaterEqual(query_key)){
      shard_queue_->push(reader);
    }
    else{
      delete reader;
    }
  }

  return found_valid_key;
}

bool StringTableReader::NextGreaterEqual(const std::string& query_key, std::string* key, std::string* value){
  const char* key_buf;
  uint64 key_buf_len;
  const char* value_buf;
  uint64 value_buf_len;
  bool result = NextGreaterEqualNoCopy(query_key, &key_buf, &key_buf_len, &value_buf, &value_buf_len);

  key->assign(key_buf, key_buf_len);
  value->assign(value_buf, value_buf_len);

  return result;
}




void StringTableReader::Close(){
  CHECK(is_open_);

  if (is_sorted_){
    FlushQueue();
  }

  BOOST_FOREACH(StringTableShardReader* reader, shard_readers_){
    delete reader;
  }
  shard_readers_.clear();

  is_open_ = false;
}

bool StringTableReader::HasMetadata(const std::string& name) const{
  CHECK(is_open_);
  CHECK(shard_readers_.size());
  return shard_readers_[0]->HasMetadata(name);
}

bool StringTableReader::GetMetadata(const std::string& name, std::string* data) const{
  CHECK(is_open_);
  CHECK(shard_readers_.size());
  return shard_readers_[0]->GetMetadata(name, data);
}

std::vector<std::string> StringTableReader::ListMetadataNames() const{
  CHECK(is_open_);
  CHECK(shard_readers_.size());
  return shard_readers_[0]->ListMetadataNames();
}


std::string StringTableReader::ShardInfo(){
  std::string info;
  info += StringPrintf("num shards: %d \n", shard_readers_.size());
  int index = 0;
  BOOST_FOREACH(StringTableShardReader* reader, shard_readers_){
    info += StringPrintf("shard %d has %d blocks\n", index, reader->Blocks());
    info += reader->GetBlockTableInfo();
    info += "\n";
    index++;
  }
  return info;
}

uint64 StringTableReader::MinBlockSize(){
  CHECK(is_open_);
  return min_block_size_;
}

uint64 StringTableReader::MaxBlockSize(){
  CHECK(is_open_);
  return max_block_size_;
}

///////////////////////////////////////////////////////////////////////////////
// ShardedStringTableWriter
///////////////////////////////////////////////////////////////////////////////
StringTableWriter::StringTableWriter() :  num_shards_(0), is_open_(false) {

}

StringTableWriter::~StringTableWriter(){
  Close();
}

bool StringTableWriter::Open(const std::string& uri, int num_shards, WriterOptions options){
  CHECK(!is_open_);

  num_shards_ = num_shards;
  partitioner_.reset(new pert::ModKeyPartitioner(num_shards));

  std::string scheme, path;
  if (!pert::ParseUri(uri, &scheme, &path)){
    LOG(ERROR) << "uri is missing schema prefix <scheme>:// - provided uri: " << uri;
    return false;
  }

  pert::Remove(uri);

  // Open each file
  for(int i = 0; i < num_shards_; ++i){
    StringTableShardWriter* new_writer = new StringTableShardWriter();
    std::string shard_uri = StringPrintf("%s/part-%05d", uri.c_str(), i);

    if (!new_writer->Open(shard_uri, options)){
      LOG(ERROR) << "Failed to open output shard uri: " << shard_uri;
      return false;
    }

    shard_writers_.push_back(new_writer);
  }

  is_open_ = true;
  return true;
}

void StringTableWriter::Close(){
  //LOG(INFO) << "StringTableWriter::Close()";
  if(!is_open_){
    return;
  }

  BOOST_FOREACH(StringTableShardWriter* writer, shard_writers_){
    writer->Close();
    delete writer;
  }
  shard_writers_.clear();

  is_open_ = false;
}

bool StringTableWriter::Add(const std::string& key, const std::string& value){
  uint32 shard = partitioner_->Partition(key);
  return shard_writers_[shard]->Add(key, value);
}

void StringTableWriter::AddMetadata(const std::string& name, const std::string& data){
  for (int shard=0; shard < num_shards_; ++shard){
    shard_writers_[shard]->AddMetadata(name, data);
  }
}


} // close namespace
