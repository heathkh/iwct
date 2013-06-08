#pragma once

#include "snap/pert/core/md5.h"
#include "snap/pert/core/common.h"
#include "snap/pert/core/common.h"
#include "snap/pert/core/comparator.h"
#include "snap/pert/core/partitioner.h"
#include "snap/pert/core/compression.h"
#include "snap/pert/core/ufs.h"
#include "snap/pert/core/cbfile.h"
#include "snap/pert/pert.pb.h"
#include "snap/pert/core/bloom.h"
#include "snap/boost/scoped_ptr.hpp"
#include "snap/google/glog/logging.h"
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <string>
#include <queue>


namespace pert {

std::string BytesToHexString(const std::string& in);
std::vector<std::string> GetShardUris(const std::string& uri);
int GetNumShards(const std::string& uri);
bool ShardSetIsValid(const std::vector<std::string>& shard_uris);

bool GetShardSetFingerprint(const std::string& uri, std::string* hex_fingerprint);

///////////////////////////////////////////////////////////////////////////////
// WriterOptions
///////////////////////////////////////////////////////////////////////////////
class WriterOptions {
public:

  WriterOptions() {
    SetBlockSize(10 * (1 << 20)); // default block size 10 MB
    SetSorted();
    SetCompressed();
    SetNoBloomFilter();
  }

  void SetUnsorted(){
    comparator_name_ = "";
  }

  void SetSorted(const std::string comparator_name = "memcmp"){
    CHECK(pert::ValidComparatorName(comparator_name))
    << "not valid sort comparator: " << comparator_name;
    comparator_name_ = comparator_name;
  }

  void SetUncompressed() {
    compression_codec_name_ = "none";
  }

  void SetCompressed(std::string compression_codec_name = "snappy") {
    CHECK(pert::ValidCompressionCodecName(compression_codec_name))
    << "not valid compression_codec_name: " << compression_codec_name;
    compression_codec_name_ = compression_codec_name;
  }

  void SetBlockSize(uint64 size) {
    CHECK(size < 512*(1<<20)) << "max block size is 512 MB"; // coded streams have limited size to prevent abuse
    desired_uncompressed_data_block_size_ = size;
  }

  void SetNoBloomFilter(){
    create_bloom_filter_ = false;
  }

  void SetBloomFilterPredictBitsPerShard(uint64 max_entries_per_shard, double desired_error_rate = 0.001, double inefficiency_factor = 10.0){
    create_bloom_filter_ = true;
    CHECK_GT(desired_error_rate, 0.0);
    CHECK_LT(desired_error_rate, 0.5) << "you are probably not using bloom filters correctly.";
    uint64 ideal_num_bits = PredictRequiredBits(max_entries_per_shard, desired_error_rate);
    bloom_filter_num_bits_ = ideal_num_bits * inefficiency_factor;
  }

  void SetBloomFilterBitsPerShard(uint64 num_bits_per_shard){
    create_bloom_filter_ = true;
    bloom_filter_num_bits_ = num_bits_per_shard;
  }

private:
  friend class StringTableShardWriter;
  std::string comparator_name_;
  std::string compression_codec_name_;
  uint64 desired_uncompressed_data_block_size_;
  bool create_bloom_filter_;
  uint64 bloom_filter_num_bits_;
};

/** Normally splits have many entries for efficient indexing.  Use this config
 * when you need a split to contain a single entry.
 */
WriterOptions TinyWriter();

///////////////////////////////////////////////////////////////////////////////
// StringTableShardWriter
///////////////////////////////////////////////////////////////////////////////
class StringTableShardWriter {
public:
  StringTableShardWriter();
  virtual ~StringTableShardWriter();

  virtual bool Open(const std::string& uri, WriterOptions options = WriterOptions());
  void Close();
  bool Add(const std::string& key, const std::string& value);
  void AddMetadata(const std::string& name, const std::string& data);

protected:
  void ResetDataBlockBuffers();
  void FlushCurrentDataBlock();
  void WriteMetadataBlock(const std::string& name, const std::string& data);

  WriterOptions options_;
  MetadataIndex metadata_index_;
  typedef std::map<std::string, std::string> MetadataMap;
  MetadataMap metadata_;

  bool is_open_;
  uint32 desired_uncompressed_data_block_size_;
  boost::scoped_ptr<pert::OutputCodedBlockFile> output_;

  boost::scoped_ptr<pert::CompressionCodec> compression_codec_;
  std::string uncompressed_data_;
  google::protobuf::io::StringOutputStream* uncompressed_data_stream_;
  google::protobuf::io::CodedOutputStream* coded_uncompressed_data_stream_;
  std::string compressed_data_;

  DataIndex data_index_;

  std::string first_key_;
  std::string last_key_;

  bool is_sorted_;
  std::string comparator_name_;
  boost::scoped_ptr<pert::KeyComparator> comparator_;

  uint64 num_entries_;
  uint64 cur_block_initial_entry_index_;
  std::string compression_codec_name_;

  uint64 data_compressed_bytes_;
  uint64 data_uncompressed_bytes_;

  boost::scoped_ptr<pert::KeyBloomFilterGenerator> bloom_filter_generator_;

  uint32 num_opens_;

  Md5Digest md5_digest_;

  DISALLOW_COPY_AND_ASSIGN(StringTableShardWriter);
};

///////////////////////////////////////////////////////////////////////////////
// StringTableShardReader
///////////////////////////////////////////////////////////////////////////////
class StringTableShardReader {
public:
  StringTableShardReader();
  virtual ~StringTableShardReader();

  static bool GetShardInfo(const std::string& uri, pert::FileInfo* info );

  virtual bool Open(const std::string& uri);
  virtual bool OpenSplit(const std::string& uri, uint64 split_start,
      uint64 split_end);
  virtual void Close();
  bool IsSorted() {
    return is_sorted_;
  }

  // API available for any file

  uint64 SplitEntries() {
    return entries_in_split_;
  } // Number of entries in the requested split
  uint64 CurEntry() {
    return cur_entry_index_;
  } // Index of current entry (relative to all entries in file)
  uint64 Entries(); // Total number of entries in the file (ignoring the split restriction)
  uint64 Blocks() {
    return data_index_.blocks_size();
  }

  // Requesting indices in ascending order is much more efficient.
  bool GetIndexNoCopy(uint64 index, const char** key_buf, uint64* key_buf_len, const char** value_buf, uint64* value_buf_len);
  bool GetIndex(uint64 index, std::string* key, std::string* value);

  void SeekToStart(); // moves the cursor back to the first key in the scan range
  virtual bool Next(std::string* key, std::string* value); // returns false if at end, otherwise returns true and advances the cursor
  virtual bool NextNoCopy(const char** key_buf, uint64* key_buf_len,
      const char** value_buf, uint64* value_buf_len);

  // Methods available only if file is sorted

  bool Find(const std::string& key, std::string* value);
  bool FindVerbose(const std::string& key, std::string* value, bool* hit_disk);

  //bool SeekTo(const std::string& key, std::string* value); // moves the cursor to the first key greater than or equal to the input key, returns true if the key equals the input key
  virtual bool NextGreaterEqualNoCopy(const std::string& query_key,
      const char** key_buf, uint64* key_buf_len, const char** value_buf,
      uint64* value_buf_len);
  virtual bool NextGreaterEqual(const std::string& query_key, std::string* key,
      std::string* value);

  bool HasMetadata(const std::string& name) const;
  bool GetMetadata(const std::string& name, std::string* data) const;
  std::vector<std::string> ListMetadataNames() const;

  float SplitProgress(); // progress through the split range (0 to 1)

  uint64 Size(); // total size of the file (ignoring the requested split range)

  double CompressionRatio();
  std::string CompressionCodecName();
  std::string ComparatorName();

  uint64 MinBlockSize();
  uint64 MaxBlockSize();
  std::string GetBlockTableInfo();

  bool HasBloomFilter() { return bloom_filter_.get() != NULL; }

private:
  bool LoadDataBlock(int block_id);
  bool LoadNextDataBlock();
  bool LoadMetadataBlock(const MetadataIndexRecord* metadata_block_record,
      std::string* data) const;

  int DataBlockStartingBeforePosition(uint64 pos);
  int DataBlockEndingBeforePosition(uint64 pos);

  bool is_open_;
  boost::scoped_ptr<pert::InputCodedBlockFile> input_;

  std::string uri_;
  FileInfo file_info_;
  DataIndex data_index_;
  MetadataIndex metadata_index_;

  uint32 min_block_id_;
  uint32 max_block_id_;
  uint32 cur_block_id_;

  uint64 min_entry_index_;
  uint64 max_entry_index_;
  uint64 entries_in_split_;

  bool is_sorted_;
  boost::scoped_ptr<pert::KeyComparator> comparator_;
  boost::scoped_ptr<pert::CompressionCodec> compression_codec_;

  std::string decompressed_data_block_;
  uint64 cur_entry_index_;

  bool cur_block_has_more_data_;
  boost::scoped_ptr<google::protobuf::io::CodedInputStream> data_block_coded_stream_;

  boost::scoped_ptr<pert::KeyBloomFilter> bloom_filter_;

  friend class StringTableReader;
  DISALLOW_COPY_AND_ASSIGN(StringTableShardReader);
};


///////////////////////////////////////////////////////////////////////////////
// StringTableReaderBase
///////////////////////////////////////////////////////////////////////////////

// Abstract API for all Readers
class StringTableReaderBase {
public:
  StringTableReaderBase() {}
  virtual ~StringTableReaderBase() {}

  virtual bool Open(const std::string& uris) = 0;
  virtual uint64 Entries() = 0;

  virtual void SeekToStart() = 0;
  virtual void Close() = 0;
  virtual bool Next(std::string* key, std::string* value) = 0;
  virtual bool NextNoCopy(const char** key_buf, uint64* key_buf_len,
                          const char** value_buf, uint64* value_buf_len) = 0;


  virtual bool HasMetadata(const std::string& name) const = 0;
  virtual bool GetMetadata(const std::string& name, std::string* data) const = 0;
  virtual std::vector<std::string> ListMetadataNames() const = 0;
};

///////////////////////////////////////////////////////////////////////////////
// StringTableShardSetReader
///////////////////////////////////////////////////////////////////////////////


// Opening a StringTableReader when there are 1000 of shards is not ideal because
// it loads a block of each shard into ram which can exhaust ram.  If you can
// settle for a restricted API where you only need to iterate over all the values
// and don't care about sorted ordering, this class can do it by just opening
// one shard at a time.

// TODO(kheath): consider a better name (or should this be combined with the StringTableReader() api?)
class StringTableShardSetReader : public StringTableReaderBase {
public:
  StringTableShardSetReader();
  virtual ~StringTableShardSetReader();

  bool Open(const std::string& uri);
  bool Open(const std::vector<std::string>& uris);

  //uint64 Size();
  uint64 Entries();

  void SeekToStart();

  // Requesting indices in ascending order is much more efficient
  bool GetIndex(uint64 index, std::string* key, std::string* value);

  virtual void Close();
  virtual bool Next(std::string* key, std::string* value); // returns false if at end, otherwise returns true and advances the cursor

  template <class ProtoMessageType>
  bool NextProto(std::string* key, ProtoMessageType* proto) {
    CHECK(proto);
    const char* key_buf;
    uint64 key_buf_len;
    const char* value_buf;
    uint64 value_buf_len;

    bool success = NextNoCopy(&key_buf, &key_buf_len, &value_buf, &value_buf_len);
    if (success){
      if (key){
        key->assign(key_buf, key_buf_len);
      }
      CHECK(proto->ParseFromArray(value_buf, value_buf_len));
    }
    return success;
  }

  virtual bool NextNoCopy(const char** key_buf, uint64* key_buf_len,
      const char** value_buf, uint64* value_buf_len);

  virtual bool HasMetadata(const std::string& name) const;
  virtual bool GetMetadata(const std::string& name, std::string* data) const;
  virtual std::vector<std::string> ListMetadataNames() const;

private:
  bool is_open_;
  uint32 cur_uri_index_;
  std::vector<std::string> uris_;
  boost::scoped_ptr<StringTableShardReader> cur_reader_;
  DISALLOW_COPY_AND_ASSIGN(StringTableShardSetReader);

  uint64 num_entries_;
  std::vector<uint64> shard_last_index_;
};



///////////////////////////////////////////////////////////////////////////////
// StringTableReader
///////////////////////////////////////////////////////////////////////////////
// Opens a set of sharded pert file as if it were a single file
class StringTableReader : public StringTableReaderBase {
public:
  StringTableReader();
  virtual ~StringTableReader();

  bool OpenMerge(const std::vector<std::string>& uris);
  bool Open(const std::string& uri);
  bool IsOpen();
  uint64 Size();
  uint64 Entries();
  bool IsSorted();

  double CompressionRatio();
  void SeekToStart(); // moves the cursor back to the first key in the scan range
  bool Next(std::string* key, std::string* value); // returns false if at end, otherwise returns true and advances the cursor
  bool NextNoCopy(const char** key_buf, uint64* key_buf_len, const char** value_buf, uint64* value_buf_len);
  void Close();


  // Methods available only if file is sorted

  // Advances the reader to the first key greater than or equal to the input key, returns true if the key equals the input key.
  bool SeekTo(const std::string& key, std::string* value);

  // Finds a key, value pair with requested key or return false.  If there are more than one such keys, we only get one.
  bool Find(const std::string& key, std::string* value);
  bool FindVerbose(const std::string& key, std::string* value, bool* hit_disk);
  bool FindNaive(const std::string& key, std::string* value); // for debug testing

  virtual bool NextGreaterEqualNoCopy(const std::string& query_key,
      const char** key_buf, uint64* key_buf_len, const char** value_buf,
      uint64* value_buf_len);
  virtual bool NextGreaterEqual(const std::string& query_key, std::string* key,
      std::string* value);

  bool HasMetadata(const std::string& name) const;
  bool GetMetadata(const std::string& name, std::string* data) const;
  std::vector<std::string> ListMetadataNames() const;

  std::string ShardInfo();

  uint64 MinBlockSize();
  uint64 MaxBlockSize();

  bool HasBloomFilter() { return has_bloom_filters_; }

private:

  void FlushQueue();
  void FillQueue();

  template<class> friend class ProtoTableReader;
  bool is_open_;
  bool is_sorted_;
  bool has_bloom_filters_;

  uint64 num_entries_;
  uint64 size_; // total size of all shards
  double avg_compression_ratio_;
  std::vector<StringTableShardReader*> shard_readers_;
  int cur_reader_index_;
  uint64 min_block_size_;
  uint64 max_block_size_;
  uint32 num_shards_;

  // Determine priority.
  class OrderedShardReader;
  class CompareShardReaderPtrs {
  public:
    CompareShardReaderPtrs(pert::KeyComparator* comparator) : comparator_(comparator) { }
    bool operator()(const OrderedShardReader* a, const OrderedShardReader* b)  {
      return (comparator_->CompareKey(a->key_buf_, a->key_buf_len_, b->key_buf_, b->key_buf_len_) > 0); // ie A > B
    }
  private:
    pert::KeyComparator* comparator_;
  };

  class OrderedShardReader {
  public:

    OrderedShardReader(StringTableShardReader* reader) :
      key_buf_(NULL), key_buf_len_(0), value_buf_(NULL), value_buf_len_(0),
      reader_(reader) {

    }

    bool Next(){
      return reader_->NextNoCopy(&key_buf_, &key_buf_len_, &value_buf_, &value_buf_len_);
    }

    bool NextGreaterEqual(const std::string& query){
      return reader_->NextGreaterEqualNoCopy(query, &key_buf_, &key_buf_len_, &value_buf_, &value_buf_len_);
    }

    void Get(const char** key_buf, uint64* key_buf_len, const char** value_buf, uint64* value_buf_len) const {
      *key_buf = key_buf_;
      *key_buf_len = key_buf_len_;
      *value_buf = value_buf_;
      *value_buf_len = value_buf_len_;
    }

  private:
    const char* key_buf_;
    uint64 key_buf_len_;
    const char* value_buf_;
    uint64 value_buf_len_;
    StringTableShardReader* reader_;
    friend class StringTableReader::CompareShardReaderPtrs;
    friend class StringTableReader;

    DISALLOW_COPY_AND_ASSIGN(OrderedShardReader);
  };

   typedef std::priority_queue<OrderedShardReader*, std::vector<OrderedShardReader*>, CompareShardReaderPtrs> ShardReaderQueue;
   OrderedShardReader* active_ordered_reader_;
   boost::scoped_ptr<ShardReaderQueue> shard_queue_;
   std::string comparator_name_;
   boost::scoped_ptr<pert::KeyComparator> comparator_;
   boost::scoped_ptr<pert::ModKeyPartitioner> partitioner_;



   DISALLOW_COPY_AND_ASSIGN(StringTableReader);
};


///////////////////////////////////////////////////////////////////////////////
// StringTableWriter
///////////////////////////////////////////////////////////////////////////////
class StringTableWriter {
public:
  StringTableWriter();
  virtual ~StringTableWriter();

  virtual bool Open(const std::string& uri, int num_shards = 1, WriterOptions options = WriterOptions());
  void Close();
  bool Add(const std::string& key, const std::string& value);
  void AddMetadata(const std::string& name, const std::string& data);

protected:
  int num_shards_;
  bool is_open_;
  boost::scoped_ptr<pert::KeyPartitioner> partitioner_;
  std::vector<StringTableShardWriter*> shard_writers_;

private:
  DISALLOW_COPY_AND_ASSIGN(StringTableWriter);
};

} // close namespace

