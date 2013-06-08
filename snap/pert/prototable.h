#pragma once

#include "snap/pert/pert.pb.h"
#include "snap/pert/stringtable.h"
#include "snap/google/glog/logging.h"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/message.h>

namespace pert {


void CreateProtoFileTree(const google::protobuf::FileDescriptor* file, ProtoFileTree* file_tree);
void StoreProtoFormat(const google::protobuf::Message& proto, ProtoFormat* proto_format);

///////////////////////////////////////////////////////////////////////////////
// ProtoTableWriter
///////////////////////////////////////////////////////////////////////////////
template <typename ProtoMessageType>
class ProtoTableShardWriter : public StringTableShardWriter {
public:

  ProtoTableShardWriter() : StringTableShardWriter() {
  }

  virtual ~ProtoTableShardWriter(){
  }

  virtual bool Open(const std::string& uri, WriterOptions options = WriterOptions()){
    StringTableShardWriter::Open(uri, options);

    ProtoFormat proto_format;
    StoreProtoFormat(ProtoMessageType(), &proto_format);
    AddMetadata(PERT_PROTO_FORMAT_METADATA_NAME, proto_format.SerializeAsString());
    return true;
  }

  bool Add(const std::string& key, const ProtoMessageType& proto) {
    return StringTableShardWriter::Add(key, proto.SerializeAsString());
  }

  bool Add(const std::string& key, const std::string& value) {
      return StringTableShardWriter::Add(key, value);
    }

protected:


};


///////////////////////////////////////////////////////////////////////////////
// ProtoTableWriter
///////////////////////////////////////////////////////////////////////////////
template <typename ProtoMessageType>
class ProtoTableWriter : public StringTableWriter {
public:

  ProtoTableWriter() {
  }

  virtual ~ProtoTableWriter(){
  }

  virtual bool Open(const std::string& uri, int num_shards = 1, WriterOptions options = WriterOptions()){
    StringTableWriter::Open(uri, num_shards, options);

    ProtoFormat proto_format;
    StoreProtoFormat(ProtoMessageType(), &proto_format);
    AddMetadata(PERT_PROTO_FORMAT_METADATA_NAME, proto_format.SerializeAsString());
    return true;
  }

  bool Add(const std::string& key, const ProtoMessageType& proto) {
    return StringTableWriter::Add(key, proto.SerializeAsString());
  }

  using StringTableWriter::Add; // need to explicitly bring the hidden Add from the base class... (http://www.cplusplus.com/forum/general/35681/)

private:
  DISALLOW_COPY_AND_ASSIGN(ProtoTableWriter);
};


/*
///////////////////////////////////////////////////////////////////////////////
// PythonShardedProtoTableWriter
///////////////////////////////////////////////////////////////////////////////
// helper class for nicer swig binding
class PythonShardedProtoTableWriter : public StringTableWriter {
public:

  PythonShardedProtoTableWriter() {
  }

  virtual ~PythonShardedProtoTableWriter(){
  }

  virtual bool Open(const std::string& serialized_proto_format, const std::string& uri, int num_shards, WriterOptions options = WriterOptions()){
    CHECK(StringTableWriter::Open(uri, num_shards, options));
    AddMetadata(PERT_PROTO_FORMAT_METADATA_NAME, serialized_proto_format);
    CHECK(is_open_);
    return true;
  }

};
*/

///////////////////////////////////////////////////////////////////////////////
// ProtoTableShardReader
///////////////////////////////////////////////////////////////////////////////

template <typename ProtoMessageType>
class ProtoTableShardReader : public StringTableShardReader {
public:
  ProtoTableShardReader() {
  }

  virtual ~ProtoTableShardReader(){
  }

  virtual bool Open(const std::string& uri)  {
    if (!StringTableShardReader::Open(uri)){
      return false;
    }

    // Verify this is actually a ProtoTable file by checking for the format metadata.
    std::string value;
    if (!GetMetadata(PERT_PROTO_FORMAT_METADATA_NAME, &value)){
      LOG(ERROR) << "This is not ProtoTable file.  Try StringTableReader instead.";
      return false;
    }

    // Check that proto format metadata is valid.
    ProtoFormat proto_format;
    if (!proto_format.ParseFromString(value)){
      LOG(ERROR) << "Proto format metadata is corrupt.";
      return false;
    }

    return true;
  }

  virtual bool NextProto(std::string* key, ProtoMessageType* proto) {
    CHECK(proto);
    const char* key_buf;
    uint64 key_buf_len;
    const char* value_buf;
    uint64 value_buf_len;

    bool success = NextNoCopy(&key_buf, &key_buf_len, &value_buf, &value_buf_len);
    if (success){
      key->assign(key_buf, key_buf_len);
      CHECK(proto->ParseFromArray(value_buf, value_buf_len));
    }
    return success;
  }

};


///////////////////////////////////////////////////////////////////////////////
// ProtoTableReader
///////////////////////////////////////////////////////////////////////////////

template <typename ProtoMessageType>
class ProtoTableReader : public StringTableReader {
public:
  ProtoTableReader() {
  }

  virtual ~ProtoTableReader(){
  }

  virtual bool Open(const std::string& uri)  {
    if (!StringTableReader::Open(uri)){
      return false;
    }

    // Verify this is actually a ProtoTable file by checking for the format metadata.
    std::string value;
    if (!GetMetadata(PERT_PROTO_FORMAT_METADATA_NAME, &value)){
      LOG(ERROR) << "This is not ProtoTable file.  Try StringTableReader instead.";
      return false;
    }

    // Check that proto format metadata is valid.
    ProtoFormat proto_format;
    if (!proto_format.ParseFromString(value)){
      LOG(ERROR) << "Proto format metadata is corrupt.";
      return false;
    }

    return true;
  }

  virtual bool NextProto(std::string* key, ProtoMessageType* proto) {
    CHECK(proto);
    const char* key_buf;
    uint64 key_buf_len;
    const char* value_buf;
    uint64 value_buf_len;

    bool success = NextNoCopy(&key_buf, &key_buf_len, &value_buf, &value_buf_len);
    if (success){
      key->assign(key_buf, key_buf_len);
      CHECK(proto->ParseFromArray(value_buf, value_buf_len));
    }
    return success;
  }

};

///////////////////////////////////////////////////////////////////////////////
// ProtoTablePrettyPrinter
///////////////////////////////////////////////////////////////////////////////
class ProtoTablePrettyPrinter {
public:
  ProtoTablePrettyPrinter();
  bool Init(const StringTableShardReader& table);
  bool Init(const StringTableReader& table);
  bool Init(const std::string& serialized_proto_format);
  std::string ProtoDescription();
  std::string Proto(const std::string& value);
  std::string ProtoFields(const std::string& value, const std::vector<std::string>& field_names);

private:

  bool initialized_;
  const google::protobuf::Descriptor* desc_;
  google::protobuf::Message* dynamic_message_;
  const google::protobuf::Message* dynamic_message_prototype_;
  boost::scoped_ptr<google::protobuf::DynamicMessageFactory> dynamic_message_factory_;
  google::protobuf::DescriptorPool descriptor_pool_;
};

// Convenience routines for proto persistence with pert format

template<class Proto>
void SaveProtoOrDie(const Proto& proto, const std::string& uri){
  pert::ProtoTableWriter<Proto> writer;
  CHECK(writer.Open(uri));
  writer.Add("", proto);
  writer.Close();
}

template<class Proto>
void LoadProtoOrDie(const std::string& uri, Proto* proto){
  CHECK(proto);
  pert::ProtoTableReader<Proto> reader;
  CHECK(reader.Open(uri));
  CHECK_EQ(reader.Entries(), 1);
  std::string key;
  reader.NextProto(&key, proto);
}

} // close namespace

