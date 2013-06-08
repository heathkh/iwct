#include "snap/pert/prototable.h"
#include "snap/google/base/stringprintf.h"

namespace pert {


void CreateProtoFileTree(const google::protobuf::FileDescriptor* file, ProtoFileTree* file_tree){
    // store this file
    file->CopyTo(file_tree->mutable_file());

    // store this file's dependencies
    for (int i=0; i < file->dependency_count(); ++i){
      const google::protobuf::FileDescriptor* dep_file = file->dependency(i);
      CreateProtoFileTree(dep_file, file_tree->add_deps());
    }
  }

void StoreProtoFormat(const google::protobuf::Message& proto, ProtoFormat* proto_format) {
  CHECK(proto_format);
  proto_format->Clear();
  proto_format->set_name(proto.GetDescriptor()->name());
  CreateProtoFileTree(proto.GetDescriptor()->file(),
                      proto_format->mutable_file_tree());
}

const google::protobuf::FileDescriptor* LoadProtoFileTree(const ProtoFileTree& proto_file_tree,
                       google::protobuf::DescriptorPool* descriptor_pool) {

  for (int i=0; i < proto_file_tree.deps_size(); ++i){
    LoadProtoFileTree(proto_file_tree.deps(i), descriptor_pool);
  }

  const google::protobuf::FileDescriptor* file_desc = descriptor_pool->BuildFile(proto_file_tree.file());
  return file_desc;
}


///////////////////////////////////////////////////////////////////////////////
// PertPrettyPrinter
///////////////////////////////////////////////////////////////////////////////

ProtoTablePrettyPrinter::ProtoTablePrettyPrinter() :
    initialized_(false),
    desc_(NULL),
    dynamic_message_(NULL),
    dynamic_message_factory_(new google::protobuf::DynamicMessageFactory())
{
}

bool ProtoTablePrettyPrinter::Init(const StringTableShardReader& table){
  std::string value;
  if (!table.GetMetadata(PERT_PROTO_FORMAT_METADATA_NAME, &value)){
    return false;
  }
  return Init(value);
}

bool ProtoTablePrettyPrinter::Init(const StringTableReader& table) {
  std::string value;
  if (!table.GetMetadata(PERT_PROTO_FORMAT_METADATA_NAME, &value)){
    return false;
  }
  return Init(value);
}



bool ProtoTablePrettyPrinter::Init(const std::string& serialized_proto_format){
  ProtoFormat proto_format;
  if (!proto_format.ParseFromString(serialized_proto_format)){
    return false;
  }

  using namespace google::protobuf;

  const FileDescriptor* file_desc = LoadProtoFileTree(proto_format.file_tree(), &descriptor_pool_);
  if (!file_desc){
    return false;
  }
  desc_ = file_desc->FindMessageTypeByName(proto_format.name());
  CHECK(desc_);

  // Memory managed by descriptor pool, so we shouldn't delete these
  dynamic_message_prototype_ = dynamic_message_factory_->GetPrototype(desc_);
  dynamic_message_ = dynamic_message_prototype_->New();

  initialized_ = true;
  return true;
}

std::string ProtoTablePrettyPrinter::Proto(const std::string& value){
  CHECK(initialized_) << "Must call Init() first.";
  CHECK(dynamic_message_->ParseFromString(value));
  return dynamic_message_->DebugString();
}

std::string ProtoTablePrettyPrinter::ProtoFields(const std::string& value, const std::vector<std::string>& field_names){
  CHECK(initialized_) << "Must call Init() first.";
  CHECK(dynamic_message_->ParseFromString(value));
  std::string output;
  for (int field_index=0; field_index < field_names.size(); ++field_index ){
    const google::protobuf::FieldDescriptor* field_desc = desc_->FindFieldByName(field_names[field_index]);
    CHECK(field_desc) << "can't find field with name " << field_names[field_index];

    using namespace google::protobuf;
    const Reflection* reflection = dynamic_message_->GetReflection();

    CHECK(!field_desc->is_repeated()) << "Printing of repeated fields not yet implemented... ";

    if (field_desc->type() == FieldDescriptor::TYPE_STRING){
      output += StringPrintf("%s : %s\n", field_desc->name().c_str(), reflection->GetString(*dynamic_message_, field_desc).c_str());
    }
    else if (field_desc->type() == FieldDescriptor::TYPE_UINT64){
      output += StringPrintf("%s : %llu\n", field_desc->name().c_str(), reflection->GetUInt64(*dynamic_message_, field_desc));
    }
    else if (field_desc->type() == FieldDescriptor::TYPE_UINT32){
      output += StringPrintf("%s : %d\n", field_desc->name().c_str(), reflection->GetUInt32(*dynamic_message_, field_desc));
    }
    else if (field_desc->type() == FieldDescriptor::TYPE_DOUBLE){
      output += StringPrintf("%s : %f\n", field_desc->name().c_str(), reflection->GetDouble(*dynamic_message_, field_desc));
    }
    else if (field_desc->type() == FieldDescriptor::TYPE_FLOAT){
      output += StringPrintf("%s : %f\n", field_desc->name().c_str(), reflection->GetFloat(*dynamic_message_, field_desc));
    }
    else if (field_desc->type() == FieldDescriptor::TYPE_BOOL ){
      output += StringPrintf("%s : %s\n", field_desc->name().c_str(), reflection->GetBool(*dynamic_message_, field_desc) ? "true" : "false");
    }
    else if (field_desc->type() == FieldDescriptor::TYPE_MESSAGE ){
      output += StringPrintf("%s : %s\n", field_desc->name().c_str(), reflection->GetMessage(*dynamic_message_, field_desc).ShortDebugString().c_str());
    }
    else{
      LOG(FATAL) << "unhandled field type: " << field_desc->type();  // If you get error here, you need to add a handler to list above...
    }

  }

  return output;
}

std::string ProtoTablePrettyPrinter::ProtoDescription(){
  CHECK(initialized_) << "Must call Init() first.";
  return desc_->DebugString();
}

} // close namespace
