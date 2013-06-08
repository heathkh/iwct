%module py_pert

%include snap/google/base/base.swig 
%include std_string.i
%include "std_vector.i"
%include exception.i
%include snap/google/base/base.swig
%apply std::string& INPUT {const std::string&};
%apply std::string* OUTPUT {std::string*};
%apply uint64* OUTPUT {uint64*};


%{
#include "snap/pert/stringtable.h"
%}

// This code is run on python module initalization... Want to turn on stack trace feature of glog!
%init %{
  google::InstallFailureSignalHandler();
%}

/// Magic to wrap vector of strings in a nice way
%{ 
#include <vector>
#include <string>
%} 
namespace std {
  %template(VectorOfString) vector<string>;
}

%apply std::vector<std::string>* OUTPUT {std::vector<std::string>*};

%include "snap/pert/stringtable.h"

// Make StringTableShardReader iterable in python
%exception pert::StringTableShardReader::next {
   try {
      $action
   } catch (char const* msg) {
      //PyErr_SetString(PyExc_IndexError, const_cast<char*>(msg));
      PyErr_SetNone(PyExc_StopIteration);
      return NULL;
   }
}

%extend pert::StringTableShardReader {
  pert::StringTableShardReader* __iter__() {
    return $self;
  }

  void next(std::string* key , std::string* value) {    
    bool ok = $self->Next(key, value);
    if (!ok) {
      throw "out of range";
    }        
  }
}


// Make StringTableReader iterable in python
%exception pert::StringTableReader::next {
   try {
      $action
   } catch (char const* msg) {
      //PyErr_SetString(PyExc_IndexError, const_cast<char*>(msg));
      PyErr_SetNone(PyExc_StopIteration);
      return NULL;
   }
}

%extend pert::StringTableReader {
  pert::StringTableReader* __iter__() {
    return $self;
  }

  void next(std::string* key , std::string* value) {    
    bool ok = $self->Next(key, value);
    if (!ok) {
      throw "out of range";
    }        
  }
}


// Make StringTableShardSetReader iterable in python
%exception pert::StringTableShardSetReader::next {
   try {
      $action
   } catch (char const* msg) {
      //PyErr_SetString(PyExc_IndexError, const_cast<char*>(msg));
      PyErr_SetNone(PyExc_StopIteration);
      return NULL;
   }
}

%extend pert::StringTableShardSetReader {
  pert::StringTableShardSetReader* __iter__() {
    return $self;
  }

  void next(std::string* key , std::string* value) {    
    bool ok = $self->Next(key, value);
    if (!ok) {
      throw "out of range";
    }        
  }
}

///////////////////////////////////////////////////////////////////////////////
// ProtoTableShardWriter
///////////////////////////////////////////////////////////////////////////////


namespace pert {
%feature("shadow") ProtoTableShardWriter::Open(const std::string& serialized_proto_format, const std::string& uri, WriterOptions options = WriterOptions()) %{
def Open(*args):
    
    # We intercept the SWIG binding and modify param 1 so that we have converted
    # a python protobuf into the serialized proto format string that can be 
    # passed to the C++      
    from pert import pert_pb2    
    def CreateProtoFileTree(desc, file_tree):
      # store this file            
      desc.file.CopyToProto(file_tree.file)  

      # store this file's dependencies         
      for name, field in desc.fields_by_name.iteritems():        
        if field.message_type:                    
          CreateProtoFileTree(field.message_type, file_tree.deps.add())
          
      return
      
    def GetSerializedFormat(proto):  
      proto_format = pert_pb2.ProtoFormat();        
      proto_format.name = proto.DESCRIPTOR.name            
      CreateProtoFileTree(proto.DESCRIPTOR, proto_format.file_tree);        
      return proto_format.SerializeToString()
    
    sf = GetSerializedFormat(args[1])
    
    # replace the second arg with the serialized proto format string
    args = (args[0], sf) + args[2:]    
    return $action(*args)    
%}

} // close namespace pert


%inline %{

namespace pert {

class ProtoTableShardWriter : public StringTableShardWriter {
public:
  ProtoTableShardWriter() {}
  virtual ~ProtoTableShardWriter(){}
  virtual bool Open(const std::string& serialized_proto_format, const std::string& uri, WriterOptions options = WriterOptions()){        
    CHECK(StringTableShardWriter::Open(uri, options));
    AddMetadata(PERT_PROTO_FORMAT_METADATA_NAME, serialized_proto_format);    
    CHECK(is_open_);
    return true;
  }
};

} // close namespace pert

%}

///////////////////////////////////////////////////////////////////////////////
// ProtoTableWriter
///////////////////////////////////////////////////////////////////////////////


namespace pert {
%feature("shadow") ProtoTableWriter::Open(const std::string& serialized_proto_format, const std::string& uri, int num_shards, WriterOptions options = WriterOptions()) %{
def Open(*args):
    
    # We intercept the SWIG binding and modify param 1 so that we have converted
    # a python protobuf into the serialized proto format string that can be 
    # passed to the C++      
    from pert import pert_pb2    
    def CreateProtoFileTree(desc, file_tree):
      # store this file            
      desc.file.CopyToProto(file_tree.file)  

      # store this file's dependencies         
      for name, field in desc.fields_by_name.iteritems():        
        if field.message_type:                    
          CreateProtoFileTree(field.message_type, file_tree.deps.add())
          
      return
      
    def GetSerializedFormat(proto):  
      proto_format = pert_pb2.ProtoFormat();        
      proto_format.name = proto.DESCRIPTOR.name            
      CreateProtoFileTree(proto.DESCRIPTOR, proto_format.file_tree);        
      return proto_format.SerializeToString()
    
    sf = GetSerializedFormat(args[1])
    
    # replace the second arg with the serialized proto format string
    args = (args[0], sf) + args[2:]    
    return $action(*args)    
%}
} // close namespace pert


%inline %{
namespace pert {
class ProtoTableWriter : public StringTableWriter {
public:
  ProtoTableWriter() {}
  virtual ~ProtoTableWriter(){}
  virtual bool Open(const std::string& serialized_proto_format, const std::string& uri, int num_shards, WriterOptions options = WriterOptions()){        
    CHECK(StringTableWriter::Open(uri, num_shards, options));
    AddMetadata(PERT_PROTO_FORMAT_METADATA_NAME, serialized_proto_format);    
    CHECK(is_open_);
    return true;
  }
  using StringTableWriter::Add; // need to explicitly bring the hidden Add from the base class... (http://www.cplusplus.com/forum/general/35681/)
  using StringTableWriter::Close;
};
} // close namespace pert
%}



///////////////////////////////////////////////////////////////////////////////
// utils
///////////////////////////////////////////////////////////////////////////////

%{
#include "snap/pert/utils.h"
%}
%include "snap/pert/utils.h"


///////////////////////////////////////////////////////////////////////////////
// UFS
///////////////////////////////////////////////////////////////////////////////

%{
#include "snap/pert/core/ufs.h"
%}
%include "snap/pert/core/ufs.h"


///////////////////////////////////////////////////////////////////////////////
// Bloom
///////////////////////////////////////////////////////////////////////////////

%{
#include "snap/pert/core/bloom.h"
#include "snap/pert/pert.pb.h"
%}
%include "snap/pert/core/bloom.h"


///////////////////////////////////////////////////////////////////////////////
// Partitioner
///////////////////////////////////////////////////////////////////////////////

%{
#include "snap/pert/core/partitioner.h"
%}
%include "snap/pert/core/partitioner.h"




