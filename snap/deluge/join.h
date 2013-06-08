#pragma once

#include "deluge/pipes/Pipes.hh"
#include "snap/pert/stringtable.h"

namespace deluge {

struct NoCopyBuffer {
  NoCopyBuffer(const char* buffer, uint64 length) :
    buffer(buffer),
    length(length)  {
  }
  NoCopyBuffer() {}
  const char* buffer;
  uint64 length;
  bool is_null;
};


class JoinTuple {
public:

  JoinTuple(int size) :
    key_(NULL),
    tuple_values_(size) {
    CHECK_GE(size, 0);
  }

  const std::string& getKey() const {
    CHECK(key_);
    return *key_;
  }

  // Returns true if the value at the requested index is not NULL
  bool getTupleValueNoCopy(int index, const char** value_buf, uint64* value_buf_len) const{
    CHECK_LT(index, tuple_values_.size()) << "Value index out of range " << index << ". Expected 0..." << tuple_values_.size()-1;
    if (tuple_values_[index].is_null){
      return false;
    }
    *value_buf = tuple_values_[index].buffer;
    *value_buf_len = tuple_values_[index].length;
    return true;
  }

  // Returns true if the value at the requested index is not NULL
  bool getTupleValue(int index, std::string* value) const{
    const char* value_buf;
    uint64 value_buf_len;
    bool result = getTupleValueNoCopy(index, &value_buf, &value_buf_len);
    value->assign(value_buf, value_buf_len);
    return result;
  }

  // Returns true if the value at the requested index is not NULL
  bool getTupleValue(int index, google::protobuf::Message* proto) const{
      const char* value_buf;
      uint64 value_buf_len;
      bool result = getTupleValueNoCopy(index, &value_buf, &value_buf_len);
      if (result){
        CHECK(proto->ParseFromArray(value_buf, value_buf_len)) << "Proto is not compatible.";
      }
      return result;
    }


protected:
  friend class JoinMapper;
  const std::string* key_;
  std::vector<NoCopyBuffer> tuple_values_;
};


class JoinMapper : public HadoopPipes::Mapper {
public:

  JoinMapper(HadoopPipes::TaskContext& context);
  virtual ~JoinMapper();
  void map(HadoopPipes::MapContext& context);

  virtual void map(const JoinTuple& join_tuple, HadoopPipes::MapContext& context) = 0;  // to be defined by a subclass

protected:

  JoinTuple join_tuple_;

  pert::StringTableShardReader secondary_reader_;
  bool secondary_input_done_;

  bool advance_k2_;
  const char* k2_buf;
  uint64 k2_len;
  const char* v2_buf;
  uint64 v2_len;

  std::string prev_k1_;
  std::string prev_k2_;

  boost::scoped_ptr<pert::KeyComparator> comparator_;

  HadoopPipes::TaskContext::Counter* primary_key_counter_;
  HadoopPipes::TaskContext::Counter* secondary_key_counter_;
  HadoopPipes::TaskContext::Counter* matching_key_counter_;
};


} // close namespace deluge
