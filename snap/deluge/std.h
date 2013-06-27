#pragma once

#include "snap/pert/core/partitioner.h"
#include "deluge/debug.h"
#include "deluge/pertio.h"
#include "deluge/pipes/Pipes.hh"
#include "snap/boost/scoped_ptr.hpp"
#include <string>
#include <vector>

namespace deluge {


class ModKeyPartitioner : public HadoopPipes::Partitioner {
public:
  ModKeyPartitioner(HadoopPipes::MapContext& context);
  virtual ~ModKeyPartitioner();
  virtual int partition(const std::string& key, int numOfReduces);

private:
  boost::scoped_ptr<pert::ModKeyPartitioner> partitioner_;
};

// Special partitioner used to prodcast a copy of the key to all reducers
// If there are N reducers, the mapper must emit the same key N times or this
// is an error.
class BroadcastPartitioner : public HadoopPipes::Partitioner {
public:
  BroadcastPartitioner(HadoopPipes::MapContext& context);
  virtual ~BroadcastPartitioner();
  virtual int partition(const std::string& key, int numOfReduces);

private:
  int num_reducers_;
  std::string broadcast_key_;
  int broadcast_count_;
};

class IdentityMapper : public HadoopPipes::Mapper {
public:
  IdentityMapper(HadoopPipes::TaskContext& context) {}
  void map(HadoopPipes::MapContext& context) {
    context.emit(context.getInputKey(), context.getInputValue());
  }
};


class DropValueMapper : public HadoopPipes::Mapper {
public:
  DropValueMapper(HadoopPipes::TaskContext& context) {}
  void map(HadoopPipes::MapContext& context) {
    context.emit(context.getInputKey(), "");
  }
};

//TODO(kheath): Can we use the -reduce org.apache.hadoop.mapred.lib.IdentityReducer
// to accelerate this? or does this bypass our RecordWriter?
class IdentityReducer : public HadoopPipes::Reducer {
public:
  IdentityReducer(HadoopPipes::TaskContext& context) {}
  void reduce(HadoopPipes::ReduceContext& context) {
    while (context.nextValue()){
      context.emit(context.getInputKey(), context.getInputValue());
    }
  }
};


class AtMostOnceReducer : public HadoopPipes::Reducer {
public:
  AtMostOnceReducer(HadoopPipes::TaskContext& context) : counters_(context, "deluge") { }
  void reduce(HadoopPipes::ReduceContext& context) {
    CHECK(context.nextValue());
    context.emit(context.getInputKey(), context.getInputValue());
    int duplicate_count = 0;
    while (context.nextValue()){
      ++duplicate_count;
    }
    if (duplicate_count){
      counters_.Increment("num_keys_with_duplicates");
      counters_.Increment("num_dropped_duplicates", duplicate_count);
    }
  }
  private:
  CounterGroup counters_;
};

} // close namespace deluge
