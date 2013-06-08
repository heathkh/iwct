#pragma once
#include "deluge/deluge.h"
#include "snap/google/glog/logging.h"
#include "iw/iw.pb.h"

using namespace std;
using namespace deluge;

class MatchBatchesMapper : public JoinMapper {
public:
  MatchBatchesMapper(HadoopPipes::TaskContext& context) : JoinMapper(context) {
  }

  virtual void map(const JoinTuple& join_tuple, HadoopPipes::MapContext& context) {
    //const string& input_key = join_tuple.getKey();

    iw::ImageFeatures features;
    CHECK(join_tuple.getTupleValue(0, &features));

    iw::MatchBatchMetadata metadata;
    CHECK(join_tuple.getTupleValue(1, &metadata));


    CHECK_GT(metadata.batch_name().size(), 0);
    metadata.mutable_features()->CopyFrom(features);
    context.emit(metadata.batch_name(), metadata.SerializeAsString());
  }

};
