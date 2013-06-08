#include "deluge/std.h"
#include "deluge/job.h"
#include "snap/google/base/hashutils.h"
#include "snap/google/glog/logging.h"

using namespace std;

namespace deluge {

ModKeyPartitioner::ModKeyPartitioner(HadoopPipes::MapContext& context) { }

ModKeyPartitioner::~ModKeyPartitioner() { }

int ModKeyPartitioner::partition(const std::string& key, int numOfReduces){
  if (!partitioner_.get()){
    partitioner_.reset(new pert::ModKeyPartitioner(numOfReduces));
  }
  CHECK_EQ(numOfReduces, partitioner_->NumShards());
  return partitioner_->Partition(key);
}

BroadcastPartitioner::BroadcastPartitioner(HadoopPipes::MapContext& context) :
    num_reducers_(-1),
    broadcast_key_(""),
    broadcast_count_(0)
{
  LOG(INFO) << "BroadcastPartitioner()";
}

BroadcastPartitioner::~BroadcastPartitioner(){
  LOG(INFO) << "~BroadcastPartitioner()";
}

int BroadcastPartitioner::partition(const std::string& key, int numOfReduces){
  if (num_reducers_ == -1){
    num_reducers_ = numOfReduces;
  }
  CHECK_EQ(numOfReduces, num_reducers_);

  int partition = -1;
  if (broadcast_key_.empty()){
    broadcast_count_ = 0;
    broadcast_key_ = key;
  }

  LOG(INFO) << "key: " << KeyToUint64(key);

  if (key != broadcast_key_){
    LOG(INFO) << "broadcast_key_: " << KeyToUint64(broadcast_key_);
    CHECK_EQ(broadcast_count_, num_reducers_); // only switch keys after broadcasting same key exactly R times
    broadcast_key_ = key;
    broadcast_count_ =0;
  }

  partition = broadcast_count_;
  broadcast_count_ += 1;

  CHECK_LT(partition, num_reducers_);
  CHECK_GE(partition, 0);
  return partition;
}

} // close namespace deluge
