#include "iw/matching/cbir/bow/observation_accumulator.h"

namespace cbir {

ObservationAccumulator::ObservationAccumulator() : num_dims_(0) {

}

void ObservationAccumulator::Add(const ClusterObservation& obs){
  if (!num_dims_){
    num_dims_ = obs.entries_size();
    acc_.CopyFrom(obs);
  }
  else{
    CHECK_EQ(obs.entries_size(), num_dims_);
    double alpha = double(acc_.count())/(acc_.count() + obs.count()); // The weight assigned to the previous observerations
    double beta = 1.0 - alpha; // The weight assigned to the new observation

    for (int i=0; i < num_dims_; ++i){
      double new_value = alpha*acc_.entries(i) + beta*obs.entries(i);
      acc_.mutable_entries()->Set(i, new_value);
    }

    acc_.set_count(acc_.count() + obs.count());
  }
}

ClusterObservation ObservationAccumulator::AccumulatedObservation() {
  return acc_;
}



}
