#pragma once

#include "snap/google/glog/logging.h"
#include "snap/google/base/integral_types.h"
#include "iw/iw.pb.h"
#include "iw/matching/cbir/bow/bow.pb.h"

namespace cbir {

class ObservationAccumulator {
public:

  ObservationAccumulator();
  void Add(const ClusterObservation& obs);
  ClusterObservation AccumulatedObservation();

private:

  uint32 num_dims_;
  ClusterObservation acc_;

};

} //close namespace iw
