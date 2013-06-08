#pragma once

// Evaluation 3 - Propagates labels based on regions, but labeled documents are images not region... hybrid of eval1 and eval2

#include <vector>
#include <map>
#include <set>
#include "snap/google/base/integral_types.h"
#include "iw/eval/labelprop/eval3/eval3.pb.h"
#include "tide/tide.pb.h"
#include "iw/iw.pb.h"
#include "tide/tide.h"
#include "snap/boost/unordered_map.hpp"
#include "snap/boost/unordered_set.hpp"
#include "snap/scul/scul.hpp"
#include "iw/labelprop.h"
#include "snap/pert/pert.h"

#define UNKNOWN_LABEL kuint32max

namespace labelprop_eval3 {

class EvaluationRunner {
public:
  EvaluationRunner(const std::string& irg_uri, const std::string& tide_uri);
  bool Run(const labelprop_eval3::Params& params, labelprop_eval3::Result* result);

private:
  tide::TideDataset tide_;
  iw::ImageRegionGraph irg_;
  uint32 num_regions_;
  uint32 num_labels_;
  scul::BidiractionalArray<uint32> region_ids_; // maps between matrix index and region ids
  scul::BidiractionalArray<uint32> object_ids_; // maps between matrix index and object ids

  typedef boost::unordered_map<uint64, std::vector<uint32> > PhotoIdToRegionsMap;
  PhotoIdToRegionsMap primaryphotoid_to_clusterregions_;
  //PhotoIdToRegionsMap primaryphotoid_to_unmarked_clusterregions_;
  //PhotoIdToRegionsMap primaryphotoid_to_gtregions_;
  typedef boost::unordered_map<uint32, std::vector<uint32> > VertexToEdgeMap;
  VertexToEdgeMap gtregion_overlap_edges_;
  void CreateSimilarityMatrix(const tide::ExperimentTrial& trial, iw::SparseMatrix* S);
  void CreateLabelMatrix(const tide::ExperimentTrial& trial, labelprop_eval3::Params_SupervisionType supervision_type, iw::SparseMatrix* Y);
};

void SaveResultOrDie(const labelprop_eval3::Result& result, const std::string& uri);
void LoadResultOrDie(const std::string& uri, labelprop_eval3::Result* result);

}
