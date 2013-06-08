#pragma once

// Evaluation 1 - Explorers how label propagation performance on the image graph is
// affected by the number of supervised and unsupervised images.
// Questions we want to answer:
//   Given a fixed number of supervised images... How rapidly does performance improve as we add semi-supervised images.
//   Given a fixed number of semi-supervised images... How many supervised images do we need to achieve a certain performance?

#include "snap/google/base/integral_types.h"
#include "iw/labelprop.h"
#include "iw/eval/labelprop/eval1/eval1.pb.h"
#include <map>
#include <string>
#include "tide/tide.h"
#include "snap/scul/scul.hpp"

namespace labelprop_eval1 {

class EvaluationRunner {
public:
  EvaluationRunner(const std::string& image_graph_uri, const std::string& tide_uri);
  bool Run(int num_training_images, int num_trials, labelprop_eval1::Result* result);

private:
  typedef ::google::protobuf::RepeatedPtrField< ::tide::ExperimentTrial_ImageLabelPair > RepeatedImageLabel;
  void InitPhaseSimilarityMatrix(int phase_id);
  void CreateLabelMatrix(const RepeatedImageLabel& training_labels, iw::SparseMatrix* Y);
  bool EvalPhase(int phase, int num_training_images, int num_trials, labelprop_eval1::Result::Phase* phase_result);

  iw::ImageGraph ig_;
  iw::SparseMatrix similarity_matrix_;
  scul::BidiractionalArray<uint64> image_ids_; // maps between matrix index and image ids
  scul::BidiractionalArray<uint32> object_ids_; // maps between matrix index and object ids
  tide::TideDataset dataset_;
  uint32 num_labels_; // num objects + special unknown label
  uint32 num_images_;
};

void SaveResultOrDie(const labelprop_eval1::Result& result, const std::string& uri);
void LoadResultOrDie(const std::string& uri, labelprop_eval1::Result* result);

} // close namespace labelprop_eval1


