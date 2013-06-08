#include "iw/eval/labelprop/eval3/eval3.h"
#include "iw/imageregiongraph.h"
#include "snap/pert/stringtable.h"
#include <iostream>
#include <algorithm>
#include "snap/scul/scul.hpp"

using namespace std;
using namespace iw;
using namespace scul;

namespace labelprop_eval3 {

struct EdgeSortComparator {
  EdgeSortComparator(const ImageRegionGraph& irg) : irg_(irg) {}
  bool operator()(uint32 edge_a, uint32 edge_b){
    return irg_.edge(edge_a).weight() > irg_.edge(edge_b).weight();
  }
  const ImageRegionGraph& irg_;
};


EvaluationRunner::EvaluationRunner(const std::string& irg_uri,
                                   const std::string& tide_uri) :
 tide_(tide_uri)
{
  CHECK(LoadImageRegionGraph(irg_uri, &irg_));
  num_regions_ = irg_.vertex_size();

  std::vector<uint32> object_ids = tide_.GetObjectIds();
  object_ids.push_back(UNKNOWN_OBJECT);
  object_ids_ = scul::BidiractionalArray<uint32>(object_ids);
  num_labels_ = object_ids_.Size();
  CHECK_GT(num_labels_, 1);

  // Construct mapping from primary images to list of cluster regions and GT regions respectively
  for (int vertex_index=0; vertex_index < irg_.vertex_size(); ++vertex_index){
    const ImageRegionGraph_Vertex& v = irg_.vertex(vertex_index);
    if (!tide_.ImageIsPrimary(v.image_id())) continue;
    // region in a primary image
    CHECK_NE(v.type(), ImageRegionGraph_Vertex_VertexType_CLUSTER); // invariant... all cluster regions should be converted into another type

    if (v.type() == ImageRegionGraph_Vertex_VertexType_CLUSTER_OVERLAP_GROUND_TRUTH ||
        v.type() == ImageRegionGraph_Vertex_VertexType_CLUSTER_NONOVERLAP_GROUND_TRUTH){
      primaryphotoid_to_clusterregions_[v.image_id()].push_back(vertex_index);

      //if (v.type() == ImageRegionGraph_Vertex_VertexType_CLUSTER_NONOVERLAP_GROUND_TRUTH){
        //primaryphotoid_to_unmarked_clusterregions_[v.image_id()].push_back(vertex_index);
      //}
    }
    //else if (v.type() == ImageRegionGraph_Vertex_VertexType_GROUND_TRUTH){
    //  primaryphotoid_to_gtregions_[v.image_id()].push_back(vertex_index);
    //}
  }

  // Pre-compute Ground Truth data that gets reused
  // accumulate list of cluster region overlap edges (GT region <-> list of CLUSTER regions)
  for (int edge_index=0; edge_index < irg_.edge_size(); ++edge_index){
    const ImageRegionGraph_Edge& edge = irg_.edge(edge_index);
    uint32 src_vertex_id = edge.src();
    const ImageRegionGraph_Vertex& src_vertex = irg_.vertex(src_vertex_id);
    if (src_vertex.type() == ImageRegionGraph_Vertex_VertexType_GROUND_TRUTH){
      gtregion_overlap_edges_[src_vertex_id].push_back(edge_index);
    }
  }

  // sort each gtregion_overlap_edges_ array decreasing order by overlap weight
  EdgeSortComparator comparator(irg_);
  BOOST_FOREACH(VertexToEdgeMap::value_type& v, gtregion_overlap_edges_){
    scul::Sort(&v.second, comparator);
  }

  srand(unsigned(time(NULL))); // make sure seed is random
}

void EvaluationRunner::CreateLabelMatrix(const tide::ExperimentTrial& trial, Params_SupervisionType supervision_type, iw::SparseMatrix* Y){
  *Y = SparseMatrix(num_regions_, num_labels_);
  BOOST_FOREACH(const tide::ExperimentTrial_ImageLabelPair& label_pair, trial.training()){
    uint64 photo_id = label_pair.image_id();
    uint32 label_index = object_ids_.GetIndex(label_pair.object_id());
    if (supervision_type == Params_SupervisionType_DETECTION){
      // for each cluster region in training image
      BOOST_FOREACH(uint32 cluster_region_index, primaryphotoid_to_clusterregions_[photo_id]){
        Y->coeffRef(cluster_region_index, label_index) += 1.0;
      }
    }
    else if (supervision_type == Params_SupervisionType_LOCALIZATION){
      // for each cluster region in training image
      BOOST_FOREACH(uint32 cluster_region_index, primaryphotoid_to_clusterregions_[photo_id]){
        // if cluster region overlaps any GT regions
        if (irg_.vertex(cluster_region_index).type() == ImageRegionGraph_Vertex_VertexType_CLUSTER_OVERLAP_GROUND_TRUTH){
          Y->coeffRef(cluster_region_index, label_index) += 1.0;
        }
      }
    }
  }
}

// sort images descending by frequency
bool ImageFrequencyComparator(const ConfusionMatrixItemFreq_Row_Cell_Item& i1, const ConfusionMatrixItemFreq_Row_Cell_Item& i2){
  return i1.frequency() > i2.frequency();
}

bool EvaluationRunner::Run(const Params& params, Result* result) {
  CHECK(result);
  result->Clear();
  typedef boost::unordered_map <uint64, LabelPredictions> ImageIdToLabelPredictions;
  typedef std::vector<ImageIdToLabelPredictions> ConfusionMatrixLabelPredictionsRow;
  typedef std::vector<ConfusionMatrixLabelPredictionsRow> ConfusionMatrixLabelPredictions;

  ConfusionMatrixLabelPredictions confusion_matrix_label_predictions;
  for (int i=0; i < num_labels_; ++i){
    confusion_matrix_label_predictions.push_back(ConfusionMatrixLabelPredictionsRow(num_labels_));
  }

  Eigen::ArrayXXd trial_precision(params.num_trials(), num_labels_);
  Eigen::ArrayXXd trial_recall(params.num_trials(), num_labels_);

  // for each trial
  const uint32 unknown_label_index = object_ids_.GetIndex(UNKNOWN_OBJECT);
  DenseMatrix confusion_matrix_trial_history = DenseMatrix::Zero(params.num_trials(), num_labels_*num_labels_);
  for (uint32 trial_index=0; trial_index < params.num_trials(); ++trial_index) {
    LOG(INFO) << "starting trial: " << trial_index;
    tide::ExperimentTrial trial;
    tide_.GenerateTrial(params.num_training_images(), params.frac_aux_images(), &trial);
    iw::SparseMatrix Y, R, S;
    CreateSimilarityMatrix(trial, &S);
    CreateLabelMatrix(trial, params.supervision(), &Y);
    PropagateLabels(S, Y, &R);
    DenseMatrix trial_confusion_matrix = DenseMatrix::Zero(num_labels_, num_labels_);
    // score results
    BOOST_FOREACH(const tide::ExperimentTrial_ImageLabelPair& p, trial.testing()){
      uint64 test_image_id = p.image_id();
      uint32 best_region_index = 0;
      uint32 best_label_index = unknown_label_index;
      double best_label_score = 0;
      // Predict the label of the test image using the label of the best region in the test image
      BOOST_FOREACH(uint32 cur_region_index, primaryphotoid_to_clusterregions_[test_image_id]){
        uint32 region_label_index = unknown_label_index;
        double region_label_score = 0;
        region_label_index = GetBestLabel(R, cur_region_index, unknown_label_index, params.min_score_threshold(), params.uniqueness_threshold(), &region_label_score);
        if (region_label_score > best_label_score){
          best_region_index = cur_region_index;
          best_label_score = region_label_score;
          best_label_index = region_label_index;
        }
      }

      uint32 actual_label_index = object_ids_.GetIndex(tide_.GetObjectId(test_image_id));
      CHECK_LT(actual_label_index, num_labels_);
      uint32 predicted_label_index = best_label_index;
      double predicted_label_score = best_label_score;
      CHECK_LT(predicted_label_index, num_labels_);
      trial_confusion_matrix(predicted_label_index, actual_label_index) += 1.0;
      if (best_region_index > 0){
        confusion_matrix_label_predictions[predicted_label_index][actual_label_index][test_image_id].AddPredictions(R, best_region_index);
      }
    }
    Eigen::ArrayXd eps = Eigen::ArrayXd::Constant(num_labels_,1e-10);

    // compute precision
    // precision = tp / (tp + fp)  which is just the diagonal vector divided by the row sum vector
    Eigen::ArrayXd denom = trial_confusion_matrix.rowwise().sum().array();
    Eigen::ArrayXd precision = trial_confusion_matrix.diagonal().array() / (denom + eps); // add eps to prevent divide by zero
    CHECK_EQ(denom.cols(), 1);
    CHECK_EQ(precision.rows(), denom.rows());

    // handle the edge case where fp = 0
    for (int i=0; i < denom.rows(); ++i){
      if (denom(i,0) == 0){ // if tp + fp = 0  then fp = 0 then precision = 1
        precision(i,0) = 1.0;
      }
    }
    trial_precision.row(trial_index) = precision;
    // compute recall
    // recall = tp / (tp + fn)  which is just the diagonal vector divided by the col sum vector
    Eigen::ArrayXd recall = trial_confusion_matrix.diagonal().transpose().array() / (trial_confusion_matrix.colwise().sum().array() + eps.transpose());  // add eps to prevent divide by zero
    trial_recall.row(trial_index) = recall;
    confusion_matrix_trial_history.row(trial_index) = Eigen::Map<DenseMatrix>(trial_confusion_matrix.data(), 1, num_labels_*num_labels_);
  }

  // Copy output to result proto
  result->mutable_params()->CopyFrom(params);
  BOOST_FOREACH(uint32 label_id, object_ids_.GetElements()){
    result->add_label_ids(label_id);
    string name = "unknown";
    if (label_id != UNKNOWN_OBJECT){
      name = tide_.GetObject(label_id).name;
    }
    result->add_label_names(name);
  }

  // Populate precision and recall across all object classes (mean and std)
  double mean, std;
  ComputeMeanStd(trial_precision.leftCols(num_labels_-1), &mean, &std);
  result->mutable_precision()->set_mean(mean);
  result->mutable_precision()->set_std(std);
  ComputeMeanStd(trial_recall.leftCols(num_labels_-1), &mean, &std);
  result->mutable_recall()->set_mean(mean),
  result->mutable_recall()->set_std(std);

  // Populate per object class precision and recall
  ComputeColumnStats(trial_precision,
                     result->mutable_object_precision()->mutable_mean(),
                     result->mutable_object_precision()->mutable_std());
  ComputeColumnStats(trial_recall,
                     result->mutable_object_recall()->mutable_mean(),
                     result->mutable_object_recall()->mutable_std());

  // Populate the confusion matrix stats
  result->mutable_confusion_matrix()->set_rows(num_labels_);
  result->mutable_confusion_matrix()->set_cols(num_labels_);
  ComputeColumnStats(confusion_matrix_trial_history,
                     result->mutable_confusion_matrix()->mutable_mean(),
                     result->mutable_confusion_matrix()->mutable_std());

  // Copy confusion matrix cell image-frequency details
  for (int i=0; i < num_labels_; ++i){
    ConfusionMatrixItemFreq_Row* row = result->mutable_confusion_matrix_item_freq()->add_rows();
    for (int j=0; j < num_labels_; ++j){
      ConfusionMatrixItemFreq_Row_Cell* cell = row->add_cells();

      const ImageIdToLabelPredictions& imageid_to_labelpredictions = confusion_matrix_label_predictions[i][j];
      BOOST_FOREACH(ImageIdToLabelPredictions::value_type v, imageid_to_labelpredictions){
        uint64 region_id = v.first;
        const LabelPredictions& label_predictions = v.second;
        int num_predictions = label_predictions.GetNumPredictions();
        ConfusionMatrixItemFreq_Row_Cell_Item* region = cell->add_items();
        float frequency = float(num_predictions)/params.num_trials();
        region->set_item_id(region_id);
        region->set_frequency(frequency);
        label_predictions.GetPredictionStats(region->mutable_label_stats());
        CHECK(region->IsInitialized());
      }

      // Sort entries in descending order of frequency for later convenience
      scul::Sort(cell->mutable_items(), ImageFrequencyComparator);
    }
  }
  return true;
}

void EvaluationRunner::CreateSimilarityMatrix(const tide::ExperimentTrial& trial, SparseMatrix* S) {
  int num_edges = irg_.edge_size();
  CHECK_GT(num_edges, 0);
  boost::unordered_set<uint64> aux_blacklist_image_set(trial.heldout_aux_images().begin(), trial.heldout_aux_images().end());
  typedef Eigen::Triplet<double> Triplet;
  SparseMatrix W(num_regions_, num_regions_);
  {
    std::vector<Triplet> triplets;
    triplets.reserve(num_edges*2);
    // Create symmetric similarity matrix
    BOOST_FOREACH(const ImageRegionGraph_Edge& edge, irg_.edge()){
      // skip adding any edge between regions in images in blacklist
      if (scul::Contains(aux_blacklist_image_set, irg_.vertex(edge.src()).image_id())) {
        continue;
      }
      if (scul::Contains(aux_blacklist_image_set, irg_.vertex(edge.dst()).image_id())) {
        continue;
      }
      // skip any edge from a GT region (GT shouldn't impact label propagation)
      if (irg_.vertex(edge.src()).type() == ImageRegionGraph_Vertex_VertexType_GROUND_TRUTH){
        continue;
      }
      triplets.push_back(Triplet(edge.src(), edge.dst(), edge.weight()));
      triplets.push_back(Triplet(edge.dst(), edge.src(), edge.weight()));
    }
    W.setFromTriplets(triplets.begin(), triplets.end());
  }
  LOG(INFO) << "done creating W";
  // Compute symmetric normalization
  SparseMatrix F(num_regions_, num_regions_);
  {
    std::vector<Triplet> triplets;
    triplets.reserve(num_regions_);
    for (int k = 0; k < W.outerSize(); ++k){
      double row_sum = 0;
      for (SparseMatrix::InnerIterator it(W, k); it; ++it){
        row_sum += it.value();
      }
      if (row_sum > 0){
        triplets.push_back(Triplet(k, k, sqrt(1.0/row_sum)));
      }
    }
    F.setFromTriplets(triplets.begin(), triplets.end());
  }
  LOG(INFO) << "done creating F";
  *S = F*W*F;
}


void SaveResultOrDie(const labelprop_eval3::Result& result, const std::string& uri){
  pert::SaveProtoOrDie(result, uri);
}

void LoadResultOrDie(const std::string& uri, labelprop_eval3::Result* result){
  pert::LoadProtoOrDie(uri, result);
}


} // close namespace labelprop_eval3





