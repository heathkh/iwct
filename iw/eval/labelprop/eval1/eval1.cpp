#include "iw/eval/labelprop/eval1/eval1.h"
#include "iw/imagegraph.h"
#include <string>
#include "snap/pert/pert.h"
#include "snap/scul/scul.hpp"

using namespace iw;
using namespace std;

namespace labelprop_eval1 {

EvaluationRunner::EvaluationRunner(const std::string& image_graph_uri, const std::string& tide_uri) :
  dataset_(tide_uri)
{
  CHECK(LoadImageGraph(image_graph_uri, &ig_));

  num_images_ = ig_.vertices_size();



  // Create mapping from image_id to image_index needed to map between image index and image id
  {
    std::vector<uint64> vertices(ig_.vertices_size());
    for (int i=0; i < ig_.vertices_size(); ++i){
      vertices[i] = ig_.vertices(i).image_id();
    }
    image_ids_ = scul::BidiractionalArray<uint64>(vertices);
  }

  std::vector<uint32> object_ids = dataset_.GetObjectIds();
  object_ids.push_back(UNKNOWN_OBJECT);
  object_ids_ = scul::BidiractionalArray<uint32>(object_ids);
  num_labels_ = object_ids_.Size();
  srand(unsigned(time(NULL))); // make sure seed is random
}

// sort Images descending by frequency
bool ImageFrequencyComparator(const ConfusionMatrixItemFreq_Row_Cell_Item& i1, const ConfusionMatrixItemFreq_Row_Cell_Item& i2){
  return i1.frequency() > i2.frequency();
}


// Create item-item similarity matrix from image graph at a given phase
void EvaluationRunner::InitPhaseSimilarityMatrix(int phase_id) {
  uint64 num_edges = ig_.edges_size();
  CHECK_GT(num_edges, 0);
  typedef Eigen::Triplet<double> Triplet;
  SparseMatrix W(num_images_, num_images_);
  {
   std::vector<Triplet> triplets;
   triplets.reserve(num_edges*2);
   // Create symmetric similarity matrix
   BOOST_FOREACH(const ImageGraph_Edge& edge, ig_.edges()){
     if (edge.phase() <= phase_id){
       triplets.push_back(Triplet(edge.src(), edge.dst(), edge.weight()));
       triplets.push_back(Triplet(edge.dst(), edge.src(), edge.weight()));
     }
   }
   W.setFromTriplets(triplets.begin(), triplets.end());
  }
  LOG(INFO) << "done creating W";

  // Compute symmetric normalization
  SparseMatrix F(num_images_, num_images_);
  {
   std::vector<Triplet> triplets;
   triplets.reserve(num_images_);
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
  similarity_matrix_ = F*W*F;
}


bool EvaluationRunner::Run(int num_training_images, int num_trials, Result* result) {
  CHECK_GT(num_training_images, 0);
  CHECK_GT(num_trials, 0);
  CHECK(result);
  result->Clear();

  // Copy output to result proto
  BOOST_FOREACH(uint32 label_id, object_ids_.GetElements()){
    result->add_label_ids(label_id);
    string name = "unknown";
    if (label_id != UNKNOWN_OBJECT){
      name = dataset_.GetObject(label_id).name;
    }
    result->add_label_names(name);
  }

  // run eval for each phase

  int num_phases =  ig_.num_phases();
  CHECK_GT(num_phases, 0);

  for (int phase=0; phase < num_phases; ++phase){
    Result::Phase* phase_result = result->add_phases();
    CHECK(EvalPhase(phase, num_training_images, num_trials, phase_result));
  }

  return true;
}

bool EvaluationRunner::EvalPhase(int phase, int num_training_images, int num_trials, Result::Phase* result) {
  CHECK_GE(phase, 0);
  InitPhaseSimilarityMatrix(phase);
  Eigen::ArrayXXd trial_precision(num_trials, num_labels_);
  Eigen::ArrayXXd trial_recall(num_trials, num_labels_);

  typedef boost::unordered_map <uint64, LabelPredictions> ImageIdToLabelPredictions;
  typedef std::vector<ImageIdToLabelPredictions> ConfusionMatrixImageLabelPredictionsRow;
  typedef std::vector<ConfusionMatrixImageLabelPredictionsRow> ConfusionMatrixImageLabelPredictions;

  ConfusionMatrixImageLabelPredictions confusion_matrix_image_label_predictions;
  for (int i=0; i < num_labels_; ++i){
    confusion_matrix_image_label_predictions.push_back(ConfusionMatrixImageLabelPredictionsRow(num_labels_));
  }

  // for each trial
  const uint32 unknown_label_index = object_ids_.GetIndex(UNKNOWN_OBJECT);
  DenseMatrix confusion_matrix_trial_history = DenseMatrix::Zero(num_trials, num_labels_*num_labels_);
  for (uint32 trial_index=0; trial_index < num_trials; ++trial_index) {
    LOG(INFO) << "starting trial: " << trial_index;
    tide::ExperimentTrial trial;
    dataset_.GenerateTrial(num_training_images, 1.0, &trial);
    CHECK_EQ(trial.heldout_aux_images_size(), 0);
    SparseMatrix Y, R;
    CreateLabelMatrix(trial.training(), &Y);
    PropagateLabels(similarity_matrix_, Y, &R);

    // score result
    // note num_objects_ includes the extra "Unknown Object" label
    DenseMatrix trial_confusion_matrix = DenseMatrix::Zero(num_labels_, num_labels_);

    double min_score_threshold = 2e-5; // if a score is less than this, we don't try to guess
    double uniqueness_threshold = 1.5;//  number of orders of magnitude by which the first best score must exceed the second best score

    BOOST_FOREACH(const tide::ExperimentTrial_ImageLabelPair& p, trial.testing()){

      uint32 actual_label_index = object_ids_.GetIndex(dataset_.GetObjectId(p.image_id()));
      uint32 predicted_label_index = unknown_label_index;
      uint32 image_index = image_ids_.GetIndex(p.image_id());
      predicted_label_index = GetBestLabel(R, image_index, unknown_label_index, min_score_threshold, uniqueness_threshold );

      CHECK_LT(actual_label_index, num_labels_);
      CHECK_LT(predicted_label_index, num_labels_);
      trial_confusion_matrix(predicted_label_index, actual_label_index) += 1.0;
      //confusion_matrix_image_counts[predicted_label_index][actual_label_index][p.image_id()] += 1;
      confusion_matrix_image_label_predictions[predicted_label_index][actual_label_index][p.image_id()].AddPredictions(R, image_index);
    }
    Eigen::ArrayXd eps = Eigen::ArrayXd::Constant(num_labels_, 1e-10);
    // compute precision
    // precision = tp / (tp + fp)  which is just the diagonal vector divided by the row sum vector
    Eigen::ArrayXd precision = trial_confusion_matrix.diagonal().array() / (trial_confusion_matrix.rowwise().sum().array() + eps); // add eps to prevent divide by zero
    trial_precision.row(trial_index) = precision;

    // compute recall
    // recall = tp / (tp + fn)  which is just the diagonal vector divided by the col sum vector
    Eigen::ArrayXd recall = trial_confusion_matrix.diagonal().transpose().array() / (trial_confusion_matrix.colwise().sum().array() + eps.transpose());  // add eps to prevent divide by zero
    trial_recall.row(trial_index) = recall;
    confusion_matrix_trial_history.row(trial_index) = Eigen::Map<DenseMatrix>(trial_confusion_matrix.data(), 1, num_labels_*num_labels_);
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

      const ImageIdToLabelPredictions& imageid_to_labelpredictions = confusion_matrix_image_label_predictions[i][j];
      BOOST_FOREACH(ImageIdToLabelPredictions::value_type v, imageid_to_labelpredictions){
        uint64 image_id = v.first;
        const LabelPredictions& label_predictions = v.second;
        int num_predictions = label_predictions.GetNumPredictions();
        ConfusionMatrixItemFreq_Row_Cell_Item* image = cell->add_items();
        float frequency = float(num_predictions)/num_trials;
        image->set_item_id(image_id);
        image->set_frequency(frequency);
        label_predictions.GetPredictionStats(image->mutable_label_stats());
        CHECK(image->IsInitialized());
      }

      // Sort entries in descending order of frequency for later convenience
      scul::Sort(cell->mutable_items(), ImageFrequencyComparator);
    }
  }

  return true;
}

// Initialize the seed label matrix Y from the given photo id to label mapping
void EvaluationRunner::CreateLabelMatrix(const RepeatedImageLabel& training_labels, SparseMatrix* Y){
  *Y = SparseMatrix(num_images_, num_labels_);
  BOOST_FOREACH(const tide::ExperimentTrial_ImageLabelPair& l, training_labels){
    uint32 image_index = image_ids_.GetIndex(l.image_id());
    uint32 object_index = object_ids_.GetIndex(l.object_id());
    Y->coeffRef(image_index, object_index) += 1.0;
  }
}


void SaveResultOrDie(const labelprop_eval1::Result& result, const std::string& uri){
  pert::SaveProtoOrDie(result, uri);
}

void LoadResultOrDie(const std::string& uri, labelprop_eval1::Result* result){
  pert::LoadProtoOrDie(uri, result);
}

} // close namespace labelprop_eval3
