#include "iw/labelprop.h"
#include "snap/google/glog/logging.h"
#include <fstream>
#include "snap/scul/scul.hpp"
#include "snap/google/glog/logging.h"
#include "snap/boost/accumulators/accumulators.hpp"
#include "snap/boost/accumulators/statistics/stats.hpp"
#include "snap/boost/accumulators/statistics/variance.hpp"
#include "snap/boost/accumulators/statistics/mean.hpp"
#include "snap/boost/accumulators/statistics/moment.hpp"




void ComputeMeanAndVariance(std::vector<double> values, double* mean_val, double* variance_val){
  CHECK(values.size());
  CHECK(mean_val);
  CHECK(variance_val);
  using namespace boost::accumulators;
  typedef accumulator_set<double, stats<tag::mean, tag::variance(lazy)> > StatsAccumulator;
  StatsAccumulator acc;
  for (int i=0; i < values.size(); ++i){
    acc(values[i]);
  }
  *mean_val = mean(acc);
  *variance_val = variance(acc);
}



namespace iw {

bool IsFinite(const double x) {
  return !isinf(x)  &&  !isnan(x);
}

void PropagateLabels(const SparseMatrix& S, const SparseMatrix& Y, SparseMatrix* R_out){
  CHECK(R_out);
  int max_iterations = 40;
  double alpha = 0.40;
  SparseMatrix G = S*alpha;
  SparseMatrix H = (1.0-alpha)*Y;
  SparseMatrix& R = *R_out;
  SparseMatrix Rprev = Y;
  double delta = 1e6;
  int iter = 0;
  double term_threshold = 0.1;
  while (delta > term_threshold) {
    if (iter > max_iterations){
      break;
    }
    R = G*Rprev + H;
    SparseMatrix d = R-Rprev;
    delta = 0;
    for (int k=0; k < d.outerSize(); ++k)
      for (SparseMatrix::InnerIterator it(d,k); it; ++it){
        delta += fabs(it.value());
      }
    Rprev = R;
    LOG(INFO) << "iter: " << iter << " delta: "  << delta;
    iter++;
  }
}

void Copy(const Eigen::ArrayXd& src, ::google::protobuf::RepeatedField< double >* dst){
  CHECK_EQ(dst->size(), 0);
  CHECK_EQ(src.cols(), 1);
  for (int row = 0; row < src.rows(); ++row){
    dst->Add(src(row));
  }
}

void ColStats(const Eigen::ArrayXXd& mat, Eigen::ArrayXd* col_mean, Eigen::ArrayXd* col_std){
  *col_mean = mat.colwise().mean();
  Eigen::ArrayXd mean_sqr = (*col_mean) * (*col_mean);
  Eigen::ArrayXd sqr_mean = mat.square().colwise().mean();
  *col_std = (sqr_mean - mean_sqr).sqrt();

  CHECK_EQ(col_mean->cols(), 1);
  CHECK_EQ(col_std->cols(), 1);
  CHECK_EQ(col_mean->rows(), col_std->rows());
}

void ComputeColumnStats(const Eigen::ArrayXXd& mat, ::google::protobuf::RepeatedField< double >* mean_repeated, ::google::protobuf::RepeatedField< double >* std_repeated){
  Eigen::ArrayXd mean, std;
  ColStats(mat, &mean, &std);
  Copy(mean, mean_repeated);
  Copy(std, std_repeated);
}


void ComputeMeanStd(const Eigen::ArrayXXd& mat, double* mean, double* std){
  int n = mat.rows()*mat.cols();
  const Eigen::Map<const Eigen::VectorXd> v(mat.data(), n);
  *mean = v.mean();
  double mean_sqr = (*mean) * (*mean);
  double sqr_mean = v.squaredNorm() / n;
  *std = sqrt(sqr_mean - mean_sqr);
}


typedef std::pair<uint32, double> LabelCandidatePair;
typedef std::vector<LabelCandidatePair> LabelCandidatePairs;


bool SortCandidatesDecreasingCmp(const LabelCandidatePair& a, const LabelCandidatePair& b ){
    return a.second > b.second;
}


uint32 GetBestLabel(const SparseMatrix& R,
                    uint64 item_index,
                    uint32 default_label,
                    double min_score_threshold, // 2e-5 if a score is less than this, we don't try to guess
                    double uniqueness_threshold, // 1.5 number of orders of magnitude by which the first best score must exceed the second best score
                    double* best_label_score
                    ){
  // compute the best label assignments for the given image_id
  // If no label is above the min value threshold or if the 2nd place candidate
  // has a similar score (potential ambiguity) we choose not to choose and return
  // UNKNOWN_OBJECT...
  CHECK(R.IsRowMajor);
  CHECK_LT(item_index,  R.outerSize());
  //uint32 best_label_index = object_ids_.GetIndex(UNKNOWN_OBJECT);
  uint32 best_label_index = default_label;
  double max_value = 0;

  LabelCandidatePairs candidate_label_scores;

  for (SparseMatrix::InnerIterator it(R,item_index); it; ++it) {
    CHECK(IsFinite(it.value())) << "got non-finite value: " << it.value();
    candidate_label_scores.push_back(LabelCandidatePair(it.col(), it.value()));
  }

  if (candidate_label_scores.size() >= 1){
    LabelCandidatePair best_label;
    if (candidate_label_scores.size() == 1){
      best_label = candidate_label_scores[0];
    }
    else {
      // Find the top 2 ranked labels
      std::partial_sort(candidate_label_scores.begin(),
                        candidate_label_scores.begin()+2,
                        candidate_label_scores.end(),
                        SortCandidatesDecreasingCmp);
      best_label = candidate_label_scores[0];
    }

    if (best_label.second > min_score_threshold){
      // if there is only one label and the score is above threshold, it can be selected
      if (candidate_label_scores.size() == 1){
        best_label_index = best_label.first;
      }
      // if there is more than one candidate label, the first must be significantly better than the 2nd best candidate to be selected
      // (we are only confident if there is no close runner-up)
      else{
        LabelCandidatePair next_best_label = candidate_label_scores[1];
        double mag_better_than_next = log10(best_label.second) - log10(next_best_label.second);
        if (mag_better_than_next > uniqueness_threshold){  // at least one order of magnitude better score
          best_label_index = best_label.first;
          if (best_label_score){
            *best_label_score = best_label.second;
          }
        }
      }
    }
  }

  return best_label_index; // index of UNKNOWN_OBJECT if no label can be propagated
}


struct LabelPrediction {
  uint32 label_index;
};

bool SortDecreasingByMeanCmp(const iw::LabelPredictionStatistics& a, const iw::LabelPredictionStatistics& b){
  return a.mean() > b.mean();
}


LabelPredictions::LabelPredictions() : num_predictions_(0) {
}

void LabelPredictions::AddPredictions(const SparseMatrix& R, uint64 item_index){
  // compute the best label assignments for the given image_ids
  CHECK(R.IsRowMajor);
  CHECK_LT(item_index,  R.outerSize());
  for(SparseMatrix::InnerIterator it(R,item_index); it; ++it) {
    CHECK(IsFinite(it.value())) << "got non-finite value: " << it.value();
    uint32 label_index = it.col();
    label_to_values_[label_index].push_back(it.value());
  }
  ++num_predictions_;
}

int LabelPredictions::GetNumPredictions() const {
  return num_predictions_;
}

void LabelPredictions::GetPredictionStats(iw::LabelPredictionStatisticsList* prediction_stats_list) const {
  CHECK(prediction_stats_list);
  prediction_stats_list->Clear();
  BOOST_FOREACH(const LabelToValuesMap::value_type& v, label_to_values_){
    double mean, variance;
    ComputeMeanAndVariance(v.second, &mean, &variance);
    iw::LabelPredictionStatistics* label_stats = prediction_stats_list->add_entries();
    label_stats->set_label_index(v.first);
    label_stats->set_mean(mean);
    label_stats->set_variance(variance);
  }

  scul::Sort(prediction_stats_list->mutable_entries(), SortDecreasingByMeanCmp);
}


} // close namespace
