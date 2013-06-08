#pragma once

#include "snap/Eigen/SparseCore"
#include "snap/google/glog/logging.h"
#include "snap/google/base/basictypes.h"
#include "google/protobuf/repeated_field.h"
#include "iw/iw.pb.h"
#include "snap/boost/unordered_map.hpp"

namespace iw {


typedef Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor> DenseMatrix;
typedef Eigen::SparseMatrix<double, Eigen::RowMajor> SparseMatrix;
//typedef Eigen::DynamicSparseMatrix<double, Eigen::RowMajor> DSMat;
typedef Eigen::SparseVector<double> SparseVector;

void PropagateLabels(const SparseMatrix& S, const SparseMatrix& Y, SparseMatrix* R_out);

bool IsFinite(const double x);
void Copy(const Eigen::ArrayXd& src, ::google::protobuf::RepeatedField< double >* dst);
void ColStats(const Eigen::ArrayXXd& mat, Eigen::ArrayXd* col_mean, Eigen::ArrayXd* col_std);
void ComputeColumnStats(const Eigen::ArrayXXd& mat, ::google::protobuf::RepeatedField< double >* mean_repeated, ::google::protobuf::RepeatedField< double >* std_repeated);
void ComputeMeanStd(const Eigen::ArrayXXd& mat, double* mean, double* std);

uint32 GetBestLabel(const SparseMatrix& R, uint64 item_index, uint32 default_label, double min_score_threshold, double uniqueness_threshold, double* best_label_score = NULL );

class LabelPredictions {
public:
  LabelPredictions();
  void AddPredictions(const SparseMatrix& R, uint64 item_index);
  int GetNumPredictions() const;
  void GetPredictionStats(iw::LabelPredictionStatisticsList* prediction_stats_list) const;
private:
  int num_predictions_;
  typedef boost::unordered_map<uint32, std::vector<double> > LabelToValuesMap;
  LabelToValuesMap label_to_values_;
};

} // close namespace iw

