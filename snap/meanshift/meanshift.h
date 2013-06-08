#pragma once
#include <vector>
#include "snap/opencv2/core/core.hpp"

namespace meanshift {

bool EstimateBandwidth(const cv::Mat& data, double quantile, int num_samples, double* bandwidth);
bool GenerateClusterLabels(const cv::Mat& data, float bandwidth, cv::Mat* centers, std::vector<unsigned int>* labels);

} // close namespace meanshift
