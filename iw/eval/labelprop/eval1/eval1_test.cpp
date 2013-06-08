#include "iw/eval/labelprop/eval1/eval1.h"
#include "snap/google/gflags/gflags.h"
#include "snap/google/glog/logging.h"

using namespace std;
using namespace labelprop_eval1;

DEFINE_string(ig_uri, "", "name of input ig");
DEFINE_string(tide_uri, "", "name of input tide");

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  CHECK(!FLAGS_ig_uri.empty()) << " Must provide --ig_uri";
  CHECK(!FLAGS_tide_uri.empty()) << " Must provide --tide_uri";
  int num_trials = 20;
  int num_training_images = 100;
  EvaluationRunner eval(FLAGS_ig_uri, FLAGS_tide_uri);
  Result result;
  CHECK(eval.Run(num_training_images, num_trials, &result));
  const Result::Phase& final_phase = result.phases(result.phases_size()-1);
  LOG(INFO) << "precision: " << final_phase.precision().mean() << " recall: " << final_phase.recall().mean();
  LOG(INFO) << "done";
  return 0;
}
