#include "iw/eval/labelprop/eval3/eval3.h"
#include "snap/google/gflags/gflags.h"
#include "snap/google/glog/logging.h"

using namespace std;
using namespace labelprop_eval3;

DEFINE_string(irg_uri, "", "name of input irg");
DEFINE_string(tide_uri, "", "name of input tide");

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  CHECK(!FLAGS_irg_uri.empty()) << " Must provide --irg_uri";
  CHECK(!FLAGS_tide_uri.empty()) << " Must provide --tide_uri";

  Params params;
  params.set_num_trials(20);
  params.set_frac_aux_images(1.0);
  params.set_num_training_images(100);

  EvaluationRunner eval(FLAGS_irg_uri, FLAGS_tide_uri);
  Result result;
  CHECK(eval.Run(params, &result));
  LOG(INFO) << result.DebugString();

  LOG(INFO) << "done";
  return 0;
}
