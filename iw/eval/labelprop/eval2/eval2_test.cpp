#include "iw/eval/labelprop/eval2/eval2.h"
#include "snap/google/gflags/gflags.h"
#include "snap/google/glog/logging.h"

using namespace std;
using namespace labelprop_eval2;

DEFINE_string(irg_uri, "", "name of input irg");
DEFINE_string(tide_uri, "", "name of input tide");

int main(int argc, char *argv[]) {
  //google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  CHECK(!FLAGS_irg_uri.empty()) << " Must provide --irg_uri";
  CHECK(!FLAGS_tide_uri.empty()) << " Must provide --tide_uri";

  uint32 num_trials = 1;
  double frac_aux_images_min = 0.9;
  double frac_aux_images_max = 1.0;
  double frac_aux_images_steps = 2;
  double frac_aux_images_delta = (frac_aux_images_max-frac_aux_images_min)/(frac_aux_images_steps-1);

  double num_training_images_min = 60;
  double num_training_images_max = 100;
  double num_training_images_steps = 2;
  double num_training_images_delta =  (num_training_images_max-num_training_images_min)/(num_training_images_steps-1);

  Params params;
  params.set_num_trials(num_trials);

  EvaluationRunner eval(FLAGS_irg_uri, FLAGS_tide_uri);

  for (int i = 0; i < frac_aux_images_steps; ++i ){
    double frac_aux_images = frac_aux_images_min + frac_aux_images_delta*i;
    params.set_frac_aux_images(frac_aux_images);
    for (int j = 0; j < num_training_images_steps; ++j){
      uint32 num_training_images = num_training_images_min + num_training_images_delta*j;
      params.set_num_training_images(num_training_images);
      LOG(INFO) << params.DebugString();

      Result result;
      CHECK(eval.Run(params, &result));
      LOG(INFO) << result.DebugString();
    }
  }
  LOG(INFO) << "done";
  return 0;
}
