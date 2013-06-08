%module py_eval2
 
%include snap/google/base/base.swig
%include iw/eval/labelprop/eval2/eval2.swig

%{
#include "iw/eval/labelprop/eval2/eval2.h"
#include "iw/eval/labelprop/eval2/eval2.pb.h"
%}

%include "iw/eval/labelprop/eval2/eval2.h"

/*
namespace labelprop_eval2 {

class EvaluationRunner {
public:
  EvaluationRunner(const std::string& irg_uri, const std::string& tide_uri);
  bool Run(const labelprop_eval2::Params& params, labelprop_eval2::Result* result);
};

} // close namespace labelprop_eval2
*/
