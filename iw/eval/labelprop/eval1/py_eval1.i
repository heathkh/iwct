%module py_eval1
 
%include snap/google/base/base.swig
%include iw/eval/labelprop/eval1/eval1.swig

%{
#include "iw/eval/labelprop/eval1/eval1.h"
#include "iw/eval/labelprop/eval1/eval1.pb.h"
%}

%include "iw/eval/labelprop/eval1/eval1.h"
