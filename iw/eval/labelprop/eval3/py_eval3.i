%module py_eval3
 
%include snap/google/base/base.swig
%include iw/eval/labelprop/eval3/eval3.swig

%{
#include "iw/eval/labelprop/eval3/eval3.h"
#include "iw/eval/labelprop/eval3/eval3.pb.h"
%}

%include "iw/eval/labelprop/eval3/eval3.h"

