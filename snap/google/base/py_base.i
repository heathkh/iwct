%module py_base

%include "snap/google/base/base.swig"

%{
#include "snap/google/base/hashutils.h"
using std::string;
%}



%include "snap/google/base/hashutils.h"
