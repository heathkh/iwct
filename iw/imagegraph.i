%module py_imagegraph

%include snap/google/base/base.swig
%include std_string.i
%include exception.i
%include iw/iw.swig

%apply std::string& INPUT {const std::string&};

%{
#include "iw/imagegraph.h"
#include "iw/iw.pb.h"
%}

%include "iw/imagegraph.h"
