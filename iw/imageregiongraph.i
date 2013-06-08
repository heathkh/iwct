%module py_imageregiongraph

%include snap/google/base/base.swig
%include std_string.i
%include exception.i
%include iw/iw.swig

%apply std::string& INPUT {const std::string&};

%{
#include "iw/imageregiongraph.h"
#include "iw/iw.pb.h"


%}

%include "iw/imageregiongraph.h"
