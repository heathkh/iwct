%module py_featureextractor

%include "snap/google/base/base.swig" 
%include "std_string.i"
%include "std_vector.i"
%include "exception.i"
%include "snap/google/base/base.swig"
%include "iw/iw.swig"

%apply std::string& INPUT {const std::string&};
%apply std::string* OUTPUT {std::string*};

%{
#include "iw/matching/feature_extractor.h"
%}

%include "iw/matching/feature_extractor.h"
