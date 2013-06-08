%module py_bow

%include iw/iw.swig
%include iw/matching/cbir/cbir.swig
%include iw/matching/cbir/bow/bow.swig

%{
#include "iw/matching/cbir/bow/quantizer.h"
%}

%include "iw/matching/cbir/bow/quantizer.h"


