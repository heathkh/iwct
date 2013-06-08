%module py_ctis

%include snap/google/base/base.swig
%include iw/iw.swig
%include iw/matching/cbir/cbir.swig
%include iw/matching/cbir/bow/bow.swig

%{
#include "iw/matching/cbir/bow/ctis/index.h"
%}

%include "iw/matching/cbir/bow/ctis/index.h"