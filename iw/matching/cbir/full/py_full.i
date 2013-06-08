%module py_full

%include snap/google/base/base.swig 
%include std_string.i
%include std_vector.i
%include exception.i
%include snap/google/base/base.swig
%include iw/iw.swig
%include iw/matching/cbir/cbir.swig
%include typemaps.i

%{
#include "iw/matching/cbir/full/full.h"
%}

/// Magic to wrap vector of uint64 in a nice way
%{ 
#include <vector>
%}

/// Magic to wrap vector of strings in a nice way
%{ 
#include <vector>
#include <string>
%} 
namespace std {
   %template(VectorOfString) vector<string>;
}

namespace std {
   %template(VectorOfUint64) vector<unsigned long long>;
   specialize_std_vector(uint64,PyLong_Check,PyLong_AsUnsignedLongLong,PyLong_FromUnsignedLongLong);
}

// rewrite Search to be python friendly (take list of python longs)
%ignore Search(uint64 ignore_image_id, int k, const iw::ImageFeatures& features, cbir::ImageFeaturesNeighbors* features_neighbors) const;
%ignore Search(const std::set<uint64>& ignore_image_ids, int k, const iw::ImageFeatures& features, cbir::ImageFeaturesNeighbors* features_neighbors) const;

%include "iw/matching/cbir/full/feature_index.h"

// TODO(kheath): change c++ source so that swig can wrap correctly directly from header instead of this tweak
%extend cbir::FeatureIndex {
  bool Search(const std::vector<unsigned long long>& ignore_image_ids_python, int k, bool include_keypoints, const iw::ImageFeatures& features, cbir::ImageFeaturesNeighbors* neighbors){
    std::set<uint64> ignore_image_ids;
    ignore_image_ids.insert(ignore_image_ids_python.begin(), ignore_image_ids_python.end());    
    return self->Search(ignore_image_ids, k, include_keypoints, features, neighbors);
  }
};

%include "iw/matching/cbir/full/scoring.h"



