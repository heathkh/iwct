# matching - contains all code related to the matching of local image features
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

ADD_SUBDIRECTORY(vgg)

CPP_LIBRARY(
  NAME      imagematcher
  SOURCES   image_matcher.cpp
            feature_matcher.cpp
            acransac_image_matcher.cpp
            acransac_correspondences.cpp      
            acransac_models.cpp                                       
  HEADERS   image_matcher.h
            feature_matcher.h
            acransac_image_matcher.h    
            acransac_correspondences.h
            acransac_models.h                   
  PACKAGES  //snap/google/glog:glog
            //snap/flann:flann
            //snap/orsa:orsa
            //snap/opencv2/core:core
            //iw:proto            
  LIB_TYPE  STATIC
)

CPP_LIBRARY(
  NAME      featureextractor  
  SOURCES   feature_extractor.cpp  
  HEADERS   feature_extractor.h
  PACKAGES  //snap/google/glog:glog
            //iw:proto
            //iw/matching/vgg:vgg
            //snap/opencv2/nonfree:nonfree        
            //snap/zlib:zlib
  LIB_TYPE  STATIC_AND_SHARED  
  SWIG_PY   py_featureextractor.i              
)

CPP_LIBRARY(
  NAME      point2d_index
  SOURCES   point2d_index.cpp                                       
  HEADERS   point2d_index.h                              
  PACKAGES  //snap/google/glog:glog
            //snap/flann:flann                        
  LIB_TYPE  STATIC
)

CPP_LIBRARY(
  NAME      visutil  
  SOURCES   visutil.cpp  
  HEADERS   visutil.h
  PACKAGES  //snap/google/glog:glog
            //iw/matching:imagematcher
            //snap/base64:base64
            //snap/opencv2/highgui:highgui
  LIB_TYPE  STATIC          
)

PY_LIBRARY(
  NAME    feature_extractor_density
  SOURCES feature_extractor_density.py
)

ADD_SUBDIRECTORY(cbir)
ADD_SUBDIRECTORY(test)