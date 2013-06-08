CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_LIBRARY(
  NAME      vgg  
  SOURCES   vgg_feature_extractor.cpp          
  HEADERS   vgg_feature_extractor.h            
  PACKAGES  //iw:proto
            //snap/google/base:base
            //snap/google/glog:glog            
            //snap/boost/filesystem:filesystem
            //snap/opencv2/highgui:highgui            
  LIB_TYPE  STATIC
)

ADD_SUBDIRECTORY(test)