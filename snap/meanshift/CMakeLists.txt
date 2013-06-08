CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_LIBRARY(
  NAME      meanshift
  HEADERS   meanshift.h
  SOURCES   meanshift.cpp
  PACKAGES  //snap/opencv2/core:core
            //snap/opencv2/flann:flann
            //snap/google/glog:glog
  LIB_TYPE  STATIC              
)

CPP_BINARY(
  NAME      meanshift_test
  SOURCES   meanshift_test.cpp   
  PACKAGES  //snap/meanshift:meanshift
            //snap/google/gtest:gtest             
            //snap/google/glog:glog     
  TEST_SIZE small            
)


