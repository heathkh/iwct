CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROTO_LIBRARY(
  NAME      proto
  PROTO     eval3.proto
)

CPP_LIBRARY(
  NAME      eval3 
  SOURCES   eval3.cpp            
  HEADERS   eval3.h
  PACKAGES  //iw/eval/labelprop/eval3:proto
            //iw:labelprop
            //iw:imageregiongraph
            //tide:tide
            //iw:proto
            //snap/pert:pert                  
  LIB_TYPE  STATIC_AND_SHARED
  #SWIG_PY   py_eval3.i
)

CPP_BINARY(
  NAME      eval3_test 
  SOURCES   eval3_test.cpp            
  HEADERS 
  PACKAGES  //iw/eval/labelprop/eval3:eval3
            //snap/google/gflags:gflags
            //snap/google/glog:glog
)
