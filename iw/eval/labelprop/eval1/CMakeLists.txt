CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROTO_LIBRARY(
  NAME      proto
  PROTO     eval1.proto
)

CPP_LIBRARY(
  NAME      eval1 
  SOURCES   eval1.cpp            
  HEADERS   eval1.h
  PACKAGES  //iw:labelprop
            //iw/eval/labelprop/eval1:proto
            //iw:proto              
            //tide:tide         
            //iw:imagegraph
            //iw:labelprop       
            //snap/pert:pert            
  LIB_TYPE  STATIC_AND_SHARED
  SWIG_PY   py_eval1.i                           
)


CPP_BINARY(
  NAME      eval1_test 
  SOURCES   eval1_test.cpp            
  HEADERS 
  PACKAGES  //iw/eval/labelprop/eval1:eval1
            //snap/google/gflags:gflags
            //snap/google/glog:glog
)


ADD_SUBDIRECTORY(viewer)