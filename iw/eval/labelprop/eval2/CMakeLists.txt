CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROTO_LIBRARY(
  NAME      proto
  PROTO     eval2.proto
)

CPP_LIBRARY(
  NAME      eval2 
  SOURCES   eval2.cpp            
  HEADERS   eval2.h
  PACKAGES  //iw/eval/labelprop/eval2:proto
            //iw:labelprop
            //iw:imageregiongraph
            //tide:tide
            //iw:proto
            //snap/pert:pert            
  LIB_TYPE  STATIC_AND_SHARED
  SWIG_PY   py_eval2.i
)


CPP_BINARY(
  NAME      eval2_test 
  SOURCES   eval2_test.cpp            
  HEADERS 
  PACKAGES  //iw/eval/labelprop/eval2:eval2
            //snap/google/gflags:gflags
            //snap/google/glog:glog
)

PY_BINARY(
  NAME      create_synth_dataset
  SOURCES   create_synth_dataset.py
)

ADD_SUBDIRECTORY(viewer)
ADD_SUBDIRECTORY(tex_table)