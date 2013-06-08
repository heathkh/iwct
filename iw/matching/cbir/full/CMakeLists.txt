CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROTO_LIBRARY(
  NAME     proto
  PROTO    full.proto
)

CPP_LIBRARY(
  NAME      full
  SOURCES   feature_index.cpp
            scoring.cpp
  HEADERS   feature_index.h
            scoring.h
  PACKAGES  //iw/matching/cbir/full:proto
            //iw/matching/cbir:proto
            //iw:proto
            //iw/matching:point2d_index            
            //snap/google/base:base
            //snap/google/glog:glog      
            //snap/pert:pert
            //snap/boost/timer:timer
            //snap/boost/thread:thread            
  LIB_TYPE  STATIC_AND_SHARED
  SWIG_PY   py_full.i                                       
)

PY_LIBRARY(
  NAME      util
  SOURCES   util.py
)

ADD_SUBDIRECTORY(test)

