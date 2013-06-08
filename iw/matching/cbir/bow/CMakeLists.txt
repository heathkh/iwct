CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROTO_LIBRARY(
  NAME      proto
  PROTO     bow.proto
)

CPP_LIBRARY(
  NAME      bow
  SOURCES   quantizer.cpp
            observation_accumulator.cpp
            util.cpp
  HEADERS   index.h
            quantizer.h
            observation_accumulator.h
            util.h
  PACKAGES  //snap/google/base:base
            //snap/google/glog:glog                            
            //snap/pert:pert
            //iw:proto
            //iw/matching/cbir/bow:proto
            //iw/matching/cbir:proto
  LIB_TYPE  STATIC_AND_SHARED
  SWIG_PY   py_bow.i
)

PY_LIBRARY(
  NAME     test_util
  SOURCES  test_util.py
  PACKAGES //iw/matching/cbir/bow:bow
)

ADD_SUBDIRECTORY(ctis)
ADD_SUBDIRECTORY(test)
