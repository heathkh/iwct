CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROTO_LIBRARY(
  NAME      proto
  PROTO     tide.proto  
)

PY_LIBRARY(
  NAME      tide
  SOURCES   tide.py                   
)

CPP_LIBRARY(
  NAME      tide
  HEADERS   tide.h
  SOURCES   tide.cpp
  PACKAGES  //snap/boost:boost
            //snap/pert:pert
            //tide:proto
  LIB_TYPE  STATIC                   
)


CPP_BINARY(
  NAME      tide_test
  SOURCES   tide_test.cpp   
  PACKAGES  //snap/google/gtest:gtest
            //snap/google/glog:glog
            //tide:tide
  TEST_SIZE small            
)

ADD_SUBDIRECTORY(util)
ADD_SUBDIRECTORY(viewer)
