CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROTO_LIBRARY(
  NAME      proto
  PROTO     iw.proto
)

CPP_LIBRARY(
  NAME      imagegraph
  SOURCES   imagegraph.cpp
  HEADERS   imagegraph.h            
  PACKAGES  //snap/google/base:base
            //snap/google/glog:glog
            //snap/scul:scul
            //snap/pert:pert
            //iw:proto          
            //iw:util
            //tide:tide
  LIB_TYPE  STATIC_AND_SHARED
  SWIG_PY    imagegraph.i   
)

CPP_LIBRARY(
  NAME      imageregiongraph
  SOURCES   imageregiongraph.cpp
  HEADERS   imageregiongraph.h            
  PACKAGES  //snap/google/base:base
            //snap/google/glog:glog
            //snap/scul:scul
            //snap/pert:pert
            //snap/meanshift:meanshift
            //iw:proto          
            //iw:util
            //iw:imagegraph
            //tide:tide
  LIB_TYPE  STATIC_AND_SHARED
  SWIG_PY   imageregiongraph.i   
)


CPP_LIBRARY(
  NAME      labelprop
  SOURCES   labelprop.cpp          
  HEADERS   labelprop.h          
  PACKAGES  //snap/google/base:base
            //snap/google/glog:glog           
            //iw:proto           
  LIB_TYPE  STATIC
)

CPP_LIBRARY(
  NAME      util
  SOURCES   util.cpp
  HEADERS   util.h            
  PACKAGES  //snap/google/base:base
            //snap/google/glog:glog
            //iw:proto
  LIB_TYPE  STATIC  
)

PY_LIBRARY(
  NAME      util
  SOURCES   util.py                   
)


ADD_SUBDIRECTORY(apps)
ADD_SUBDIRECTORY(eval)
ADD_SUBDIRECTORY(flow)
ADD_SUBDIRECTORY(matching)
ADD_SUBDIRECTORY(vis)
ADD_SUBDIRECTORY(test)
