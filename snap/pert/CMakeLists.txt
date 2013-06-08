CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

ADD_SUBDIRECTORY(core)

PROTO_LIBRARY(
  NAME      proto
  PROTO     pert.proto
)

CPP_LIBRARY(
  NAME      pert 
  SOURCES   stringtable.cpp
            prototable.cpp        
            utils.cpp 
  HEADERS   stringtable.h
            prototable.h
            utils.h            
  PACKAGES  //snap/pert:proto                         
            //snap/pert/core:core      
            //snap/google/base:base
  LIB_TYPE  STATIC_AND_SHARED              
  SWIG_PY   py_pert.i  
)

ADD_SUBDIRECTORY(test)
ADD_SUBDIRECTORY(pertview)
ADD_SUBDIRECTORY(pertedit)
ADD_SUBDIRECTORY(pertsort)