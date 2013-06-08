CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_LIBRARY(
  NAME      ctis  
  SOURCES   index.cpp      
  HEADERS   index.h      
  PACKAGES  //iw/matching/cbir/bow:bow 
            //snap/ctis:ctis
            //snap/flann:flann
  LIB_TYPE  STATIC_AND_SHARED
  SWIG_PY   py_ctis.i                             
)

ADD_SUBDIRECTORY(test)


