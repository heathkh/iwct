CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

CPP_LIBRARY(
  NAME flann
  SOURCES  
    src/flann.cpp
    src/miniflann.cpp
    src/precomp.cpp    
  HEADERS 
    flann.hpp
    defines.h
  PACKAGES
    //snap/opencv2/core:core
  LIB_TYPE
    STATIC
)
