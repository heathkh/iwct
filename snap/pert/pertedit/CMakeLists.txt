CMAKE_MINIMUM_REQUIRED(VERSION 2.6)


CPP_BINARY(
  NAME 
    pertedit
  SOURCES
    pertedit.cpp  
  PACKAGES
    //snap/pert:pert
    //snap/google/gflags:gflags
    //snap/google/base:base
)

