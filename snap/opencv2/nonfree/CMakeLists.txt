CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_LIBRARY(
  NAME      nonfree
  SOURCES   src/nonfree_init.cpp
            src/precomp.cpp
            src/sift.cpp
            src/surf.cpp    
  HEADERS   nonfree.hpp
  PACKAGES  //snap/opencv2/core:core
            //snap/opencv2/features2d:features2d
            //snap/zlib:zlib
  LIB_TYPE
    STATIC
)
