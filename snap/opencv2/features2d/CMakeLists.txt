CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_LIBRARY(
  NAME      features2d
  SOURCES   #src/bagofwords.cpp
            src/blobdetector.cpp
            src/brief.cpp
            src/descriptors.cpp
            src/detectors.cpp
            src/draw.cpp
            src/dynamic.cpp
            src/evaluation.cpp
            src/fast.cpp
            src/features2d_init.cpp
            src/freak.cpp
            src/keypoint.cpp
            src/matchers.cpp
            src/mser.cpp
            src/orb.cpp
            src/precomp.cpp
            src/stardetector.cpp            
  HEADERS   features2d.hpp
  PACKAGES  //snap/opencv2/imgproc:imgproc
            //snap/opencv2/flann:flann
  LIB_TYPE  STATIC
)
