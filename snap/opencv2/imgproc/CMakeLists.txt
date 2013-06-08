CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_LIBRARY(
  NAME      imgproc  
  SOURCES   src/accum.cpp
            src/approx.cpp
            src/canny.cpp
            src/color.cpp
            src/contours.cpp
            src/convhull.cpp
            src/corner.cpp
            src/cornersubpix.cpp
            src/deriv.cpp
            src/distransform.cpp
            src/emd.cpp
            src/featureselect.cpp
            src/filter.cpp
            src/floodfill.cpp
            src/gabor.cpp
            src/geometry.cpp
            src/grabcut.cpp
            src/histogram.cpp
            src/hough.cpp
            src/imgwarp.cpp
            src/linefit.cpp
            src/matchcontours.cpp
            src/moments.cpp
            src/morph.cpp
            src/phasecorr.cpp
            src/precomp.cpp
            src/pyramids.cpp
            src/rotcalipers.cpp
            src/samplers.cpp
            src/segmentation.cpp
            src/shapedescr.cpp
            src/smooth.cpp
            src/subdivision2d.cpp
            src/sumpixels.cpp
            src/tables.cpp
            src/templmatch.cpp
            src/thresh.cpp
            src/undistort.cpp
            src/utils.cpp
  HEADERS   imgproc.hpp
  PACKAGES  //snap/opencv2/core:core
  LIB_TYPE  STATIC
)
