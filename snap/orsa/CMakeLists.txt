CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

# This allows include paths to look like this: #include "orsa/..."
SET(ORSA_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..)
INCLUDE_DIRECTORIES(${ORSA_ROOT})

ADD_SUBDIRECTORY(numeric)
ADD_SUBDIRECTORY(test)

CPP_LIBRARY(
  NAME      orsa 
  SOURCES   conditioning.cpp
            homography_model.cpp
            fundamental_model.cpp
            orsa_model.cpp
  HEADERS   conditioning.hpp
            fundamental_model.hpp
            homography_model.hpp
            orsa_model.hpp
  PACKAGES  //snap/orsa/numeric:numeric
            //snap/Eigen:eigen
  LIB_TYPE  STATIC  
)




