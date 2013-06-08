CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_LIBRARY(
  NAME      numeric 
  SOURCES   matrix.cpp  numerics.cpp  vector.cpp
  HEADERS   matrix.h  numerics.h  poly.h  svd.hpp
  LIB_TYPE  STATIC
)





