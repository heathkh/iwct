CMAKE_MINIMUM_REQUIRED(VERSION 2.6)


CPP_LIBRARY(
  NAME      base64 
  SOURCES   cdecode.cpp
            cencode.cpp 
  HEADERS   cdecode.h
            cencode.h
            decode.h
            encode.h
  LIB_TYPE  STATIC
)



