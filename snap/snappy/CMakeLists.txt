CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_LIBRARY(
  NAME snappy 
  SOURCES   snappy.cc snappy-sinksource.cc snappy-c.cc  snappy-stubs-internal.cc
  HEADERS   snappy-c.h  snappy.h  snappy-internal.h  snappy-sinksource.h  
            snappy-stubs-internal.h  snappy-stubs-public.h
  LIB_TYPE  STATIC
)



