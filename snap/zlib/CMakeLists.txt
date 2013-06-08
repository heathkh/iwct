CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
ADD_DEFINITIONS(-DZ_PREFIX) # prefix all symbols with z_ so they don't collide with system zlib if it exists

CPP_LIBRARY(
  NAME      zlib
  SOURCES   adler32.c   crc32.c    gzclose.c  gzread.c   infback.c  inflate.c   
            trees.c compress.c  deflate.c  gzlib.c    gzwrite.c  inffast.c  
            inftrees.c  uncompr.c  
  HEADERS   crc32.h    gzguts.h   inffixed.h  inftrees.h  zconf.h  zutil.h  
            deflate.h  inffast.h  inflate.h   trees.h     zlib.h  
  PACKAGES      
  LIB_TYPE  STATIC
)
