CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})

CPP_LIBRARY(
  NAME       png
  SOURCES    png.c       pngget.c  pngpread.c  pngrio.c    pngrutil.c  pngtest.c   pngwio.c    pngwtran.c
             pngerror.c  pngmem.c  pngread.c   pngrtran.c  pngset.c    pngtrans.c  pngwrite.c  pngwutil.c     
  HEADERS    pngconf.h  pngdebug.h  png.h  pnginfo.h  pnglibconf.h  pngpriv.h  pngstruct.h  
  PACKAGES   //snap/zlib:zlib   
  LIB_TYPE   STATIC
)
