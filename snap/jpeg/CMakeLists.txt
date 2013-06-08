CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

#INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})

CPP_LIBRARY(
  NAME      jpeg
  SOURCES   jcapimin.c  jcmarker.c  jdapimin.c  jdinput.c   jdtrans.c   jidctred.c
            jcapistd.c  jcmaster.c  jdapistd.c  jdmainct.c  jerror.c    jmemansi.c
            jccoefct.c  jcomapi.c   jdatadst.c  jdmarker.c  jfdctflt.c  jmemmgr.c
            jccolor.c   jcparam.c   jdatasrc.c  jdmaster.c  jfdctfst.c  jquant1.c
            jcdctmgr.c  jcphuff.c   jdcoefct.c  jdmerge.c   jfdctint.c  jquant2.c
            jchuff.c    jcprepct.c  jdcolor.c   jdphuff.c   jidctflt.c  jutils.c
            jcinit.c    jcsample.c  jddctmgr.c  jdpostct.c  jidctfst.c  transupp.c
            jcmainct.c  jctrans.c   jdhuff.c    jdsample.c  jidctint.c
  
  HEADERS   jchuff.h   jdct.h    jerror.h    jmemsys.h   jpegint.h  jversion.h
            jconfig.h  jdhuff.h  jinclude.h  jmorecfg.h  jpeglib.h  transupp.h  
  PACKAGES  
  LIB_TYPE  STATIC
)
