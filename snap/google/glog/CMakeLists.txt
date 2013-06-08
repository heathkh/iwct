CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

ADD_DEFINITIONS(-D_GNU_SOURCE)
#ADD_DEFINITIONS(-DHAVE_STACKTRACE)
ADD_DEFINITIONS(-DHAVE_LIB_UNWIND)
ADD_DEFINITIONS(-Wno-sign-compare)
ADD_DEFINITIONS(-Wl,--eh-frame-hdr) # required for static linking -- see note in glog install instructions

CPP_LIBRARY(
  NAME      glog 
  SOURCES   demangle.cc
            raw_logging.cc
            symbolize.cc
            vlog_is_on.cc
            logging.cc
            signalhandler.cc
            utilities.cc  
  HEADERS   config.h
            demangle.h
            stacktrace.h
            stl_logging.h
            logging.h
            symbolize.h
            log_severity.h
            utilities.h
            raw_logging.h
            vlog_is_on.h
  PACKAGES  //snap/google/base:base
            //snap/google/gflags:gflags
            SYS://threads
            SYS://libunwind
  LIB_TYPE  STATIC
)
