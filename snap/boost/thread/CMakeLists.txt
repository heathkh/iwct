CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

SET(SRC_BASE "${CMAKE_CURRENT_SOURCE_DIR}/../libs/thread/src/")

CPP_LIBRARY (
  NAME      thread  
  SOURCES   ${SRC_BASE}/future.cpp
            ${SRC_BASE}/tss_null.cpp
            ${SRC_BASE}/pthread/thread.cpp
            ${SRC_BASE}/pthread/once.cpp
                                      
  HEADERS   
  PACKAGES  //snap/boost/system:system
            SYS://rt
            SYS://threads
  LIB_TYPE  STATIC           
)


