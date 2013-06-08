CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

SET(BOOST_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..)
INCLUDE_DIRECTORIES(${BOOST_ROOT})

ADD_SUBDIRECTORY(system)
ADD_SUBDIRECTORY(filesystem)
ADD_SUBDIRECTORY(regex)
ADD_SUBDIRECTORY(chrono)
ADD_SUBDIRECTORY(thread)
ADD_SUBDIRECTORY(timer)

ADD_DEFINITIONS(-Wno-unused-variable)

CPP_LIBRARY(
  NAME      boost
  HEADERS   foreach.hpp
            #TODO(kheath): list other headers here... not stricktly required since this is a header only lib and the action is achieved simply by modifying the include root to allow references to #include <boost/thingy.hpp>
  LIB_TYPE  HEADER              
)

