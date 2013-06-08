CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

SET(SRC_BASE "../libs/filesystem/src/")

CPP_LIBRARY (
  NAME      filesystem  
  SOURCES   ${SRC_BASE}/codecvt_error_category.cpp
            ${SRC_BASE}/operations.cpp
            ${SRC_BASE}/path.cpp
            ${SRC_BASE}/path_traits.cpp
            ${SRC_BASE}/portability.cpp
            ${SRC_BASE}/unique_path.cpp
            ${SRC_BASE}/utf8_codecvt_facet.cpp
            ${SRC_BASE}/windows_file_codecvt.cpp
            ${SRC_BASE}/windows_file_codecvt.hpp   
  HEADERS   config.hpp
            convenience.hpp
            exception.hpp
            fstream.hpp
            operations.hpp
            path_traits.hpp
            path.hpp
  PACKAGES  //snap/boost/system:system
  LIB_TYPE  STATIC           
)


