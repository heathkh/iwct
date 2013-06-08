CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

# Enable support for available distributed filesystem types
SET(OPTIONAL_DFS_PACKAGES)

# Check for MaprFS
FIND_PACKAGE("SYS-maprhdfs" QUIET)
IF(SYS-maprhdfs_FOUND)  
  ADD_DEFINITIONS(-DPERT_USE_MAPR_FS)
  LIST(APPEND OPTIONAL_DFS_PACKAGES "SYS://maprhdfs") 
ENDIF()

# Check for hdfs
FIND_PACKAGE("SYS-hdfs" QUIET)
IF(SYS-hdfs_FOUND)  
  ADD_DEFINITIONS(-DPERT_USE_HDFS)
  LIST(APPEND OPTIONAL_DFS_PACKAGES "SYS://hdfs") 
ENDIF()

CPP_LIBRARY(
  NAME      core  
  SOURCES   bloom.cpp
            comparator.cpp
            compression.cpp
            cbfile.cpp
            ufs.cpp      
            fs_local.cpp
            fs_mapr.cpp
            md5.cpp
            partitioner.cpp
  HEADERS   bloom.h
            common.h 
            comparator.cpp
            compression.cpp
            cbfile.h  
            ufs.h    
            fs_base.h
            fs_local.h
            fs_mapr.h
            md5.h
            partitioner.h
  PACKAGES  SYS://protobuf    
            //snap/google/glog:glog      
            //snap/snappy:snappy           
            //snap/boost/regex:regex
            //snap/boost/filesystem:filesystem
            //snap/google/base:base
            ${OPTIONAL_DFS_PACKAGES}                                                                
  LIB_TYPE  STATIC           
)
