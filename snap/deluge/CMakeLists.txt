CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
ADD_SUBDIRECTORY(pipes)

PROTO_LIBRARY(
  NAME      proto
  PROTO     deluge.proto
)

CPP_LIBRARY (
  NAME      deluge
  SOURCES   debug.cpp
            job.cpp
            join.cpp
            pertio.cpp
            std.cpp
            util.cpp            
  HEADERS   debug.h
            job.h
            join.h
            pertio.h
            std.h
            util.h    
  PACKAGES  //snap/boost/regex:regex
            //snap/boost/timer:timer
            //snap/boost/system:system
            //snap/google/base:base       
            //snap/google/glog:glog
            //snap/boost/thread:thread     
            //snap/base64:base64
            //snap/pert:pert
            //snap/deluge/pipes:pipes           
            //snap/deluge:proto                         
            SYS://gperftools                        
  LIB_TYPE  STATIC          
)

SET(CMAKE_JAVA_INCLUDE_PATH "/opt/mapr/hadoop/hadoop-0.20.2/lib/hadoop-0.20.2-dev-core.jar")

#JAVA_LIBRARY (
#  NAME      pert
#  SOURCES   MyReducerPrefixGroupingComparator.java
#)


PY_LIBRARY (
  NAME      deluge  
  SOURCES   core.py 
            mr.py
            notification.py
            runner.py
            scheduler.py
            lebesgueset.py
            s3dict.py             
            provenance.py
)

PY_BINARY (
  NAME      s3dict_test  
  SOURCES   s3dict_test.py   
)

#ADD_SUBDIRECTORY(test)

