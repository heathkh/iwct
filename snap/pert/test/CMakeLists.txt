CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
 
PROTO_LIBRARY(
  NAME      proto
  PROTO     test.proto
)


CPP_BINARY(
  NAME      test_chunk
  SOURCES   test_chunk.cpp   
  PACKAGES  //snap/pert:pert
            //snap/google/gtest:gtest             
  TEST_SIZE small            
)

CPP_BINARY(
  NAME      test_ufs
  SOURCES   test_ufs.cpp   
  PACKAGES  //snap/pert:pert
            //snap/google/glog:glog
            //snap/google/gtest:gtest                    
  TEST_SIZE large # large because it requires maprfs            
)

CPP_BINARY(
  NAME      test_basic  
  SOURCES   test_basic.cpp   
  PACKAGES  //snap/pert:pert
            //snap/google/gtest:gtest
            //snap/boost/timer:timer
            //snap/pert/test:proto  
  TEST_SIZE small            
)

CPP_BINARY(
  NAME      test_utils  
  SOURCES   test_utils.cpp   
  PACKAGES  //snap/pert:pert
            //snap/google/gtest:gtest            
  TEST_SIZE small            
)


CPP_BINARY(
  NAME      test_bloom 
  SOURCES   test_bloom.cpp  
  PACKAGES  //snap/google/gtest:gtest
            //snap/pert:pert
  TEST_SIZE small        
)

PY_BINARY(
  NAME      testpython
  SOURCES   test.py
  PACKAGES  //snap/pert:py_pert
  TEST_SIZE small  # can't use as a unit test because it depends on maprfs cluster being available         
)


#PY_BINARY(
#  NAME      test_ufs
#  SOURCES   test_ufs.py
#  PACKAGES  //snap/pert:py_pert
#  TEST_SIZE large  # can't use as a unit test because it depends on maprfs cluster being available       
#)

