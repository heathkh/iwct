CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_BINARY(
  NAME      test_taskqueue  
  SOURCES   test_taskqueue.cpp   
  PACKAGES  //snap/google/gtest:gtest            
            //snap/google/glog:glog        
            //snap/boost/thread:thread   
            SYS://threads  
  TEST_SIZE small                          
)

CPP_BINARY(
  NAME      test_stacktrace
  SOURCES   test_stacktrace.cpp   
  PACKAGES  //snap/google/glog:glog                                                
)