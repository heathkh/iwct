CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_BINARY(
  NAME      test_scul
  SOURCES   test_scul.cpp   
  PACKAGES  
            //snap/google/glog:glog
            //snap/google/gtest:gtest
            //snap/scul:scul
  TEST_SIZE small                          
)


