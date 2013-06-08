CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_BINARY(
  NAME      test_progress
  SOURCES   test_progress.cpp   
  PACKAGES  //snap/google/glog:glog       
            //snap/progress:progress              
  TEST_SIZE small           
)
