CMAKE_MINIMUM_REQUIRED(VERSION 2.6)
 
CPP_BINARY (
  NAME      test  
  SOURCES   test.cpp
  PACKAGES  //snap/deluge:deluge
)