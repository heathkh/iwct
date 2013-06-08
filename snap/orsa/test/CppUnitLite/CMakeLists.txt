CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_LIBRARY(
  NAME      cppunitlite 
  SOURCES   Failure.cpp  SimpleString.cpp  Test.cpp  TestRegistry.cpp
            TestResult.cpp
  HEADERS   Failure.h  SimpleString.h  Test.h  TestHarness.h  TestRegistry.h
            TestResult.h
  LIB_TYPE  STATIC
)




