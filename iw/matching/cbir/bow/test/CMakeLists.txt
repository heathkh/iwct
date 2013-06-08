CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_BINARY(
  NAME     test_util
  SOURCES  test_util.cpp
  PACKAGES //iw/matching/cbir/bow:bow
           //snap/google/gtest:gtest
  TEST_SIZE small
)