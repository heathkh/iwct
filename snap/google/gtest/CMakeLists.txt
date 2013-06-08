CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

#INCLUDE_DIRECTORIES(${GOOGLE_ROOT}/gtest/include ${GOOGLE_ROOT}/gtest/ )

CPP_LIBRARY(
  NAME      gtest 
  SOURCES 
            src/gtest-all.cc
            src/gtest.cc
            src/gtest-death-test.cc
            src/gtest-filepath.cc
            src/gtest_main.cc
            src/gtest-port.cc
            src/gtest-printers.cc
            src/gtest-test-part.cc
            src/gtest-typed-test.cc
  HEADERS
            gtest-death-test.h
            gtest.h
            gtest-message.h
            gtest-param-test.h
            gtest_pred_impl.h
            gtest-printers.h
            gtest_prod.h
            gtest-spi.h
            gtest-test-part.h
            gtest-typed-test.h
  PACKAGES  SYS://threads            
  LIB_TYPE  STATIC
)
