CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

ADD_SUBDIRECTORY(CppUnitLite)

CPP_BINARY(
  NAME      homography_model_test 
  SOURCES   homography_model_test.cpp            
  HEADERS
  PACKAGES  //snap/orsa/test/CppUnitLite:cppunitlite
            //snap/orsa:orsa
)

CPP_BINARY(
  NAME      homography_orsa_test 
  SOURCES   homography_orsa_test.cpp            
  HEADERS
  PACKAGES  //snap/orsa/test/CppUnitLite:cppunitlite
            //snap/orsa:orsa
)






