CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

REMOTE_RESOURCES(
  NAME testdata
  URL  graphics.stanford.edu/~heathkh/webscratch/acransac_test_data.tar.gz
  MD5  dcf043e1a7df02eb7a636b79bc8577e7
)

CPP_BINARY (
  NAME      acransac_test
  SOURCES   acransac_test.cpp  
  PACKAGES  //iw/matching:imagematcher
            //iw/matching:featureextractor
            //iw/matching:visutil 
            //snap/zlib:zlib             
)




