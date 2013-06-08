CMAKE_MINIMUM_REQUIRED(VERSION 2.6)


CPP_BINARY (
  NAME      acransac_correspondences_test
  SOURCES   acransac_correspondences_test.cpp  
  PACKAGES  //iw/matching:imagematcher
            //iw/matching:featureextractor
            //iw/matching:visutil            
  DATA      01a.jpg
            01b.jpg
            02a.jpg
            02b.jpg
            03a.jpg
            03b.jpg
            04a.jpg
            04b.jpg
            05a.jpg
            05b.jpg
            06a.jpg
            06b.jpg
            07a.jpg
            07b.jpg
            08a.jpg
            08b.jpg
            09a.jpg
            09b.jpg
            10a.jpg
            10b.jpg
)


