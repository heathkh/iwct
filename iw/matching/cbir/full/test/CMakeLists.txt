CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

  
#PY_BINARY(
#  NAME      test_smooth_filter
#  SOURCES   test_smooth_filter.py    
#  PACKAGES  //iw/matching:py_featureextractor
#  DATA      ${IMAGES}    
#  TEST_SIZE small
#)


#PY_BINARY(
#  NAME    test_wgc_filter
#  SOURCES test_wgc_filter.py    
#  DATA    ${IMAGES}
#  TEST_SIZE small    
#)

#PY_BINARY(
#  NAME    test_transform
#  SOURCES test_transform.py
#  TEST_SIZE small    
#)

#PY_BINARY(
#  NAME    test_merge
#  SOURCES test_merge.py
#  TEST_SIZE large  # can't use as small test because it depends on local file system input data    
#)

REMOTE_RESOURCES(
  NAME testdata
  URL  graphics.stanford.edu/~heathkh/webscratch/features.pert.tar.gz
  MD5  34503c986225835fd7135b6fa2947222
)


CPP_BINARY(
  NAME       test_concurrent_query  
  SOURCES    test_concurrent_query.cpp   
  PACKAGES   //iw/matching/cbir/full:full
             //snap/google/gtest:gtest           
             //snap/google/base:base            
             //snap/pert:pert             
  TEST_SIZE small               
)


