CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

REMOTE_RESOURCES(
  NAME testdata
  URL  graphics.stanford.edu/~heathkh/webscratch/cbir_test_small_v01.tar.gz
  MD5  6ae3113767658663007f7040fc433a9a
)


PY_BINARY(
  NAME      test_index
  SOURCES   test_index.py  
  PACKAGES  //iw/matching/cbir/bow/ctis:ctis
  TEST_SIZE small  
)