CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_BINARY(
  NAME      mr_mapr_distributed_cache
  SOURCES   mr_mapr_distributed_cache.cpp  
  PACKAGES  //snap/deluge:deluge 
            //snap/pert:pert                         
)

PY_BINARY (
  NAME      mapr_distributed_cache
  SOURCES   mapr_distributed_cache.py
  PACKAGES  //snap/deluge:deluge      
)

#CPP_BINARY(
#  NAME      mr_distributed_cache
#  SOURCES   mr_distributed_cache.cpp  
#  PACKAGES  //snap/deluge:deluge 
#            //snap/pert:pert               
#)

#PY_BINARY (
#  NAME      distributed_cache
#  SOURCES   distributed_cache.py
#  PACKAGES  //snap/deluge:deluge      
#)

PY_BINARY (
  NAME      parallel_test
  SOURCES   parallel_test.py
            parallel_1.py
            parallel_2.py
            
  PACKAGES  //snap/deluge:deluge      
)


PY_BINARY (
  NAME      provenance_test
  SOURCES   provenance_test.py     
)


#JAVA_BINARY (
#  NAME      test_pert_support  
#  SOURCES   TestPertSupport.java
#  MAIN_CLASS TestPertSupport
#)


CPP_BINARY (
  NAME 
    test  
  SOURCES 
    test.cpp  
  PACKAGES
    //snap/google/gtest:gtest
    //snap/deluge:deluge        
)

add_subdirectory(utils)
add_subdirectory(mapjoin)