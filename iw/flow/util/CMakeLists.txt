CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PY_LIBRARY (
  NAME      util
  SOURCES   util.py
            image_matcher_config.py    
)

CPP_BINARY(
  NAME      mr_extract_features  
  SOURCES   mr_extract_features.cpp  
  PACKAGES  //snap/deluge:deluge    
            //iw:proto
            //iw/matching:featureextractor     
)

CPP_BINARY(
  NAME      mr_find_duplicates  
  SOURCES   mr_find_duplicates.cpp  
  PACKAGES  //snap/deluge:deluge
)

CPP_BINARY(
  NAME      mr_cbir_count_features  
  SOURCES   mr_cbir_count_features.cpp  
  PACKAGES  //snap/deluge:deluge                
            //iw/matching/cbir:proto
            //iw:proto
            //iw/matching/cbir/full:proto
)

CPP_BINARY(
  NAME      mr_cbir_create_index_shards  
  SOURCES   mr_cbir_create_index_shards.cpp  
  PACKAGES  //snap/deluge:deluge                
            //iw/matching/cbir/full:full                   
)

CPP_BINARY(
  NAME      mr_cbir_query_index_shard  
  SOURCES   mr_cbir_query_index_shard.cpp  
  PACKAGES  //snap/deluge:deluge                
            //iw/matching/cbir/full:full
)

CPP_BINARY(
  NAME      mr_cbir_merge_query_results  
  SOURCES   mr_cbir_merge_query_results.cpp  
  PACKAGES  //snap/deluge:deluge                
            //iw/matching/cbir/full:full
)


CPP_BINARY(
  NAME      mr_cbir_score_results    
  SOURCES   mr_cbir_score_results.cpp  
  PACKAGES  //snap/deluge:deluge                
            //iw/matching/cbir/full:full
            //iw/matching/cbir:proto                      
            //iw:util
)

CPP_BINARY(
  NAME      mr_create_unique_match_candidates  
  SOURCES   mr_create_unique_match_candidates.cpp  
  PACKAGES  //snap/deluge:deluge
            //iw/matching/cbir/full:full
            //iw:util       
            //iw:proto     
)

CPP_BINARY(
  NAME      mr_sort_match_candidates  
  SOURCES   mr_sort_match_candidates.cpp  
  PACKAGES  //snap/deluge:deluge       
            //iw:proto     
)

CPP_BINARY (
  NAME      mr_match_batches
  SOURCES   mr_match_batches.cpp  
  PACKAGES  //snap/deluge:deluge                                  
            //iw/matching:imagematcher        
            //snap/boost/timer:timer                 
)

CPP_BINARY (
  NAME mr_merge_matches
  SOURCES mr_merge_matches.cpp  
  PACKAGES //snap/deluge:deluge                       
           //iw:proto          
)

CPP_BINARY (
  NAME      mr_sort
  SOURCES   mr_sort.cpp  
  PACKAGES  //snap/deluge:deluge                                        
)

CPP_BINARY (
  NAME      mr_drop_value
  SOURCES   mr_drop_value.cpp  
  PACKAGES  //snap/deluge:deluge                                        
)

CPP_BINARY(
  NAME      mr_create_bag_of_words  
  SOURCES   mr_create_bag_of_words.cpp  
  PACKAGES  //snap/deluge:deluge    
            //iw:proto
            //iw/matching/cbir/bow:bow               
            //snap/boost/timer:timer   
)

CPP_BINARY(
  NAME      mr_cbir_bow_query_index
  SOURCES   mr_cbir_bow_query_index.cpp  
  PACKAGES  //snap/deluge:deluge                       
            //iw:proto
            #//iw/matching/cbir/bow/inria:inria
            //iw/matching/cbir/bow/ctis:ctis                            
            //snap/boost/timer:timer   
)
