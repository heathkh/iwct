ADD_SUBDIRECTORY(bow)
ADD_SUBDIRECTORY(full)

PROTO_LIBRARY(
  NAME     proto
  PROTO    cbir.proto
  PACKAGES //iw/matching/cbir/bow:proto
           //iw/matching/cbir/full:proto    
)

