CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

#Hack... DATA doesn't work with subdirs and LOCAL_RESOURCES makes no package target (for doc) 
PY_LIBRARY (
  NAME      js    
  SOURCES   d3.legend.js
            d3.v3.js
            d3.v3.min.js         
)

