CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_LIBRARY(
  NAME      core  
  SOURCES   src/algorithm.cpp
            src/alloc.cpp
            src/arithm.cpp
            src/array.cpp
            src/cmdparser.cpp
            src/convert.cpp
            src/copy.cpp
            src/datastructs.cpp
            src/drawing.cpp
            src/dxt.cpp
            src/gpumat.cpp
            src/lapack.cpp
            src/mathfuncs.cpp
            src/matmul.cpp
            src/matop.cpp
            src/matrix.cpp
            src/opengl_interop.cpp
            src/out.cpp
            src/persistence.cpp
            src/precomp.cpp
            src/rand.cpp
            src/stat.cpp
            src/system.cpp
            src/tables.cpp
  HEADERS   core.hpp
  PACKAGES  SYS://rt    
  LIB_TYPE  STATIC
)
