CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

#add this to allow shorthand notation for headers like #include <Eigen/Core>
SET(EIGEN_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..)
INCLUDE_DIRECTORIES(${EIGEN_ROOT})

CPP_LIBRARY(
  NAME      eigen 
  SOURCES   
  HEADERS   
    Array           Eigenvalues             PardisoSupport   StdList
    Cholesky        Geometry                PaStiXSupport    StdVector
    CholmodSupport  Householder             QR               SuperLUSupport
    CMakeLists.txt  IterativeLinearSolvers  QtAlignedMalloc  SVD
    Core            Jacobi                  Sparse           UmfPackSupport
    Dense           LeastSquares            SparseCholesky
    Eigen           LU                      SparseCore
    Eigen2Support   OrderingMethods         StdDeque
  LIB_TYPE  HEADER
)
            