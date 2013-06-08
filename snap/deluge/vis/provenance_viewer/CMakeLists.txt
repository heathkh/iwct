CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
 
PY_BINARY (
  NAME      viewer
  SOURCES   viewer.py                        
  DATA      resource.html
            main.html
            static
)

