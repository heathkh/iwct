CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PY_LIBRARY(
  NAME      workstation
  SOURCES   workstation.py                                  
)

PY_BINARY(
  NAME      cli
  SOURCES   cli.py                                  
)

ADD_SUBDIRECTORY(templates)



