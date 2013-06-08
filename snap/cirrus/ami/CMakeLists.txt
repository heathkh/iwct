CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PY_LIBRARY(
  NAME      manager
  SOURCES   manager.py                                      
)

PY_BINARY(
  NAME      ami_cli
  SOURCES   ami_cli.py
)

ADD_SUBDIRECTORY(templates)