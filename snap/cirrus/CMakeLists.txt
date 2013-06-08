CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PY_LIBRARY(
  NAME      cirrus
  SOURCES   util.py                                             
)

ADD_SUBDIRECTORY(ami)
#ADD_SUBDIRECTORY(cluster)
ADD_SUBDIRECTORY(workstation)


