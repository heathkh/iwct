CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PY_LIBRARY(
  NAME      ec2cluster
  SOURCES   ec2cluster.py                               
            mapr.py                                  
)

ADD_SUBDIRECTORY(scripts)
