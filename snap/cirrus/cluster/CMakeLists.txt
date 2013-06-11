CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PY_LIBRARY(
  NAME      cluster
  SOURCES   ec2cluster.py                               
            mapr.py      
            config.py                            
)

ADD_SUBDIRECTORY(playbooks)
