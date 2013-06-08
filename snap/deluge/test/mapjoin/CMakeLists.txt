CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

 
CPP_BINARY (
  NAME init
  SOURCES init.cpp
  PACKAGES //snap/pert:pert                     
)

CPP_BINARY (
  NAME reshard
  SOURCES reshard.cpp
  PACKAGES //snap/deluge:deluge         
)

CPP_BINARY (
  NAME innerjoin
  SOURCES innerjoin.cpp
  PACKAGES //snap/deluge:deluge                      
)