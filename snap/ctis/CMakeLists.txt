cmake_minimum_required(VERSION 2.8)

#TODO(kheath): fix code to compile cleanly without disabling these warnings
ADD_DEFINITIONS(-Wno-unused-variable)
ADD_DEFINITIONS(-Wno-sign-compare)
ADD_DEFINITIONS(-Wno-reorder)

CPP_LIBRARY(
  NAME      ctis
  SOURCES   ccDistance.cpp
            ccHKmeans.cpp
            ccInvertedFile.cpp
            ccKdt.cpp
            #ccLsh.cpp
            ccNormalize.cpp
  HEADERS   ccCommon.hpp
            ccData.hpp
            ccDistance.hpp
            ccHKmeans.hpp
            ccInvertedFile.hpp
            ccKdt.hpp
            #ccLsh.hpp
            ccMatrix.hpp
            ccNormalize.hpp
            ccVector.hpp
  PACKAGES
  LIB_TYPE  STATIC       
)
