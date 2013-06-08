CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

CPP_LIBRARY (
  NAME      pipes
  SOURCES   HadoopPipes.cc  SerialUtils.cc  StringUtils.cc  
  HEADERS   Pipes.hh  SerialUtils.hh  StringUtils.hh  TemplateFactory.hh    
  PACKAGES                                  
  LIB_TYPE  STATIC          
)
