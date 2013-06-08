CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

PY_LIBRARY(
  NAME      runner
  SOURCES   connection.py  __init__.py  poller.py  return_data.py  
)

ADD_SUBDIRECTORY(action_plugins)
ADD_SUBDIRECTORY(connection_plugins)
ADD_SUBDIRECTORY(filter_plugins)
ADD_SUBDIRECTORY(lookup_plugins)