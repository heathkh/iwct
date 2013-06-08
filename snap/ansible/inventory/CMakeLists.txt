CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

PY_LIBRARY(
  NAME      inventory
  SOURCES   expand_hosts.py  group.py  host.py  ini.py  __init__.py  script.py 
)

ADD_SUBDIRECTORY(vars_plugins)