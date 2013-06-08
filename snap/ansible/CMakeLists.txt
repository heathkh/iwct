CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PY_LIBRARY(
  NAME      ansible
  SOURCES   __init__.py
            callbacks.py  
            color.py  
            constants.py  
            errors.py              
            module_common.py
)

ADD_SUBDIRECTORY(callback_plugins)
ADD_SUBDIRECTORY(utils)
ADD_SUBDIRECTORY(inventory)
ADD_SUBDIRECTORY(playbook)
ADD_SUBDIRECTORY(library)
ADD_SUBDIRECTORY(runner)