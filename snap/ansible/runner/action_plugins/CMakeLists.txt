CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

PY_LIBRARY(
  NAME      action_plugins
  SOURCES   add_host.py  copy.py   fail.py   group_by.py  normal.py  raw.py     template.py
            async.py     debug.py  fetch.py  __init__.py  pause.py   script.py
)

