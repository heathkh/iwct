CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

PY_LIBRARY(
  NAME      lookup_plugins
  SOURCES   dnstxt.py  fileglob.py  __init__.py  lines.py  redis_kv.py  template.py
            env.py     file.py      items.py     pipe.py   sequence.py
  
)

