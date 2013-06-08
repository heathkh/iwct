CMAKE_MINIMUM_REQUIRED(VERSION 2.8)


PY_LIBRARY (
  NAME allpairsgraph
  SOURCES allpairsgraph.py     
)

PY_BINARY (
  NAME run_flow
  SOURCES run_flow.py
)

