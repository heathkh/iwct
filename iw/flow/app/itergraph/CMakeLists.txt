CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROTO_LIBRARY(
  NAME     proto
  PROTO    itergraph.proto
  PACKAGES //iw/matching/cbir:proto
           //iw:proto
)

PY_LIBRARY (
  NAME itergraph
  SOURCES itergraph.py     
)

PY_BINARY (
  NAME run_flow
  SOURCES run_flow.py
)


