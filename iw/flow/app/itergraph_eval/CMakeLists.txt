CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PY_LIBRARY (
  NAME itergraph_eval
  SOURCES itergraph_eval.py     
)

PY_BINARY (
  NAME run_flow
  SOURCES run_flow.py
)

PY_BINARY (
  NAME      compare_ig_vs_irg
  SOURCES compare_ig_vs_irg.py
)



