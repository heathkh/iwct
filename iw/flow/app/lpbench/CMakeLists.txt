CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROTO_LIBRARY(
  NAME      proto
  PROTO     lpbench.proto
  PACKAGES  
            //iw/flow/app/itergraph:proto
            //iw/eval/labelprop/eval1:proto
            //iw/eval/labelprop/eval2:proto
            //snap/deluge:proto
)

PY_LIBRARY (
  NAME      lpbench
  SOURCES   resultdb.py
            flows.py                 
)

PY_BINARY (
  NAME      run_lpbench
  SOURCES   run_lpbench.py
)

PY_BINARY (
  NAME      plot_lpbench
  SOURCES   plot_lpbench.py
  DATA      plot_template.html
)


PY_BINARY (
  NAME      download_results
  SOURCES   download_results.py  
)



ADD_SUBDIRECTORY(viewer)
ADD_SUBDIRECTORY(param_table)