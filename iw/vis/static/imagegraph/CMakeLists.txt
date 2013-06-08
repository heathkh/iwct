CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
 
PY_BINARY (
  NAME      export
  SOURCES   export.py
            renderers.py
            tide_image_graph.py  
  DATA      index_template.html
            graph_template.html
            js/d3.legend.js
            js/d3.v3.js
            js/d3.v3.min.js
)

