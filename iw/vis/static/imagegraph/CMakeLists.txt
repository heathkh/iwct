CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

add_subdirectory(js) 

 
PY_BINARY (
  NAME      exporter
  SOURCES   exporter.py
            renderers.py
            tide_image_graph.py  
  DATA      index_template.html
            graph_template.html
  #PACKAGES  //iw/vis/static/imagegraph/js:js         
         
)

