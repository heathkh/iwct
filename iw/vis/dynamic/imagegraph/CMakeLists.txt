CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

 
PY_BINARY (
  NAME      viewer
  SOURCES   viewer.py
            renderers.py
            tide_image_graph.py
  DATA      header.html
            style.html
            graph.html            
            footer.html
            main.html
)

