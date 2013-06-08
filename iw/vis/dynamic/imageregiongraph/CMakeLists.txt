CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PY_BINARY (
  NAME      viewer
  SOURCES   viewer.py
            renderers.py
            graph.py
  DATA      header.html
            style.html
            graph.html            
            footer.html
            main.html
)

