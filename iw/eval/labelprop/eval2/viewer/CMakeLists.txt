CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PY_BINARY(
  NAME      viewer
  SOURCES   viewer.py              
  DATA      main.html
            confusion_matrix_cell.html
            confusion_matrix.html   
            jetcolormap.png        
)



