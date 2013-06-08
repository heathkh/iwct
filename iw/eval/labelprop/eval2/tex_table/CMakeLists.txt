CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PY_BINARY (
  NAME      export_table
  SOURCES   export_table.py
  DATA      table_frag_template.txt   
            table_frag_test.tex
)
