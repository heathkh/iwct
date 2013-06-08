CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

PY_LIBRARY(
  NAME      connection_plugins
  SOURCES   fireball.py  __init__.py  local.py  paramiko_ssh.py  ssh.py   
)

