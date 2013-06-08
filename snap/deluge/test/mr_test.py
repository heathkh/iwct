#!/usr/bin/python

import os
from deluge import mr
from pert import py_pert 

def main():
 
  src_uri = 'maprfs://test/tide_v00/usift/1000/centers_00000.pert'
  dst_uri = 'maprfs://test/tide_v00/usift/1000/foo.pert'
  
  mr.CopyPertFile(src_uri, dst_uri) 
 
  return 0    

if __name__ == "__main__":        
  main()
    
    
    
    
    
  
