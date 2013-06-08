#!/usr/bin/python

import os
from pert import py_pert 

class MyOperationBase(object):
  def __init__(self, uri = None):
    self.uri = uri
    return
    
    
    
  def Run(self):
    print 'pid: %s' % os.getpid()
    print 'id(py_pert): %s' % id(py_pert)
    ok, scheme, path = py_pert.ParseUri(self.uri)
    print 'path: %s' % path
    print 'exists: %s' % py_pert.Exists(self.uri)
    if py_pert.Exists(self.uri):
      print 'num shards: %s' % py_pert.GetNumShards(self.uri)
      reader = py_pert.StringTableReader()
      print 'about to open reader'
      reader.Open(self.uri)
      print 'about to use reader'
      count = 0
      for k,v in reader:
        print k
        count += 1
        if count > 5:
          break
        
      
    return True
