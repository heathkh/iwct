#!/usr/bin/python
import os
from parallel_1 import MyOperationBase
from pert import py_pert

class MyOperation(MyOperationBase):
  def __init__(self, uri = None):
    super(MyOperation,self).__init__(uri)
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
    
    super(MyOperation,self).Run()
      
    return True
    
