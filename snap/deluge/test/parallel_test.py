#!/usr/bin/python
import inspect
import os
from multiprocessing import Pool
import parallel_2
from pert import py_pert 


def RunFlowExternal(run_flow_config):
  module_file = run_flow_config[0]
  
  # in case the module file is pyc change it to py
  if module_file[-1] == 'c':
    module_file = module_file[0:-1]
  
  class_name = run_flow_config[1]
  state = run_flow_config[2]
  
  namespace = {}
  execfile(module_file, namespace)
  my_class = namespace[class_name]
  my_instance = my_class.__new__(my_class)
  my_instance.__dict__ = state
  my_instance.Run()
  
  return

def CreateRunFlowExternalConfig(obj):
    module_file = inspect.getmodule(obj).__file__
    class_name = obj.__class__.__name__
    state = obj.__dict__
    return (module_file, class_name, state)

def main():
  print 'id(py_pert): %s' % id(py_pert)
  ops = []
  ops.append(parallel_2.MyOperation('maprfs:///data/test/tide_v00/photoid_to_image.pert'))
  
  pool = Pool(processes=2)
  
  async_results = []
  for op in ops:
    run_config = CreateRunFlowExternalConfig(op)
    async_result = pool.apply_async(RunFlowExternal, [run_config])
    async_results.append(async_result)
    
  
  for result in async_results:
    result.get()
  
  
  
  return 0    

if __name__ == "__main__":        
  main()
    
    
    
    
    
  
