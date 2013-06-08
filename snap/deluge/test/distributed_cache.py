#!/usr/bin/python

from pyglog import *
from deluge import mr
import os
from pert import py_pert
  
  
def CreateTestData(uri):   
  writer = py_pert.StringTableWriter()
  writer.Open(uri, 4)
  
  for i in range(10000):
    d = '%05d' % i
    writer.Add(d,d)
  return
  
     
def main():   
  
  test_data_uri = 'maprfs://data/test/test.pert';
  CreateTestData(test_data_uri)
  
  secondary_data_uri = 'maprfs://data/test/secondary.pert';
  CreateTestData(secondary_data_uri)
  
  uris_to_cache = 'maprfs://data/test/secondary.pert'; 
  
  pipes_binary = '%s/%s' % (os.path.abspath(os.path.dirname(__file__)), 'mr_distributed_cache')
  
  input_uri = test_data_uri
  output_uri = 'maprfs://data/test/test_out.pert';
  num_map_jobs = 2
  num_reduce_jobs = 3 
  output_is_sorted = True
  parameters = { 'secondary_input_uri': secondary_data_uri }  
  mr_driver = mr.MapReduceDriverHadoopCache(pipes_binary, 
                                 input_uri, 
                                 output_uri, 
                                 num_map_jobs, 
                                 num_reduce_jobs, 
                                 output_is_sorted,
                                 parameters,
                                 uris_to_cache
                                 )    
  CHECK(mr_driver.Run())
  return
  

if __name__ == "__main__":        
  main()
    
    
    
        