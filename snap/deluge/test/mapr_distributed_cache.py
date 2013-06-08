#!/usr/bin/python

import os
from snap.pyglog import *
from snap.deluge import mr
from snap.pert import py_pert  
import time
  
def CreateTestData(uri):
   
  writer = py_pert.StringTableWriter()
  writer.Open(uri, 4)  
  num_entries = 10000
  for i in range(num_entries):
    d = '%05d' % i
    writer.Add(d,d)
  writer.Close()
  
  reader = py_pert.StringTableReader()
  CHECK(reader.Open(uri))
  
  CHECK_EQ(reader.Entries(), num_entries)
  reader.Close()
  
  return
       
def main():   
  test_data_uri = 'maprfs://data/test/test.pert';
  CreateTestData(test_data_uri)
  
  secondary_data_uri = 'maprfs://data/test/secondary.pert';
  CreateTestData(secondary_data_uri)
  
  uris_to_cache = 'maprfs://data/test/secondary.pert'; 
  
  pipes_binary = '%s/%s' % (os.path.abspath(os.path.dirname(__file__)), 'mr_mapr_distributed_cache')
  
  input_uri = test_data_uri
  output_uri = 'maprfs://data/test/test_out.pert';
  num_map_jobs = 2
  num_reduce_jobs = 3 
  output_is_sorted = True
  parameters = { 'secondary_input_uri': secondary_data_uri }  
  mr_driver = mr.MapReduceDriverMaprCache(pipes_binary, 
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
    
    
    
        