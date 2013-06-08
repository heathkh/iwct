#!/usr/bin/python

from pyglog import *
from snap.pert import py_pert
from snap.google.base import py_base

def GenerateTestData():
  for i in range(1000):
    yield (py_base.Uint64ToKey(i), py_base.Uint64ToKey(i))

def CreateTestFile(uri):
  writer = py_pert.StringTableWriter()
  writer.Open(uri, 10)
  for k,v in GenerateTestData():
    writer.Add(k,v)
  return


def test_OpenSplit():
  remote_uri = "maprfs://data/itergraph/tide_v13/photoid_to_image.pert/part-00046"
  
  CHECK(py_pert.Exists(remote_uri))
  
  reader = py_pert.StringTableShardReader()
  split_start = 4598228
  split_length = 1255113
  split_end = split_start + split_length
  reader.OpenSplit(remote_uri, split_start, split_end)
  
  count = 0
  for key, value in reader:
    count += 1
    
  print count
  return


def test_CopyLocalToUri():
  local_uri = "local://tmp/data/test_ufs.pert";
  remote_uri = "maprfs://data/tmp/test_ufs.pert";
  
  CreateTestFile(local_uri)
  
  ok, scheme, path, error = py_pert.ParseUri(local_uri)
  CHECK(ok)
  
  py_pert.CopyLocalToUri(path, remote_uri)
  
  CHECK(py_pert.Exists(local_uri))
  CHECK(py_pert.Exists(remote_uri))
  
  reader = py_pert.StringTableReader()
  reader.Open(remote_uri)
  
  expected_count = 1000
  count = 0
  for (key, value), (expected_key, expected_value) in zip(reader, GenerateTestData()):
    CHECK_EQ(key, expected_key)
    CHECK_EQ(value, expected_value)
    count += 1
    
  CHECK_EQ(count, expected_count)
  
  
  print py_pert.ListDirectory(local_uri)
  print py_pert.ListDirectory(remote_uri)
  
  return


def test_CopyLocalToUri():
  CHECK(py_pert.Exists(fingerprint_uri))
  input_file = py_pert.OpenInput(fingerprint_uri)    
  ok, fingerprint = input_file.ReadToString()

        
if __name__ == "__main__":
  #test_OpenSplit() 
  #test_CopyLocalToUri()

  

  
    
  
