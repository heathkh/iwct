#!/usr/bin/python

import logging
from pert import py_pert as pert
from pert.test import test_pb2
from snap.pyglog import *

def test_string_table():
  filename = "local:///home/ubuntu/Desktop/test_string_table";
  
  person = test_pb2.Person()
  person.first_name = 'foo'
  person.last_name = 'bar'
  
  writer = pert.StringTableWriter()
  writer.Open(filename, 1)  
  writer.Add('key1', person.SerializeToString())
  writer.Add('key2', person.SerializeToString())
  writer.Close()
   
  reader = pert.StringTableReader()
  reader.Open(filename)
  
  for k,v in reader:
    my_person = test_pb2.Person()
    my_person.ParseFromString(v)
    print "key %s value %s" % (k, my_person)
    
  return


def test_proto_table():
  filename = "local:///home/ubuntu/Desktop/test_proto_table";
  
  person = test_pb2.Person()
  person.first_name = 'foo'
  person.last_name = 'bar'
    
  writer = pert.ProtoTableWriter()
  writer.Open(person, filename, 10)  
  writer.Add('key1', person.SerializeToString())
  writer.Add('key2', person.SerializeToString())
  writer.Close()
   
  print "press a key to continue",
  f = raw_input()
   
  reader = pert.StringTableReader()
  CHECK(reader.Open(filename))
  
  person = test_pb2.Person()
  for k, v in reader:
    person.ParseFromString(v)      
    print "key %s person %s" % (k, person)
    
  return
        
if __name__ == "__main__": 
  #test_string_table()
  test_proto_table()
  
    
  
