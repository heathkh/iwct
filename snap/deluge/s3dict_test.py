#!/usr/bin/python
import unittest
import time
from snap.pyglog import *
from snap.deluge import s3dict
from snap.deluge import deluge_pb2


  
def CreateTestProto(param):
  test_proto = deluge_pb2.ResourceProvenance()
  test_proto.fingerprint = param
  test_proto.flow = param
  test_proto.name = param
  test_proto.uri = param
  test_proto.start_time_sec = 1000
  test_proto.end_time_sec = 1010
  test_proto.input_fingerprints.extend(['fp1','fp2', 'fp3'])  
  return test_proto


class TestS3Dict(unittest.TestCase):    
  def test_basic(self):        
    input_key = 'test_key'
    input_value = 'test_value'
    test_bucket_name = 's3dict_test_foobar' 
    d = s3dict.S3Dictionary(test_bucket_name)
    self.assertEqual(d.Get(input_key), None)
    d.Set(input_key, input_value)
    self.assertEqual(d.Get(input_key), input_value)
    return
    
  def test_iter(self):            
    test_bucket_name = 's3dict_test_foobar_%s' % time.time()
    d = s3dict.S3Dictionary(test_bucket_name)

    test_data = {}
    test_data['k1'] = 'v1'
    test_data['k2'] = 'v2'
    test_data['k3'] = 'v3'
    test_data['k4'] = 'v4'
    
    for k, v in test_data.iteritems():      
      d.Set(k,v)
      
    out_data = {}  
    for k, v in d.iteritems():      
      out_data[k] = v
      
    for k, v in test_data.iteritems():
      self.assertEqual(v, out_data[k])
    
    return
  
class TestS3ProtoDict(unittest.TestCase):
  def test_proto_to_proto(self):        
    input_key = CreateTestProto('key_foo')
    input_value = CreateTestProto('value_foo')    
    test_bucket_name = 's3dict_test_foobar_%s' % time.time()
    key_type = deluge_pb2.ResourceProvenance()
    value_type = deluge_pb2.ResourceProvenance()
    d = s3dict.S3ProtoDictionary(test_bucket_name, key_type, value_type)
    self.assertEqual(d.Get(input_key), None)
    d.Set(input_key, input_value)
    output_value = d.Get(input_key)
    self.assertEqual(output_value, input_value)    
    #print 'key: %s' % input_key
    #print 'value: %s' % output_value    
    return
  
  def test_str_to_proto(self):        
    input_key = 'key_foo'
    input_value = CreateTestProto('value_foo')    
    test_bucket_name = 's3dict_test_foobar_%s' % time.time()    
    d = s3dict.S3ProtoDictionary(test_bucket_name, 
                                 key_proto = None,
                                 value_proto = deluge_pb2.ResourceProvenance())
    self.assertEqual(d.Get(input_key), None)
    d.Set(input_key, input_value)
    output_value = d.Get(input_key)
    self.assertEqual(output_value, input_value)    
    #print 'key: %s' % input_key
    #print 'value: %s' % output_value    
    return  
    

if __name__ == '__main__':
    unittest.main()
