#!/usr/bin/python
from snap.pyglog import *
from snap import boto
from snap.boto.s3 import key
import copy
import md5

class S3Dictionary(object):
  """ A key-value store using s3."""  
  def __init__(self, s3_bucket_name):
    self.s3_bucket_name = s3_bucket_name    
    self.s3 = boto.connect_s3()  
    self.bucket = self.s3.create_bucket(self.s3_bucket_name, policy='public-read')    
    return
  
  def Contains(self, key):
    """ Returns True if key exists, false otherwise. """
    found_key = (self.bucket.get_key(key) != None)
    return found_key
    
  def Get(self, key):    
    """ Returns the value for this key, or none if key not found."""
    value = None            
    k = self.bucket.get_key(key)    
    if k:
      value = k.get_contents_as_string()    
    return value
      
  def Set(self, key, value):    
    k = self.bucket.new_key(key)
    k.set_contents_from_string(value)
    return
  
  def iteritems(self):
    return S3Dictionary.__Iterator(self.bucket)
  
  class __Iterator(object):
    def __init__(self, bucket):
      self.bucket = bucket
      self.key_iter = self.bucket.list().__iter__()
      return

    def __iter__(self):
      return self

    def next(self):
      s3key = self.key_iter.next()
      key = s3key.name
      value = s3key.get_contents_as_string()
      return key, value

  
    
class S3ProtoDictionary(S3Dictionary):
  """ A key-value store where keys and values can be a string or a specific proto type."""
  def __init__(self, s3_bucket_name, key_proto = None, value_proto = None):
    super(S3ProtoDictionary, self).__init__(s3_bucket_name)
    self.key_proto_template = None
    if key_proto:
      self.key_proto_template = copy.deepcopy(key_proto)      
    self.value_proto_template = None
    if value_proto:
      self.value_proto_template = copy.deepcopy(value_proto)  
    return

  def Contains(self, key):    
    self.__CheckValidKey(key)
    return super(S3ProtoDictionary, self).Contains(self.__ToStringKey(key))

  def Get(self, key):
    self.__CheckValidKey(key)    
    value = super(S3ProtoDictionary, self).Get(self.__ToStringKey(key))    
    if value and self.value_proto_template:
      proto_value = copy.deepcopy(self.value_proto_template)
      proto_value.ParseFromString(value)
      CHECK(proto_value.IsInitialized())
      value = proto_value
    return value
    
  def Set(self, key, value):
    self.__CheckValidKey(key)
    self.__CheckValidValue(value)            
    super(S3ProtoDictionary, self).Set(self.__ToStringKey(key), self.__ToStringValue(value))
    return
  
  def iteritems(self):
    return S3ProtoDictionary.__Iterator(self)
  
  class __Iterator(object):
    def __init__(self, dictionary):
      self.dictionary = dictionary      
      self.key_iter = dictionary.bucket.list().__iter__()
      return

    def __iter__(self):
      return self

    def next(self):
      s3key = self.key_iter.next()
      key = s3key.name
      value = s3key.get_contents_as_string()
        
      if self.dictionary.value_proto_template != None:
        proto_value = copy.deepcopy(self.dictionary.value_proto_template)
        proto_value.ParseFromString(value)
        #if not proto_value.IsInitialized():
        #  LOG(INFO, 'proto is not initialized: %s' % (proto_value)) 
        value = proto_value  
      
      return key, value
  
  def __CheckValidKey(self, key):
    if self.key_proto_template:
      CHECK_EQ(self.key_proto_template.__class__.__name__, key.__class__.__name__)
    else:
      CHECK(isinstance(key, basestring), 'expected a string but got: %s ' % (key.__class__.__name__))      
    return
  
  def __CheckValidValue(self, value):
    if self.value_proto_template:
      CHECK_EQ(self.value_proto_template.__class__.__name__, value.__class__.__name__)
    else:
      CHECK(isinstance(value, basestring))
    return
  
  def __ToStringValue(self, value):
    string_value = value
    if self.value_proto_template:
      string_value = value.SerializeToString()    
    return string_value
  
  def __ToStringKey(self, key):
    string_key = key
    if self.key_proto_template:
      string_key = self.__ProtoToKey(key)      
    return string_key
      
  def __ProtoToKey(self, proto):
    """ Returns a UUID for a protobuffer computed as md5sum of serialized data."""
    data = proto.SerializeToString()
    CHECK_GE(len(data), 10, 'require non-empty proto for valid md5 digest.') 
    return md5.new(data).hexdigest()