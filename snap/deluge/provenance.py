#!/bin/python

from snap.pyglog import *
from snap.deluge import lebesgueset as ls
from snap.deluge import s3dict
from snap.deluge import deluge_pb2

provenance_dict = None

def GetProvenanceDict():
  global provenance_dict
  if not provenance_dict:        
    provenance_dict = s3dict.S3ProtoDictionary('deluge_resource_provenance_db_v02', value_proto = deluge_pb2.ResourceProvenance())
  return provenance_dict 

  
def SetResourceProvenanceRecord(record):    
  fingerprint = record.fingerprint
  CHECK(fingerprint != None)
  CHECK(len(fingerprint), 32)
  GetProvenanceDict().Set(fingerprint, record)  
  return


def GetResourceProvenanceRecord(fingerprint):
  CHECK_NE(fingerprint, 'd41d8cd98f00b204e9800998ecf8427e', 'You requested the fingerprint for an empty resource (md5 digest of empty string)... This is not a valid operation.')
  record = GetProvenanceDict().Get(fingerprint)  
  return record


def BuildResourceProvenanceList(fingerprint):
  fingerprint_stack = [fingerprint]
  visited_fingerprints = set()
  ancestor_records = []  
  while fingerprint_stack:
    fingerprint = fingerprint_stack.pop()
    record = GetResourceProvenanceRecord(fingerprint)
    if not record:
      continue
    #CHECK(record, 'failed to find record for fingerprint: %s' % (fingerprint))
    if record.fingerprint not in visited_fingerprints:
      ancestor_records.append(record)
      #print record      
    visited_fingerprints.add(fingerprint)
    for input_fingerprint in record.input_fingerprints:      
      CHECK_EQ(len(input_fingerprint), 32)      
      if input_fingerprint not in visited_fingerprints:        
        fingerprint_stack.append(input_fingerprint)  
  return ancestor_records
      
def GetResourceTiming(fingerprint):  
  ancestor_records = BuildResourceProvenanceList(fingerprint)
  serialized_time_sec = 0
  intervals = []  
  for record in ancestor_records:
    print record.start_time_sec, record.end_time_sec
    resource_time = record.end_time_sec - record.start_time_sec
    CHECK_GE(resource_time, 0)
    serialized_time_sec += resource_time
    intervals.append(ls.LebesgueSet([record.start_time_sec, record.end_time_sec]))    
  time_set = ls.LebesgueSet.union(intervals)   
  print time_set  
  wall_time_sec = time_set.measure()
  print 'wall_time_sec: %s' % (wall_time_sec)  
  print 'serialized_time_sec: %s' % (serialized_time_sec)  
  return wall_time_sec, serialized_time_sec


#def PrintResourceRecord(fingerprint):
#  record = GetResourceProvenanceRecord(fingerprint)  
#  print record.input_fingerprints    
#  return    
#    
#def PrintResourceProvenanceList(uri):
#  tmp_flow = Flow()  
#  resource = PertResource(tmp_flow, uri)
#  fingerprint = resource.GetFingerprint()
#  ancestor_records = ConstructResourceProvenanceList(fingerprint)
#  serialized_time_sec = 0
#  intervals = []  
#  for record in ancestor_records:
#    print record.start_time_sec, record.end_time_sec
#    resource_time = record.end_time_sec - record.start_time_sec
#    CHECK_GE(resource_time, 0)
#    serialized_time_sec += resource_time
#    intervals.append(ls.LebesgueSet([record.start_time_sec, record.end_time_sec]))    
#  time_set = ls.LebesgueSet.union(intervals)   
#  print time_set  
#  wall_time_sec = time_set.measure()
#  print 'wall_time_sec: %s' % (wall_time_sec)  
#  print 'serialized_time_sec: %s' % (serialized_time_sec)  
#  return  



#def GetResourceProvenanceRecordPath(resource_fingerprint):
#  record_base_path = '/tmp/deluge/provenance/'
#  record_filename = '%s/%s.pickle' % (record_base_path, resource_fingerprint)
#  return record_filename  
#
#def EnsureParentPathExists(f):
#  d = os.path.dirname(f)
#  if not os.path.exists(d):
#    os.makedirs(d)
#  return
#
#def SaveResourceProvenanceRecord(record):
#  fingerprint = record.fingerprint
#  CHECK(fingerprint != None)
#  path = GetResourceProvenanceRecordPath(fingerprint)
#  EnsureParentPathExists(path)
#  f = open(path, 'w')
#  pickle.dump(record, f, protocol=2)
#  return
#  
#def LoadResourceProvenanceRecord(fingerprint):
#  path = GetResourceProvenanceRecordPath(fingerprint)
#  record = None
#  if os.path.exists(path):    
#    f = open(path, 'r')
#    record = pickle.load(f)
#  return record    
       