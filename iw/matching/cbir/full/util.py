#!/usr/bin/python

from pyglog import *
from iw.matching.cbir.full import py_full
from iw import util as iwutil
from pert import py_pert
import math
from iw import iw_pb2     

def CreateIndex(features_uri, index_uri):
  reader = py_pert.StringTableShardSetReader()
  CHECK(reader.Open(features_uri))
  features = iw_pb2.ImageFeatures()
  
  include_keypoints = True
  index = py_cbir.FeatureIndex(include_keypoints)
  progress = iwutil.MakeProgressBar(reader.Entries())  
  for i, (k,v) in enumerate(reader):    
    image_id = iwutil.KeyToUint64(k)
    features.ParseFromString(v)
    index.Add(image_id, features)      
    progress.update(i)
  progress.finish()    
  index.Build()  
  return index.Save(index_uri)

def LoadIndex(index_uri):
  include_keypoints = True
  index = py_cbir.FeatureIndex(include_keypoints)
  index.Load(index_uri)
  return index


def CreateConcurrentIndex(index):  
  concurrent_index = py_cbir.ConcurrentFeatureIndex(index)  
  return concurrent_index

def CreateFeatureCountsTable(features_uri, feature_count_uri):
  print 'features_uri: ' + features_uri
  print 'feature_count_uri: ' + feature_count_uri
  counter = py_cbir.ImageFeatureCountTable();
  CHECK(counter.Create(features_uri))
  CHECK(counter.Save(feature_count_uri))
  return
    
def LoadFeatureCountsTable(feature_count_uri):    
  feature_counts_table = py_cbir.ImageFeatureCountTable()
  feature_counts_table.Load(feature_count_uri)
  return feature_counts_table    
  

#def GetCbirKeypoints(image_features):
#  #image_diag_size = math.sqrt(image_features.width*image_features.width + image_features.height*image_features.height)
#  cbir_keypoints = cbir_pb2.ImageKeypoints()
#  for keypoint in image_features.keypoints:
#    cbir_keypoint = cbir_keypoints.entries.add() 
#    cbir_keypoint.x = keypoint.pos.x
#    cbir_keypoint.y = keypoint.pos.y
#    cbir_keypoint.radius = keypoint.radius    
#  return cbir_keypoints

  