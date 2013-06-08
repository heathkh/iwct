#!/usr/bin/python
import unittest

from pyglog import *
from iw.matching.cbir import py_cbir
from iw import iw_pb2
from iw import util as iwutil
from pert import py_pert
import tempfile
import shutil


# we must use exact index because noise in approximation will cause test to fail
def CreateExactIndex(features_uri_list, index_path):
  index = py_cbir.FeatureIndex(True, True)  
  for features_uri in features_uri_list:
    reader = py_pert.StringTableShardSetReader()
    CHECK(reader.Open(features_uri))
    features = iw_pb2.ImageFeatures()
    for i, (k,v) in enumerate(reader):
      image_id = iwutil.ParseUint64Key(k)
      features.ParseFromString(v)
      index.Add(image_id, features)
    
  index.Build()
  index.Save(index_path)
  return 


def GetTestFeatures(features_uri):
  images_features = []
  reader = py_pert.StringTableShardReader()
  CHECK(reader.Open(features_uri))  
  for i, (k,v) in enumerate(reader):
    image_id = iwutil.ParseUint64Key(k)
    features = iw_pb2.ImageFeatures()
    features.ParseFromString(v)  
    images_features.append( (image_id, features) )
    break
  return images_features
  


def TestMergeIsCorrect():
  features_base_uri = 'local:///media/ebs/4a4b34/tide_v12/usift/features.pert'
  features_part_a = features_base_uri + '/part-00000'
  features_part_b = features_base_uri + '/part-00001'
  
  tmp_data_path = tempfile.mkdtemp()
  
    
  index_full_uri = 'local://%s/index_full' % tmp_data_path
  index_a_uri = 'local://%s/index_a' % tmp_data_path
  index_b_uri = 'local://%s/index_b' % tmp_data_path
  
  CreateExactIndex( [features_part_a, features_part_b], index_full_uri)
  CreateExactIndex( [features_part_a], index_a_uri)
  CreateExactIndex( [features_part_b], index_b_uri)
  
  index_full = py_cbir.FeatureIndex(True)
  ok = index_full.Load(index_full_uri)
  CHECK(ok)
  
  index_a = py_cbir.FeatureIndex(True)
  ok = index_a.Load(index_a_uri)
  CHECK(ok)
  
  index_b = py_cbir.FeatureIndex(False)
  ok = index_b.Load(index_b_uri)
  CHECK(ok)
  
  test_images = GetTestFeatures(features_part_a)
  
  k = 2
  
  for image_id, features in test_images:    
    print image_id
    print 'num_features: %d' % (len(features.descriptors))
    ok, knn_full = index_full.Search([image_id], k, features)
    CHECK(ok)
    CHECK_GT(len(knn_full.features), 0)
    
    ok, knn_a = index_a.Search([image_id], k, features)
    CHECK(ok)
    ok, knn_b = index_b.Search([image_id], k, features)
    CHECK(ok)
    
    acc = py_cbir.NearestNeighborsAccumulator(k)
    acc.Add(knn_a)
    acc.Add(knn_b)
    ok, knn_merged = acc.GetResult()
    CHECK(ok)
    
    CHECK_EQ(len(knn_full.features), len(knn_merged.features))
    CHECK_GT(len(knn_full.features), 0)
        
    for feature_full, feature_merged in zip(knn_full.features, knn_merged.features):
      CHECK_EQ(len(feature_full.entries), len(feature_merged.entries))
      CHECK_GT(len(feature_full.entries), 0)      
      for i, (neighbor_full, neighbor_merged) in enumerate(zip(feature_full.entries, feature_merged.entries)):        
        CHECK_EQ(neighbor_full.image_id, neighbor_merged.image_id, '%s %s' % (neighbor_full, neighbor_merged)) # the merging doesn't preserve order of neighbors with same distance 
        CHECK_GE(neighbor_full.distance, neighbor_merged.distance)
        #CHECK_EQ(neighbor_full.scaling, neighbor_merged.scaling)
  
  
  shutil.rmtree(tmp_data_path)
 
  return

if __name__ == '__main__':
    TestMergeIsCorrect()
