#!/usr/bin/python
import unittest
import tempfile
import shutil
from pyglog import *
from pert import py_pert
from iw.matching.cbir import util as cbirutil
from iw import iw_pb2
from iw import util as iwutil
#import matplotlib.pyplot as plt
#import numpy as np

from iw.matching.cbir import py_cbir
from iw.matching.cbir import cbir_pb2



def CreateQueryScorer(feature_counts_table):
  params = cbir_pb2.QueryScorerParams()
  params.max_results = 10  
  params.scoring = cbir_pb2.QueryScorerParams.ADAPTIVE
  params.normalization = cbir_pb2.QueryScorerParams.NRC
  params.correspondence_filter = cbir_pb2.QueryScorerParams.WGC_FILTER
  params.wgc_correspondence_filter_params.type =  cbir_pb2.WGCCorrespondenceFilterParams.SCALE
  params.wgc_correspondence_filter_params.scale_max = 4.0
  params.wgc_correspondence_filter_params.scale_histogram_size = 50.0  
  CHECK(params.IsInitialized())  
  return py_cbir.QueryScorer(feature_counts_table.Get(), params);

class TestWgcFilter(unittest.TestCase):
    def CreateTestData(self):
      self.test_data_path = '/tmp/TestWgcFilter/'
      self.images_uri =  'local://%s/images.pert' % self.test_data_path
      self.features_uri =  'local://%s/features.pert' % self.test_data_path      
      self.feature_counts_uri =  'local://%s/features_counts.pert' % self.test_data_path
      test_images_path = os.path.dirname(__file__)
      if not py_pert.Exists(self.images_uri):
        randomize = False
        max_images = None
        iwutil.PackImagesDirectoryToPert(test_images_path, self.images_uri, randomize, max_images)        
      if not py_pert.Exists(self.features_uri):
        iwutil.ExtractFeatures(self.images_uri, self.features_uri)          
      if not py_pert.Exists(self.feature_counts_uri):
        cbirutil.CreateFeatureCountsTable(self.features_uri, self.feature_counts_uri)        
      return
    
    def setUp(self):
      self.CreateTestData()
      self.tmp_path = tempfile.mkdtemp()
      return
    
    def tearDown(self):
     shutil.rmtree(self.tmp_path)
     return
    
    def test_basic(self):      
      index_uri = 'local://%s/index' % self.tmp_path
      cbirutil.CreateIndex(self.features_uri, index_uri)
      index = cbirutil.LoadIndex(index_uri)      
      features_reader = py_pert.StringTableReader()
      CHECK(features_reader.Open(self.features_uri))      
      feature_counts_table = cbirutil.LoadFeatureCountsTable(self.feature_counts_uri)      
      k = 10
      for key, value in features_reader:
        image_id = iwutil.ParseUint64Key(key)
        query_features = iw_pb2.ImageFeatures()
        query_features.ParseFromString(value)
        print image_id
        print 'num_features: %d' % (len(query_features.descriptors))        
        ok, neighbors = index.Search([image_id], k, query_features)
        CHECK(ok)
        CHECK_GT(len(neighbors.features), 0)
        scorer = CreateQueryScorer(feature_counts_table)
        ok, results = scorer.Run( neighbors)
        CHECK(ok)
        print results
      return
    

if __name__ == '__main__':
    unittest.main()
