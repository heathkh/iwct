#!/usr/bin/python
""" Tools to to help test cbir implementations."""
import glob
import os

from iw import iw_pb2
from snap.pyglog import *
from snap.google.base import py_base
from pert import py_pert
from iw import util as iwutil

from iw.matching import py_featureextractor 


from iw import util as iwutil
from iw.matching.cbir.bow import py_bow
from iw.matching.cbir.bow import bow_pb2
from iw.matching.cbir import cbir_pb2



class BowCbirTester(object):  
  def __init__(self, test_data_path, bow_cbir_params, index, force_clean = True):
    CHECK(os.path.exists(test_data_path))
    self.test_data_path = test_data_path
    self.bow_cbir_params = bow_cbir_params
    self.base_path = '/%s/tmp/%s' % (self.test_data_path, iwutil.HashProto(bow_cbir_params))
    self.base_uri = 'local://%s' % (self.base_path)      
    self.images_uri = '%s/images.pert' % (self.base_uri)
    self.features_uri = '%s/features.pert' % (self.base_uri)    
    self.bow_uri = '%s/bow.pert' % (self.base_uri)
    self.index = index    
    if force_clean:
      py_pert.Remove(self.base_uri)      
    return
    
  def Run(self):
    self.__PackImages()
    self.__ExtractFeatures()
    self.__QuantizeFeatures()
    photoid_to_name, results = self.__QueryIndex()
    self.__VerifyResults(photoid_to_name, results)    
    return
       
  def __PackImages(self):    
    LOG(INFO, 'Packing images')
    if py_pert.Exists(self.images_uri):
      return    
    iwutil.PackImagesDirectoryToPert(self.test_data_path, self.images_uri, randomize = False)    
    return
  
  def __ExtractFeatures(self):    
    LOG(INFO, 'Extracting features')
    if py_pert.Exists(self.features_uri):
      return    
    feature_extractor_params = iw_pb2.FeatureExtractorParams()
    feature_extractor_params.vgg_affine_sift_params.type = iw_pb2.FeatureExtractorParams.VggAffineSiftParams.AFFINE_HESSIAN
    feature_extractor_params.vgg_affine_sift_params.threshold = 201
    feature_extractor_params.vgg_affine_sift_params.root_sift_normalization = False
    CHECK(feature_extractor_params.IsInitialized())
    iwutil.ExtractFeatures(feature_extractor_params, self.images_uri, self.features_uri)
    return
  
  def __QuantizeFeatures(self):
    LOG(INFO, 'Quantizing features')
    if py_pert.Exists(self.bow_uri):
      return
    bag_of_words = bow_pb2.BagOfWords()    
    features = iw_pb2.ImageFeatures()
    reader = py_pert.StringTableReader()
    CHECK(reader.Open(self.features_uri))
    quantizer = py_bow.Quantizer()
    vv_uri = str(self.bow_cbir_params.visual_vocabulary_uri)
    LOG(INFO, "Initializing quantizer: %s" % (vv_uri))
    exact = False
    CHECK(quantizer.Init(vv_uri, exact))
    features = iw_pb2.ImageFeatures()    
    writer = py_pert.ProtoTableWriter()
    CHECK(writer.Open(bag_of_words, self.bow_uri, 1))
    progress = iwutil.MakeProgressBar(reader.Entries())
    for i, (key, value) in enumerate(reader):
      features.ParseFromString(value)
      ok, bag_of_words = quantizer.Quantize(features)
      CHECK(ok)
      bag_of_words.word_id.sort() # need to be sorted for inria
      writer.Add(key, bag_of_words.SerializeToString())
      progress.update(i)      
    return
  
  def __QueryIndex(self):
    LOG(INFO, 'Querying bow cbir index')
    reader = py_pert.StringTableReader()
    CHECK(reader.Open(self.bow_uri))    
    bag_of_words = bow_pb2.BagOfWords()
    photoid_to_name = []
    progress = iwutil.MakeProgressBar(reader.Entries())
    for i, (key, value) in enumerate(reader):
      bag_of_words.ParseFromString(value)
      self.index.Add(long(i), bag_of_words)
      photoid_to_name.append(key)      
      progress.update(i)
        
    #result = cbir_pb2.QueryResults()
    reader.SeekToStart()
    progress = iwutil.MakeProgressBar(reader.Entries())
    results = {}
    for i, (key, value) in enumerate(reader):
      key = key.encode('ascii','ignore')
      bag_of_words.ParseFromString(value)
      ok, result = self.index.Query(bag_of_words, self.bow_cbir_params.num_candidates)
      CHECK(ok)
      results[key] = result 
      progress.update(i)
    
    return photoid_to_name, results
    
    
  def __VerifyResults(self, photoid_to_name, results):
    LOG(INFO, 'Verifying results')
    #print photoid_to_name
    #print results
    ranks = []
    for filename, image_results in results.iteritems():
      print filename
      match_rank = 0
      for rank, entry in enumerate(image_results.entries):
        
        match_rank = rank
        result_filename = photoid_to_name[entry.image_id]
        print result_filename, entry.score
        if result_filename == filename:
          continue
        id_1 = filename[0:2]
        id_2 = result_filename[0:2]
        if id_1 == id_2:
          break     
      ranks.append(match_rank)
      
    print ranks
      
    return True
    
    