#!/usr/bin/python
""" Tools to tune feature extractor parameters to achieve desired feature density."""

import os
import random
from snap.pyglog import *
from snap.google.base import py_base
from pert import py_pert
from iw import util as iwutil
from iw.matching import py_featureextractor 
from iw import iw_pb2
from scipy.optimize import brentq


class ParamTuner(object):
  def __init__(self, images_uri, initial_params, desired_num_features_per_megapixel):
    self.desired_num_features_per_megapixel = desired_num_features_per_megapixel
    # sample images from dataset
    self.num_samples = 100
    self.reader = py_pert.StringTableShardSetReader()
    CHECK(self.reader.Open(images_uri))  
    self.num_images = self.reader.Entries()  
    self.sample_indices = random.sample(range(0,self.num_images), self.num_samples)
    
    self.cur_params = iw_pb2.FeatureExtractorParams()
    self.cur_params.CopyFrom(initial_params)
    
    self.bounds = []
    if initial_params.HasField('ocv_sift_params'):
      self.bounds = (0.00001, 0.5,)
      self.x_tol = 0.00001
    elif initial_params.HasField('vgg_affine_sift_params'):
      self.bounds = (5000, 100)
      self.x_tol = 10      
    else:
      LOG(FATAL, 'unexpected')
    return
  
  def AdjustParams(self, x, params):
    if params.HasField('ocv_sift_params'):
      params.ocv_sift_params.contrast_threshold = x
    elif params.HasField('vgg_affine_sift_params'):
      params.vgg_affine_sift_params.threshold = x      
    else:
      LOG(FATAL, 'unexpected')
    return 
  
  def __ComputeExtractorFpm(self, extractor):
    # extract features using current settings
    image = iw_pb2.JpegImage()
    total_image_area_megapixels = 0.0
    total_num_features = 0
    for sample_index in self.sample_indices:
      ok, k, v = self.reader.GetIndex(sample_index)
      CHECK(ok)
      image.ParseFromString(v)
      CHECK(image.IsInitialized())
      ok, features = extractor.Run(image.data)
      total_image_area_megapixels += image.width*image.height*1e-6;
      if ok:        
        total_num_features += len(features.keypoints)
          
    # compute stats
    mean_fpm = total_num_features/total_image_area_megapixels
    LOG(INFO, 'mean_fpm: %f' % (mean_fpm))    
    return mean_fpm
  
  def __call__(self, x):
    # create feature extractor with current params
    self.AdjustParams(x, self.cur_params)
    LOG(INFO, 'params: %s' % (str(self.cur_params)))
    extractor = py_featureextractor.CreateFeatureExtractorOrDie(self.cur_params)    
    fpm = self.__ComputeExtractorFpm(extractor)
    error = fpm - self.desired_num_features_per_megapixel     
    LOG(INFO, 'error: %f' % (error))    
    return error
    

def TuneFeatureExtractorParamsOrDie(images_uri, initial_extractor_params, desired_num_features_per_megapixel):
  tuner = ParamTuner(images_uri, initial_extractor_params, desired_num_features_per_megapixel)
  x0, result = brentq(tuner, tuner.bounds[0], tuner.bounds[1], xtol=tuner.x_tol,  rtol=10.0, full_output = True)
  print x0
  print 'num iterations: %d' % (result.iterations)
  CHECK(result.converged)  
  tuner.AdjustParams(x0, initial_extractor_params)    
  return initial_extractor_params


