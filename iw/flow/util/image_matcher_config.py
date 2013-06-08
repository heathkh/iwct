#!/usr/bin/python

import logging
from iw import iw_pb2

from google.protobuf import text_format

def GetImageMatcherConfig(feature_type):
  """ Returns the best tuned configuration for a given feature type. """  
  image_matcher_config = iw_pb2.ImageMatcherConfig()
  config_str = None

  if feature_type == 'usift':    
    config_str = """
    feature_type: "usift"
    affine_acransac_params {        
      max_iterations : 50000
      precision_ratio : 0.25  
      max_scaling : 5.0
      max_correspondence_scale_deviation : 0.75
      max_in_plane_rotation : 0.785398163
    }
    """
  elif feature_type == 'sift':    
    config_str = """
    feature_type: "sift"
    affine_acransac_params {        
      max_iterations : 50000
      precision_ratio : 0.25  
      max_scaling : 5.0
      max_correspondence_scale_deviation : 0.75
      max_in_plane_rotation : 6.28318531
    }
    """  
  elif feature_type == 'ahess':    
    config_str = """
    feature_type: "ahess"
    affine_acransac_params {        
      max_iterations : 50000
      precision_ratio : 0.25  
      max_scaling : 5.0
      max_correspondence_scale_deviation : 0.75
      max_in_plane_rotation : 6.28318531
    }
    """  
  else:    
    assert(False)  
  
  text_format.Merge(config_str, image_matcher_config)    
  assert(image_matcher_config.IsInitialized())
  return image_matcher_config 
