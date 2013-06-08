#!/usr/bin/python

from snap.pert import py_pert
from snap.pyglog import *
from iw import iw_pb2

from iw.matching import feature_extractor_density as fed

def main():    
  images_uri = 'local:///home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v12/photoid_to_image.pert'
  initial_extractor_params = iw_pb2.FeatureExtractorParams()
  
  
  initial_extractor_params.ocv_sift_params.num_octave_layers = 3
  initial_extractor_params.ocv_sift_params.contrast_threshold = 0.04
  initial_extractor_params.ocv_sift_params.edge_threshold = 30
  initial_extractor_params.ocv_sift_params.sigma = 1.2
  initial_extractor_params.ocv_sift_params.upright = True
  initial_extractor_params.ocv_sift_params.root_sift_normalization = True
  CHECK(initial_extractor_params.ocv_sift_params.IsInitialized())
  
#  initial_extractor_params.vgg_affine_sift_params.type = initial_extractor_params.vgg_affine_sift_params.AFFINE_HESSIAN
#  initial_extractor_params.vgg_affine_sift_params.threshold = 200
#  initial_extractor_params.vgg_affine_sift_params.root_sift_normalization = True  
#  CHECK(initial_extractor_params.vgg_affine_sift_params.IsInitialized())
  
  desired_num_features_per_megapixel = 3255 # or about 1000 for a 640x480 image
  tuned_params = fed.TuneFeatureExtractorParamsOrDie(images_uri, initial_extractor_params, desired_num_features_per_megapixel)
  print tuned_params
  return 0

if __name__ == "__main__":        
  main()
    
    
    
    
    
  
