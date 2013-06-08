#!/usr/bin/python

import settings
import logging

if __name__ == "__main__": 
  stage_name = 'innerjoin'
  bin_name = 'innerjoin'
  stage_path = settings.init_stage_path(stage_name)
  settings.load_pipes_bin(bin_name, stage_name)
  
  primary_input_path='/%s/reshard_primary/data' % (settings.app_name) 
  secondary_input_path='/%s/reshard_secondary/data' % (settings.app_name)
  output_path= '%s/data' % (stage_path)
  num_map_jobs=5
  num_reduce_jobs=20    
  
  extra_properties = {'join.primary.input.dir' : primary_input_path,
                      'join.secondary.input.dir' : secondary_input_path,
                      'join.type' : 'restricted_inner_join',
                      'pert.recordwriter.comparator_name' : 'memcmp',
                      'pert.recordwriter.compression_codec_name' : 'snappy'}
  
  settings.run_pipes_job(bin_name, stage_name, primary_input_path, output_path, num_map_jobs, num_reduce_jobs, extra_properties)
  









