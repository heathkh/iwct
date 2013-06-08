#!/usr/bin/python

from snap.pyglog import *
from snap.deluge import scheduler
from snap.deluge import provenance 
from iw.flow.util import image_matcher_config
from iw import iw_pb2
from iw.matching.cbir import cbir_pb2
from iw.matching.cbir.full import full_pb2
from iw.flow.app.itergraph import itergraph_pb2
from iw.flow.app.lpbench import resultdb
from iw.flow.app.lpbench import flows
from iw.flow.app.lpbench import lpbench_pb2
from iw.flow.util import util
from iw import util as iwutil
from iw.eval.labelprop.eval1 import py_eval1
from iw.eval.labelprop.eval2 import py_eval2
from iw.eval.labelprop.eval2 import eval2_pb2       


def GenerateFullCbirFlannParams():
  params = []
  
  p = full_pb2.FlannParams()  
  p.algorithm = full_pb2.FlannParams.FLANN_INDEX_KDTREE
  p.kdtree_params.trees = 2
  p.kdtree_params.checks = 15
  params.append(p)
  
#  p = full_pb2.FlannParams()  
#  p.algorithm = full_pb2.FlannParams.FLANN_INDEX_KDTREE
#  p.kdtree_params.trees = 4
#  p.kdtree_params.checks = 32
#  params.append(p)
  
#  p = full_pb2.FlannParams()  
#  p.algorithm = full_pb2.FlannParams.FLANN_INDEX_KDTREE
#  p.kdtree_params.trees = 8
#  p.kdtree_params.checks = 64
#  params.append(p)
  
#  p = full_pb2.FlannParams()  
#  p.algorithm = full_pb2.FlannParams.FLANN_INDEX_KDTREE
#  p.kdtree_params.trees = 16
#  p.kdtree_params.checks = 128
#  params.append(p)
  
  return params
  
def GenerateFullCbirScorerFilterParams():
  params = []  
  # no geometric filter
  params.append((full_pb2.QueryScorerParams.NO_FILTER,  None))  
  
#  # simple scale filter version 1
#  wgc = full_pb2.WGCCorrespondenceFilterParams()
#  wgc.type = full_pb2.WGCCorrespondenceFilterParams.SCALE
#  wgc.scale_max = 4.0
#  wgc.scale_histogram_size = 50  
#  params.append((full_pb2.QueryScorerParams.WGC_FILTER, wgc))    
  return params

def GenerateFullCbirScorerParams():  
  params = []
  filter_params = GenerateFullCbirScorerFilterParams()    
  for filter_param in filter_params:
    p = full_pb2.QueryScorerParams()
    p.max_results = 100
    p.scoring = full_pb2.QueryScorerParams.ADAPTIVE
    p.normalization = full_pb2.QueryScorerParams.NRC      
    p.correspondence_filter = filter_param[0]
    if filter_param[0] == full_pb2.QueryScorerParams.WGC_FILTER: 
      p.wgc_correspondence_filter_params.CopyFrom(filter_param[1])             
    p.bursty_candidates = True
    params.append(p)
  return params

def GenerateFullCbirParams():  
  params = []
  scorer_params = GenerateFullCbirScorerParams()
  flann_params = GenerateFullCbirFlannParams()
  neighbors_per_shard_options = [20, 40, 80]
  for flann_param in flann_params:
    for scorer_param in scorer_params:      
      for neighbors_per_shard in neighbors_per_shard_options:
        cbir_params = cbir_pb2.CbirParams()
        
        cbir_params.full.num_index_shards = 1
        cbir_params.full.num_neighbors_per_index_shard = neighbors_per_shard
        cbir_params.full.scorer.CopyFrom(scorer_param)      
        cbir_params.full.flann.CopyFrom(flann_param)
        params.append(cbir_params)
  return params


def GenerateBowCbirParams():  
  params = []
  cbir_params = cbir_pb2.CbirParams()
  visual_vocab_size = 10000
  cbir_params.bow.visual_vocabulary_uri = 'maprfs://data/vv/clust_flickr60_k%d.pert' % (visual_vocab_size)
  cbir_params.bow.visual_vocab_size = visual_vocab_size
  cbir_params.bow.num_candidates = 100  
  cbir_params.bow.implementation = 'ctis'
  CHECK(cbir_params.IsInitialized())
  params.append(cbir_params)
  return params

def GenerateConfigurations(root_uri, dataset_name):  
  configs = []
  feature_types = [ #'ahess', 
                    #'sift', 
                    'usift'
                    ]
  cbir_params = []
  full_cbir_params = GenerateFullCbirParams()
  cbir_params.extend(full_cbir_params)
  #bow_cbir_params = GenerateBowCbirParams()
  #cbir_params.extend(bow_cbir_params)
  
  use_motif_expansion_types = [False]
  desired_features_per_megapixel_settings = [ #1500,
                                              3000,
                                              #6000
                                             ]
  root_sift_normalization = True
  for feature_type in feature_types:
    for desired_features_per_megapixel in desired_features_per_megapixel_settings:
      tune_flow = util.TuneFeatureExtractorDensityFlow(root_uri, dataset_name, feature_type, desired_features_per_megapixel)
      CHECK(scheduler.FlowScheduler(tune_flow).RunSequential())          
      tuned_feature_extractor_params = iw_pb2.DensityTunedFeatureExtractorParams()
      iwutil.LoadProtoFromUriOrDie(tuned_feature_extractor_params, tune_flow.GetOutput().GetUri())
      
      if tuned_feature_extractor_params.params.HasField('ocv_sift_params'):
        tuned_feature_extractor_params.params.ocv_sift_params.root_sift_normalization = root_sift_normalization
      elif tuned_feature_extractor_params.params.HasField('vgg_affine_sift_params'):
        tuned_feature_extractor_params.params.vgg_affine_sift_params.root_sift_normalization = root_sift_normalization
      else:
        LOG(FATAL, 'unexpected')  
      
      
      CHECK(tuned_feature_extractor_params.IsInitialized())
      for cbir_param in cbir_params:
        
        if cbir_param.HasField('bow') and feature_type != 'ahess':
          LOG(FATAL, 'bow cbir currently required ahess feature type without rootsift because of inria vv')
        
        if cbir_param.HasField('bow') and root_sift_normalization:
          LOG(FATAL, 'bow cbir currently required no rootsift because of inria vv')  
          
        
        for motif_expansion in use_motif_expansion_types:
          c = lpbench_pb2.Configuration()  
          c.dataset_name = dataset_name
          c.iter_graph.tuned_feature_extractor_params.CopyFrom(tuned_feature_extractor_params)
          c.iter_graph.image_matcher_config.CopyFrom(image_matcher_config.GetImageMatcherConfig(feature_type))
          c.iter_graph.cbir.CopyFrom(cbir_param)
          
          # Create template for a "local best" phase
          phase = itergraph_pb2.PhaseParams()  
          phase.max_match_candidates = 10000
          phase.max_vertex_degree = 1000
          phase.max_match_batch_size = 100
          phase.max_image_replication_factor = 100
          phase.type = itergraph_pb2.PhaseParams.PHASE_TYPE_CBIR_RANK  
          for _ in range(4):
            new_phase = c.iter_graph.phase.add()
            new_phase.CopyFrom(phase)
    
          if motif_expansion:
            motif_phase = c.iter_graph.phase.add()
            motif_phase.max_match_candidates = 50000
            motif_phase.max_vertex_degree = phase.max_vertex_degree
            motif_phase.max_match_batch_size = 100
            motif_phase.max_image_replication_factor = 100
            motif_phase.type = itergraph_pb2.PhaseParams.PHASE_TYPE_MOTIF_EXPANSION
            motif_phase.motif_expansion.background_features_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/motif_background_v01/usift/features.pert'
                         
          CHECK(c.IsInitialized())  
          configs.append(c)  
  return configs

def main():      
  dataset_name = 'tide_v12'
  root_uri = 'maprfs://data/lpbench/'
  configs = GenerateConfigurations(root_uri, dataset_name)  
  db = resultdb.ResultDatabase()  
  for config in configs:
    print config.iter_graph.tuned_feature_extractor_params.params
    if db.ResultReady(config):
      LOG(INFO, 'result for config is complete...')  
      continue
    else:
      LOG(INFO, 'generating results for config: %s' % (config))
    
    flow = flows.MainFlow(root_uri, config)
    CHECK(scheduler.FlowScheduler(flow).RunSequential())    
    matches = flow.matches
    
    matches_fp = matches.GetFingerprint()
    matches_provenance_list = provenance.BuildResourceProvenanceList(matches_fp)
    
    eval1 = py_eval1.LoadResultOrDie(flow.eval1.GetUri())
    eval2 = py_eval2.LoadResultOrDie(flow.eval2.GetUri())
    
    result = lpbench_pb2.ConfigurationResult()
    result.config.CopyFrom(config)
    result.eval1.CopyFrom(eval1)
    result.eval2.CopyFrom(eval2)
    
    result.matches_provenance_list.extend(matches_provenance_list) 
    CHECK(result.IsInitialized())
    db.SetResult(config, result)
    
  return 0

if __name__ == "__main__":        
  main()
    
    
    
    
    
  
