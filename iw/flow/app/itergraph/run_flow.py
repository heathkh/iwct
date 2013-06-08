#!/usr/bin/python
from snap.deluge import scheduler 
from iw.flow.app.itergraph import itergraph 
from iw.flow.app.itergraph import itergraph_pb2
from snap.pyglog import *
from iw.flow.util import image_matcher_config
from iw.matching.cbir import cbir_pb2
from iw.matching.cbir.full import full_pb2
from iw import iw_pb2
from iw import util as iwutil
from iw.flow.util import util





#def DefaultParamsTideV14():
#  params = itergraph_pb2.IterGraphParams()
#  params.feature_type = 'usift'
#  params.image_matcher_config.CopyFrom(image_matcher_config.GetImageMatcherConfig(params.feature_type))
#  
#  params.cbir.num_index_shards = 1
#  params.cbir.num_neighbors_per_index_shard = 40
#  params.cbir.scorer.max_results = 100
#  params.cbir.scorer.scoring = cbir_pb2.QueryScorerParams.ADAPTIVE
#  params.cbir.scorer.normalization = cbir_pb2.QueryScorerParams.NRC
#  params.cbir.scorer.correspondence_filter = cbir_pb2.QueryScorerParams.NO_FILTER    
#  params.cbir.scorer.bursty_candidates = True
#  
#  # Create template for a "local best" phase
#  local_best_phase = itergraph_pb2.PhaseParams()  
#  local_best_phase.max_match_candidates = 20000
#  local_best_phase.max_vertex_degree = 10
#  local_best_phase.max_match_batch_size = 100
#  local_best_phase.max_image_replication_factor = 100
#  local_best_phase.type = itergraph_pb2.PhaseParams.PHASE_TYPE_CBIR_RANK
#  
#  # Add 10 iterations of this phase
#  for _ in range(5):
#    new_phase = params.phase.add()
#    new_phase.CopyFrom(local_best_phase)
#    
#  CHECK(params.IsInitialized())
#  return params


def DefaultParamsTideV12(root_uri, dataset_name):
  feature_type = 'usift'
  desired_features_per_megapixel = 3000
  root_sift_normalization = True
  
  tune_flow = util.TuneFeatureExtractorDensityFlow(root_uri, dataset_name, feature_type, desired_features_per_megapixel, root_sift_normalization)
  CHECK(scheduler.FlowScheduler(tune_flow).RunSequential())
  tuned_feature_extractor_params = iw_pb2.DensityTunedFeatureExtractorParams()
  iwutil.LoadProtoFromUriOrDie(tuned_feature_extractor_params, tune_flow.GetOutput().GetUri())
  CHECK(tuned_feature_extractor_params.IsInitialized())
  
  params = itergraph_pb2.IterGraphParams()
  params.tuned_feature_extractor_params.CopyFrom(tuned_feature_extractor_params)
  params.image_matcher_config.CopyFrom(image_matcher_config.GetImageMatcherConfig(feature_type))
  params.cbir.full.num_index_shards = 2
  params.cbir.full.num_neighbors_per_index_shard = 40
  params.cbir.full.scorer.max_results = 100  
  params.cbir.full.scorer.scoring = full_pb2.QueryScorerParams.ADAPTIVE
  params.cbir.full.scorer.normalization = full_pb2.QueryScorerParams.NRC
  params.cbir.full.scorer.correspondence_filter = full_pb2.QueryScorerParams.NO_FILTER    
  params.cbir.full.scorer.bursty_candidates = True
  
  params.cbir.full.flann.algorithm = full_pb2.FlannParams.FLANN_INDEX_KDTREE
  params.cbir.full.flann.kdtree_params.trees = 4
  params.cbir.full.flann.kdtree_params.checks = 32
  
  # Create template for a "local best" phase
  local_best_phase = itergraph_pb2.PhaseParams()  
  local_best_phase.max_match_candidates = 10000
  local_best_phase.max_vertex_degree = 100
  local_best_phase.max_match_batch_size = 100
  local_best_phase.max_image_replication_factor = 100
  local_best_phase.type = itergraph_pb2.PhaseParams.PHASE_TYPE_CBIR_RANK
  
  for _ in range(2):
    new_phase = params.phase.add()
    new_phase.CopyFrom(local_best_phase)
    
  
#  motif_phase = params.phase.add()
#  motif_phase.max_match_candidates = 5*5000
#  motif_phase.max_vertex_degree = 10
#  motif_phase.max_match_batch_size = 100
#  motif_phase.max_image_replication_factor = 100
#  motif_phase.type = itergraph_pb2.PhaseParams.PHASE_TYPE_MOTIF_EXPANSION
#  motif_phase.motif_expansion.background_features_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/motif_background_v01/usift/features.pert'
  

      
  CHECK(params.IsInitialized(), str(params))
  return params


def DefaultParamsTideV16(root_uri, dataset_name):
  feature_type = 'usift'
  desired_features_per_megapixel = 3000
  root_sift_normalization = True  
  tune_flow = util.TuneFeatureExtractorDensityFlow(root_uri, dataset_name, feature_type, desired_features_per_megapixel)
  CHECK(scheduler.FlowScheduler(tune_flow).RunSequential())
  tuned_feature_extractor_params = iw_pb2.DensityTunedFeatureExtractorParams()
  iwutil.LoadProtoFromUriOrDie(tuned_feature_extractor_params, tune_flow.GetOutput().GetUri())
  tuned_feature_extractor_params.params.ocv_sift_params.root_sift_normalization = root_sift_normalization
  CHECK(tuned_feature_extractor_params.IsInitialized())
  params = itergraph_pb2.IterGraphParams()
  params.tuned_feature_extractor_params.CopyFrom(tuned_feature_extractor_params)
  params.image_matcher_config.CopyFrom(image_matcher_config.GetImageMatcherConfig(feature_type))
  params.cbir.full.num_index_shards = 1
  params.cbir.full.num_neighbors_per_index_shard = 80
  params.cbir.full.scorer.max_results = 100
  params.cbir.full.scorer.scoring = full_pb2.QueryScorerParams.ADAPTIVE
  params.cbir.full.scorer.normalization = full_pb2.QueryScorerParams.NRC
  params.cbir.full.scorer.correspondence_filter = full_pb2.QueryScorerParams.NO_FILTER    
  params.cbir.full.scorer.bursty_candidates = True  
  params.cbir.full.flann.algorithm = full_pb2.FlannParams.FLANN_INDEX_KDTREE
  params.cbir.full.flann.kdtree_params.trees = 4
  params.cbir.full.flann.kdtree_params.checks = 32
  
  # Create template for a "local best" phase
  local_best_phase = itergraph_pb2.PhaseParams()  
  local_best_phase.max_match_candidates = 12000
  local_best_phase.max_vertex_degree = 100
  local_best_phase.max_match_batch_size = 100
  local_best_phase.max_image_replication_factor = 100
  local_best_phase.type = itergraph_pb2.PhaseParams.PHASE_TYPE_CBIR_RANK
  
  for _ in range(15):
    new_phase = params.phase.add()
    new_phase.CopyFrom(local_best_phase)
  
  CHECK(params.IsInitialized(), str(params))
  return params

#def DefaultParamsTideV08():
#  
#  params = itergraph_pb2.IterGraphParams()
#  
#  params.image_matcher_config.CopyFrom(image_matcher_config.GetImageMatcherConfig(params.feature_type))
#  params.remove_borders = True
#  params.cbir.num_index_shards = 4
#  params.cbir.num_neighbors_per_index_shard = 25
#  params.cbir.scorer.max_results = 100
#  params.cbir.scorer.scoring = cbir_pb2.QueryScorerParams.ADAPTIVE
#  params.cbir.scorer.normalization = cbir_pb2.QueryScorerParams.NRC
#  params.cbir.scorer.correspondence_filter = cbir_pb2.QueryScorerParams.NO_FILTER    
#  params.cbir.scorer.bursty_candidates = True
#  
#  params.cbir.flann.algorithm = cbir_pb2.FlannParams.FLANN_INDEX_KDTREE
#  params.cbir.flann.kdtree_params.trees = 4
#  params.cbir.flann.kdtree_params.checks = 32
#  
#  # Create template for a "local best" phase
#  local_best_phase = itergraph_pb2.PhaseParams()  
#  local_best_phase.max_match_candidates = 400000
#  local_best_phase.max_vertex_degree = 20
#  local_best_phase.max_match_batch_size = 150
#  local_best_phase.max_image_replication_factor = 200
#  local_best_phase.type = itergraph_pb2.PhaseParams.PHASE_TYPE_CBIR_RANK
#  
#  # Add iterations of cbir rank phase
#  for _ in range(40):
#    new_phase = params.phase.add()
#    new_phase.CopyFrom(local_best_phase)
#    
#  CHECK(params.IsInitialized(), str(params))
#  return params

def GetParamsForDataset(root_uri, dataset_name):
  params = None
  if dataset_name == 'tide_v12':
    params = DefaultParamsTideV12(root_uri, dataset_name)
#  elif dataset_name == 'tide_v14':
#    params = DefaultParamsTideV14()    
#  elif dataset_name == 'tide_v14_mixed':
#    params = DefaultParamsTideV14()  
#    params.remove_borders = False
#  elif dataset_name == 'tide_v14_mixed_v2':
#    params = DefaultParamsTideV14()  
#    params.remove_borders = False
#  elif dataset_name == 'tide_v16':
#    params = DefaultParamsTideV14()    
  elif dataset_name == 'tide_v16_mixed':
    params = DefaultParamsTideV16(root_uri, dataset_name)  
  elif dataset_name == 'tide_v17':
    params = DefaultParamsTideV16(root_uri, dataset_name)
  elif dataset_name == 'tide_v18_mixed':
    params = DefaultParamsTideV16(root_uri, dataset_name)  
#  elif dataset_name == 'tide_v08':
#    params = DefaultParamsTideV08()
#  elif dataset_name == 'tide_v08_distractors':
#    params = DefaultParamsTideV08()    
  else:
    LOG(FATAL, 'unknown dataset: %s' % (dataset_name))
  
  CHECK(params.IsInitialized(), str(params))
  return params


def main():      
  dataset_name = 'tide_v18_mixed'
  base_uri = 'maprfs://data/itergraph/'
  params = GetParamsForDataset(base_uri, dataset_name)
  root_uri = '%s/%s/' % (base_uri, dataset_name)
  fs = scheduler.FlowScheduler(itergraph.MainFlow(root_uri, params))
  #fs.RunParallel()    
  fs.RunSequential()
  return 0

if __name__ == "__main__":        
  main()
    
    
    
    
    
  
