#!/usr/bin/python

import shutil
import math
from snap.deluge import core
from snap.deluge import mr
from pert import py_pert
from pyglog import *
#from iw.matching.cbir import util as cbirutil
from iw import util as iwutil
from iw import iw_pb2
from iw.matching.cbir import cbir_pb2
from iw.matching.cbir.full import full_pb2
from iw import py_imagegraph
from iw import py_imageregiongraph
from iw.eval.labelprop.eval1 import py_eval1
from iw.eval.labelprop.eval2 import py_eval2
from iw.eval.labelprop.eval2 import eval2_pb2       
from iw.matching import feature_extractor_density as fed
from iw.matching.cbir.bow import bow_pb2
#from iw.matching.cbir.bow.inria import py_inria
from iw.matching.cbir.bow.ctis import py_ctis

class FindDuplicatesFlow(core.PipesFlow):
  """Emits key, count for all keys that occur more than once.
  """  
  def __init__(self, output_uri, input):
    super(FindDuplicatesFlow, self).__init__()
    self.AddInput('input', input)    
    self.AddOutput('duplicated_keys', core.PertResource(self, '%s/duplicated_keys.pert' % (output_uri)))
    self.SetPipesBinary(__file__, 'mr_find_duplicates')    
    return

class CropAndScaleImagesFlow(core.PipesFlow):
  """Crops the border of the image which may contain distractor decorations.
  Produces the same number of output shards as input.
  """  
  def __init__(self, output_uri, images, scale):
    super(CropAndScaleImagesFlow, self).__init__()
    LOG(INFO, images.GetFlow())
    self.parameters['scale'] = scale
    self.AddInput('images', images)    
    self.AddOutput('cropped_images', core.PertResource(self, '%s/cropped_scaled_photoid_to_image.pert' % (output_uri)))
    self.SetPipesBinary(__file__, 'mr_crop_border')
    return
  
  def PreRunConfig(self):
    self.num_reduce_jobs = mr.GetNumActiveTaskTrackers()*2
    return

class ScrubImagesFlow(core.PipesFlow):
  """Takes raw jpeg image data as input and crops and resizes as needed or drops
     the image if the requirements can't be satisfied.
  """  
  def __init__(self, output_uri, raw_images, crop_fraction, min_dimension_pixels, min_area_pixels, max_area_pixels):
    super(ScrubImagesFlow, self).__init__()    
    self.parameters['crop_fraction'] = crop_fraction
    self.parameters['min_dimension_pixels'] = min_dimension_pixels
    self.parameters['min_area_pixels'] = min_area_pixels
    self.parameters['max_area_pixels'] = max_area_pixels
    
    self.AddInput('raw_images', raw_images)    
    self.AddOutput('scrubbed_images', core.PertResource(self, '%s/photoid_to_image.pert' % (output_uri)))
    self.SetPipesBinary(__file__, 'mr_scrub_images')
    self.num_reduce_jobs = py_pert.GetNumShards(raw_images.GetUri())
    return
            

class ExtractFeaturesFlow(core.PipesFlow):  
  def __init__(self, resource_prefix, images, feature_extractor_params):
    super(ExtractFeaturesFlow,self).__init__()
    CHECK(feature_extractor_params.IsInitialized())
    resource_prefix = str(resource_prefix)    
    self.SetPipesBinary(__file__, 'mr_extract_features')    
    self.AddInput('images', images)    
    if not py_pert.Exists(resource_prefix):
      writer = py_pert.StringTableShardWriter()
      writer.Open('%s/dummy.foo' % (resource_prefix))
      writer.Close()        
    self.AddOutput('features', core.PertResource(self, "%s/features.pert" % resource_prefix) )
    self.output_chunk_size_bytes = 1024 * (2**20) # 1 GB is max    
    self.AddParam('feature_extractor_params', feature_extractor_params)
    return
  
  def PreRunConfig(self):
    self.num_reduce_jobs = mr.GetNumActiveTaskTrackers()*2    
    return


class CountFeaturesFlow(core.PipesFlow):  
  def __init__(self, feature_uri_base, features):
    super(CountFeaturesFlow,self).__init__()
    self.SetPipesBinary(__file__, 'mr_cbir_count_features')
    self.num_reduce_jobs = 1
    self.AddInput('features', features)   
    self.AddOutput('feature_count', core.PertResource(self, '%s/feature_counts.pert' % feature_uri_base) )    
    return


def GetNumImagesAndFeatures(feature_counts_uri):
    reader = py_pert.StringTableShardSetReader()
    CHECK(reader.Open(feature_counts_uri))    
    num_images = 0
    num_features = 0
    count = full_pb2.Count()
    for k, v in reader:
      count.ParseFromString(v)
      num_features += count.value
      num_images += 1
    return num_images, num_features
  
import fnmatch
  
#class IndexShardDirectoryResource(core.DirectoryResource):
#  """ Directory resource that creates a fingerprint.pert file. """
#  def __init__(self, flow, uri, check_exists = False, is_generated = True):    
#    super(IndexShardDirectoryResource,self).__init__(flow, uri, check_exists, is_generated)
#    return
#    
#  def GetFingerprint(self):
#    self.CheckExists()
#    fingerprint = None    
#    # get list all the uris in the directory 
#    ok, uris = py_pert.ListDirectory(self.uri)
#    CHECK(ok)    
#    CHECK(uris)
#    
#    fingerprint_uris = fnmatch.filter(uris, '*/fingerprint.pert')
#    CHECK_EQ(len(fingerprint_uris), 1)
#    fingerprint_uri = fingerprint_uris[0]
#    CHECK(py_pert.Exists(fingerprint_uri))
#    
#    reader = py_pert.StringTableShardReader()
#    CHECK(reader.Open(fingerprint_uri))
#    key, value = reader.Next()
#    CHECK_EQ(len(value), 32)
#    fingerprint = value
#    return fingerprint
#  
#  def __repr__(self):
#    return '<IndexShardDirectoryResource uri=%s>' % (self.uri)  
  
  
class CbirCreateIndexShardsFlow(core.PipesFlow):
  def __init__(self, root_uri, features, feature_counts, num_shards, flann_params):
    super(CbirCreateIndexShardsFlow, self).__init__()
    self.num_shards = num_shards
    # Specify resource dependencies
    self.AddInput('features', features)
    self.AddInput('feature_counts', feature_counts)
    self.input_path = self.GetInput('features').GetUri()
    index_base_uri = '%s/index_shards/' % (root_uri)
    
    for shard_num in range(self.num_shards):
      index_shard_uri = '%s/index_part_%05d/' % (index_base_uri, shard_num)
      self.AddOutput('index_shard_%d' % (shard_num), core.DirectoryResource(self, index_shard_uri))    
    
    self.output_path = index_base_uri
        
    # Modify PipesFlow defaults
    self.SetPipesBinary(__file__, 'mr_cbir_create_index_shards')
    
    self.AddParam('flann_params', flann_params)
    
    # reduce stage is memory intensive, limit number of simultaneous reducers    
    self.reduce_slots_per_node = 1 # at most one reduce slot per node
    self.parameters['mapred.reduce.tasks.speculative.execution']='false'  # don't allow speculative execution which might launch duplicate reducers
    
    # the reduce task could be quite slow... make sure framework doesn't kill it
    timeout_min = 120
    timeout_ms = 60000*timeout_min
    self.parameters['mapred.task.timeout'] = str(timeout_ms) 
    
    #self.parameters['profiler'] = 'on'
        
    # override the default ram allocation for the JVM to leave ram for pipes reducer
    #self.parameters['mapred.child.java.opts'] = '-Xmx512m'
    #self.parameters['mapred.map.child.java.opts'] = '-Xmx512m'
    #self.parameters['mapred.reduce.child.java.opts'] = '-Xmx512m'    
    return
  
  def GetNumFeatures(self):
    reader = py_pert.StringTableShardSetReader()
    CHECK(reader.Open(self.GetInput('feature_counts').GetUri()))
    count = full_pb2.Count()
    num_features = 0
    for k, v in reader:
      count.ParseFromString(v)
      CHECK(count.IsInitialized())
      num_features += count.value      
    return num_features 
    
  
  def GetIndexShards(self):
    index_shards = []
    for i in range(self.num_shards):
      index_shards.append(self.GetOutput('index_shard_%d' % (i)))
    return index_shards
  
#  def GetNumImagesAndFeatures(self):
#    reader = py_pert.StringTableShardSetReader()
#    CHECK(reader.Open(self.GetInput('feature_counts').GetUri()))    
#    num_images = 0
#    num_features = 0
#    count = full_pb2.Count()
#    for k, v in reader:
#      count.ParseFromString(v)
#      num_features += count.value
#      num_images += 1
#    return num_images, num_features
  
  def ComputeMinNumIndexShards(self):
    #num_images, num_features = self.GetNumImagesAndFeatures()
    num_images, num_features = GetNumImagesAndFeatures(self.GetInput('feature_counts').GetUri())
    avg_features_per_image = float(num_features)/num_images
    
    num_jobs_per_node = self.reduce_slots_per_node
    #flann_bytes_per_feature = (420609904.0/1000000)*1.75 # flann benchmark values
    flann_bytes_per_feature = (420609904.0/1000000) # flann benchmark values
    ram_per_node_bytes = mr.GetGBRamPerTaskTracker() * (2 ** 30)
    desired_index_size_bytes = 0.7*ram_per_node_bytes/num_jobs_per_node  # MapR leaves about 75% of ram for MR... Shoot to fill 70%
    features_per_index = desired_index_size_bytes/flann_bytes_per_feature
    images_per_index =  features_per_index/avg_features_per_image
    min_num_partitions_ram_limit = int(math.ceil(num_images/images_per_index)) 
    print 'min_num_partitions_ram_limit: %d' % (min_num_partitions_ram_limit)
    return min_num_partitions_ram_limit
  
  def PreRunConfig(self):
    min_num_shards = self.ComputeMinNumIndexShards()    
    CHECK_GE(self.num_shards, min_num_shards, 'Increase param: cbir.full.num_index_shards')
    print 'num_shards: %d' % (self.num_shards)
    self.num_reduce_jobs = self.num_shards
    
    total_num_features = self.GetNumFeatures()
    LOG(INFO, 'total_num_features: %d' % (total_num_features))
    estimated_features_per_index = long(float(total_num_features)/self.num_shards*1.25)
    self.parameters['estimated_features_per_index'] = estimated_features_per_index
    print 'estimated_features_per_index: %d' % (estimated_features_per_index)
    
    return


class CbirQueryIndexMetaFlow:
  def __init__(self, root_uri, features, index_shards, num_neighbors_per_shard, num_index_shards):
    self.query_results = []    
    self.query_index_shard_flows = []
    for shard_num in range(num_index_shards):
      index_shard = index_shards[shard_num]
      cur_query_flow = CbirQueryIndexShardFlow(root_uri, features, index_shard, shard_num, num_neighbors_per_shard)
      self.query_index_shard_flows.append(cur_query_flow)
      self.query_results.append(cur_query_flow.GetOutput())
    self.merge_flow = CbirMergeQueryResultShardsFlow(root_uri, self.query_results)
    self.output = self.merge_flow.GetOutput()
    return
  
  
class CbirQueryIndexShardFlow(core.PipesFlow):
  def __init__(self, root_uri, features, index_shard, shard_num, num_neighbors_per_shard, ):
    super(CbirQueryIndexShardFlow, self).__init__()
    # Modify PipesFlow defaults
    self.SetPipesBinary(__file__, 'mr_cbir_query_index_shard')
    # Specify resource dependencies
    CHECK(isinstance(num_neighbors_per_shard, int), 'expected integer but got: %f' % num_neighbors_per_shard)
    self.AddInput('features', features, is_primary=True)
    self.AddInput('index_shard', index_shard, add_to_cache=True) 
    self.AddOutput('query_results', core.PertResource(self, "%s/query_results/shard_%05d.pert" % (root_uri, shard_num)))
    # set the required parameters the MR job expects to find    
    
    self.parameters['num_neighbors_per_shard'] = num_neighbors_per_shard
    # only include keypoints when querying the first shard
    include_keypoints = 0
    if shard_num == 0:
      include_keypoints = 1    
    self.parameters['include_keypoints'] = include_keypoints
    # deal with fact that map stage is memory intensive
    # make sure only a few mappers runs on a node
    self.map_slots_per_node = 1
    self.desired_splits_per_map_slot = 8.0;
    # override the default ram allocation for the JVM to leave ram for pipes 
    self.parameters['mapred.child.java.opts'] = '-Xmx1024m'
    self.parameters['mapred.map.child.java.opts'] = '-Xmx1024m'
    self.parameters['mapred.reduce.child.java.opts'] = '-Xmx1024m'    
    self.parameters['mapred.map.tasks.speculative.execution']='false'  # don't allow speculative execution which might launch duplicate mappers
    # the task could be quite slow... make sure framework doesn't kill it
    timeout_min = 120
    timeout_ms = 60000*timeout_min
    self.parameters['mapred.task.timeout'] = str(timeout_ms)    
    #self.parameters['profiler'] = 'on'
    #self.parameters['profile_timeout_sec'] = '600'
    return
  
  def PreRunConfig(self):
    self.num_reduce_jobs = mr.GetNumActiveTaskTrackers()*2
    return
  

class CbirMergeQueryResultShardsFlow(core.PipesFlow):
  def __init__(self, root_uri, query_results):
    super(CbirMergeQueryResultShardsFlow, self).__init__()
    # Specify resource dependencies
    CHECK_GT(len(query_results), 0)
    
    # create comma seperated list of input uris
    input_uris = []
    for i, query_result in enumerate(query_results):
      self.AddInput('query_results_%d' % i, query_result)
      input_uris.append(query_result.GetUri())    
    self.input_path = ','.join(input_uris)
     
    self.AddOutput('merged_query_results', core.PertResource(self, "%s/merged_query_results.pert" % (root_uri) ))    
            
    # Modify PipesFlow defaults
    self.SetPipesBinary(__file__, 'mr_cbir_merge_query_results')
    
    # override the default ram allocation for the JVM to leave ram for pipes mapper
    self.parameters['mapred.child.java.opts'] = '-Xmx1024m'
    self.parameters['mapred.reduce.child.java.opts'] = '-Xmx4024m'
    self.parameters['mapred.reduce.tasks.speculative.execution']='false'    
    return
  
  def PreRunConfig(self):
    self.num_reduce_jobs = mr.GetNumReduceSlots()*2
    return
    

class CbirScoreResultsFlow(core.PipesFlow):  
  def __init__(self, resource_prefix, neighbors, feature_counts, query_scorer_params):
    super(CbirScoreResultsFlow,self).__init__()
    self.resource_prefix = resource_prefix
    # Configure PipesFlow parent class   
    self.SetPipesBinary(__file__, 'mr_cbir_score_results')        
    self.AddInput('neighbors', neighbors, is_primary=True)
    self.AddInput('feature_counts', feature_counts, add_to_cache=True)
    self.AddOutput('cbir_results', core.PertResource(self, "%s/cbir_results.pert" % resource_prefix) )
    #self.AddOutput('cbir_config', core.FileResource(self, "%s/cbir_config.txt" % resource_prefix) )
    self.config_pert = core.FileResource(self, "%s/cbir_config.txt" % resource_prefix)
    self.parameters['feature_counts_uri'] = feature_counts.GetUri() 
    CHECK(query_scorer_params.IsInitialized()) 
    self.parameters['query_scorer_params'] = mr.Base64EncodeProto(query_scorer_params) #TODO(heathkh): use getter / setters to simplify setting proto paramaters correctly      
    # since we have multiple inputs...
    self.query_scorer_params = query_scorer_params
    timeout_min = 90
    timeout_ms = 60000*timeout_min
    self.parameters['mapred.task.timeout'] = str(timeout_ms)
    #self.parameters['profiler'] = 'on'
    #self.parameters['profile_timeout_sec'] = '60'
    return

  def PreRunConfig(self):
    output_uri = self.config_pert.GetUri()
    local_path = mr.UriToNfsPath(output_uri)    
    iwutil.EnsureParentPathExists(local_path)     
    out = open(local_path, 'w')
    out.write(str(self.query_scorer_params))
    out.close()    
    return


class CreateOrderedMatchCandidatesMetaFlow(object):  
  def __init__(self, base_uri, ordering_method, cbir_results):
    super(CreateOrderedMatchCandidatesMetaFlow, self).__init__()
    valid_orderings = ['INCREASING_BY_CBIR_RANK', 'DECREASING_BY_CBIR_SCORE']        
    resource_prefix = None
    if ordering_method == 'INCREASING_BY_CBIR_RANK':
      resource_prefix = '%s/dec_by_rank/' % (base_uri)
    elif ordering_method == 'DECREASING_BY_CBIR_SCORE':
      resource_prefix = '%s/inc_by_score/' % (base_uri)
    else:
      LOG(FATAL, 'must be in %s, you provided %s' % (valid_orderings, ordering_method))       
    unique_flow = CreateUniqueMatchCandidatesFlow(resource_prefix, ordering_method, cbir_results)
    sort_flow = SortMatchCandidatesFlow(resource_prefix, ordering_method, unique_flow.GetOutput())    
    self.candidates = sort_flow.GetOutput() 
    return


class CreateUniqueMatchCandidatesFlow(core.PipesFlow):  
  def __init__(self, resource_prefix, ordering_method, cbir_results):
    super(CreateUniqueMatchCandidatesFlow, self).__init__()
    self.SetPipesBinary(__file__, 'mr_create_unique_match_candidates')    
    self.AddInput('cbir_results', cbir_results)
    self.AddOutput('match_candidates', core.PertResource(self, "%s/unqiue_match_candidates.pert" % resource_prefix) )
    self.parameters['ordering_method'] = ordering_method    
    return
  
  def PreRunConfig(self):
    self.num_reduce_jobs = mr.GetNumActiveTaskTrackers()*2
    return
          

class SortMatchCandidatesFlow(core.PipesFlow):  
  def __init__(self, resource_prefix,ordering_method, match_candidates):
    super(SortMatchCandidatesFlow, self).__init__()
    self.SetPipesBinary(__file__, 'mr_sort_match_candidates')        
    self.AddInput('match_candidates', match_candidates)
    self.AddOutput('sorted_match_candidates', core.PertResource(self, "%s/sorted_match_candidates.pert" % resource_prefix) )
    self.parameters['ordering_method'] = ordering_method
    return  

  def PreRunConfig(self):
    self.num_reduce_jobs = mr.GetNumActiveTaskTrackers()*2
    return
          

class MatchBatchesFlow(core.MapJoinPipesFlow):  
  def __init__(self, resource_prefix, features, sorted_match_batches, image_matcher_config, match_phase):
    super(MatchBatchesFlow, self).__init__()
    self.reserve_all_resources = True  # keep scheduler from running other jobs at same time
    self.SetPipesBinary(__file__, 'mr_match_batches')
    self.output_is_sorted = False
    self.AddInput('features', features)
    self.AddInput('sorted_match_batches', sorted_match_batches)
    self.AddOutput('matches', core.PertResource(self, "%s/matches.pert" % resource_prefix) )
    self.primary_input_uri = self.GetInput('features').GetUri()
    self.secondary_input_uri = self.GetInput('sorted_match_batches').GetUri()
    self.AddParam('image_matcher_config_proto', image_matcher_config)
    self.AddParam('match_phase', match_phase)
    return
  
  def PreRunConfig(self):
    sorted_match_matches_uri = self.GetInput('sorted_match_batches').GetUri()
    CHECK(py_pert.Exists(sorted_match_matches_uri), 'expected uri to exist: %s' % (sorted_match_matches_uri))
    return


class RecordParamsFlow(core.Flow):  
  def __init__(self, resource_prefix, itergraph_params, dummy_input):
    super(RecordParamsFlow,self).__init__()
    self.AddInput('dummy_input', dummy_input)  # every flow must have an input... this is a dummy input that will trigger the generation of the proto record
    self.itergraph_params = itergraph_params
    self.AddOutput('merged_matches', core.FileResource(self, "%s/itergraph_params.txt" % resource_prefix) )    
    return
    
  def Run(self):    
    output_uri = self.GetOutput().GetUri()
    local_path = mr.UriToNfsPath(output_uri)    
    iwutil.EnsureParentPathExists(local_path)
    out = open(local_path, 'w')
    out.write(str(self.itergraph_params))
    out.close()
    return


class MergeMatchesFlow(core.PipesFlow):  
  def __init__(self, resource_prefix, matches_list):
    super(MergeMatchesFlow,self).__init__()
    self.SetPipesBinary(__file__, 'mr_merge_matches')
    CHECK_GE(len(matches_list), 1) # make sure this is a list of resources
    
    input_uris = []
    for i, matches in enumerate(matches_list):
      CHECK(isinstance(matches, core.Resource))
      self.AddInput('matches_%d' % i, matches)
      input_uris.append(matches.GetUri())
    self.input_path = ','.join(input_uris)
    self.AddOutput('merged_matches', core.PertResource(self, "%s/merged_matches.pert" % resource_prefix) )    
    
    #self.num_reduce_jobs = 1
    #self.num_reduce_jobs = mr.GetNumActiveTaskTrackers()*2
    return
  
  def PreRunConfig(self):
    self.num_reduce_jobs = mr.GetNumActiveTaskTrackers()*2
    return
  
class SortFlow(core.PipesFlow):  
  def __init__(self, input, output_uri, num_shards):
    super(SortFlow,self).__init__()
    self.SetPipesBinary(__file__, 'mr_sort')
    self.num_reduce_jobs = num_shards
    self.AddInput('input', input)   
    self.AddOutput('output', core.PertResource(self, output_uri) )    
    return


class MatchesToImageGraphFlow(core.Flow):  
  def __init__(self, base_uri, matches, photoids):
    super(MatchesToImageGraphFlow, self).__init__()
    self.AddInput('matches', matches )    
    self.AddInput('photoids', photoids )        
    self.AddOutput('image_graph', core.PertResource(self, "%s/image_graph.pert" % base_uri)  )    
    return
  
  def Run(self):
    matches_uri = self.GetInput('matches').GetUri()
    photoids_uri = self.GetInput('photoids').GetUri()
    image_graph_uri = self.GetOutput().GetUri()
    ok, ig = py_imagegraph.CreateImageGraph2(matches_uri, photoids_uri)
    CHECK(ok)
    py_imagegraph.SaveImageGraph(ig, image_graph_uri)    
    CHECK(py_pert.Exists(image_graph_uri))
    return
    

    

class LabelpropEval1Flow(core.Flow):  
  def __init__(self, base_uri, image_graph, tide):
    super(LabelpropEval1Flow, self).__init__()
    self.AddInput('image_graph', image_graph )
    self.AddInput('tide', tide )        
    self.AddOutput('labelprop_eval1', core.PertResource(self, "%s/labelprop_eval1.pert" % base_uri)  )    
    return
  
  def Run(self):
    image_graph_uri = self.GetInput('image_graph').GetUri()
    tide_uri = self.GetInput('tide').GetUri()
    eval = py_eval1.EvaluationRunner(image_graph_uri, tide_uri)
    num_training_images = 100
    num_trials = 25
    ok, results = eval.Run(num_training_images, num_trials);    
    results_uri = self.GetOutput().GetUri()
    py_eval1.SaveResultOrDie(results, results_uri)
    
    return




class MatchesToImageRegionGraphFlow(core.Flow):  
  def __init__(self, base_uri, matches, tide):
    super(MatchesToImageRegionGraphFlow, self).__init__()
    self.AddInput('matches', matches )    
    self.AddInput('tide', tide )        
    self.AddOutput('image_region_graph', core.PertResource(self, "%s/image_region_graph.pert" % base_uri)  )    
    return
  
  def Run(self):
    matches_uri = self.GetInput('matches').GetUri()
    image_region_graph_uri = self.GetOutput().GetUri()
    tide_uri = self.GetInput('tide').GetUri()
    
    #ok, irg = py_imageregiongraph.CreateImageRegionGraph(matches_uri, tide_uri)
    #CHECK(ok)
    #py_imageregiongraph.SaveImageRegionGraph(irg, image_region_graph_uri)
    
    ok = py_imageregiongraph.CreateAndSaveImageRegionGraph(matches_uri, tide_uri, image_region_graph_uri)
    CHECK(ok)
        
    CHECK(py_pert.Exists(image_region_graph_uri))
    return


 

class LabelpropEval2Flow(core.Flow):  
  def __init__(self, base_uri, image_region_graph, tide):
    super(LabelpropEval2Flow, self).__init__()
    self.AddInput('image_region_graph', image_region_graph )
    self.AddInput('tide', tide )        
    self.AddOutput('labelprop_eval2', core.PertResource(self, "%s/labelprop_eval2.pert" % base_uri)  )    
    return
  
  def Run(self):
    image_graph_uri = self.GetInput('image_region_graph').GetUri()
    tide_uri = self.GetInput('tide').GetUri()
    eval = py_eval2.EvaluationRunner(image_graph_uri, tide_uri)
    
    params = eval2_pb2.Params()
    params.num_training_images = 100
    params.frac_aux_images = 1.0
    params.num_trials = 20    
    params.supervision = eval2_pb2._PARAMS_SUPERVISIONTYPE.values_by_name['LOCALIZATION'].number
    params.min_score_threshold = 2e-6 # if a score is less than this, we don't try to guess
    params.uniqueness_threshold = 1.0 # number of orders of magnitude by which the first best score must exceed the second best score
    
    ok, results = eval.Run(params)   
    CHECK(ok)
    results_uri = self.GetOutput().GetUri()
    py_eval2.SaveResultOrDie(results, results_uri)
    
    return
  
  
class TuneFeatureExtractorDensityFlow(core.Flow):
  def __init__(self, base_uri, dataset_name, feature_type, desired_features_per_megapixel):
    super(TuneFeatureExtractorDensityFlow, self).__init__()
    images_uri = '%s/%s/photoid_to_image.pert' % (base_uri, dataset_name)
    self.dataset_name = dataset_name
    self.feature_type = feature_type
    self.desired_features_per_megapixel = desired_features_per_megapixel
    self.images = core.PertResource(self, images_uri, check_exists=True, is_generated = False)
    self.AddOutput('tuned_params', core.PertResource(self, "%s/%s/%s_fpm%s.pert" % (base_uri, dataset_name, feature_type, desired_features_per_megapixel)))
    
    p = iw_pb2.FeatureExtractorParams()
    if feature_type == 'sift':      
      p.ocv_sift_params.num_octave_layers = 3
      p.ocv_sift_params.contrast_threshold = 0.04
      p.ocv_sift_params.edge_threshold = 30
      p.ocv_sift_params.sigma = 1.2
      p.ocv_sift_params.upright = False
      p.ocv_sift_params.root_sift_normalization = False
      CHECK(p.ocv_sift_params.IsInitialized())
    elif feature_type == 'usift':  
      p.ocv_sift_params.num_octave_layers = 3
      p.ocv_sift_params.contrast_threshold = 0.04
      p.ocv_sift_params.edge_threshold = 30
      p.ocv_sift_params.sigma = 0.6
      p.ocv_sift_params.upright = True
      p.ocv_sift_params.root_sift_normalization = False
      CHECK(p.ocv_sift_params.IsInitialized())      
    elif feature_type == 'ahess':
      p.vgg_affine_sift_params.type = p.vgg_affine_sift_params.AFFINE_HESSIAN
      p.vgg_affine_sift_params.threshold = 200
      p.vgg_affine_sift_params.root_sift_normalization = False
      CHECK(p.vgg_affine_sift_params.IsInitialized())
    else:
      LOG(FATAL, 'unknown feature type specified: %s' % (feature_type))
    
    self.initial_extractor_params = p
    return
  
  def Run(self):
    images_uri = self.images.GetUri()
    tuned_params_output_uri = self.GetOutput().GetUri()
    extractor_params = fed.TuneFeatureExtractorParamsOrDie(images_uri, self.initial_extractor_params, self.desired_features_per_megapixel)
    #print extractor_params
    tuned_params = iw_pb2.DensityTunedFeatureExtractorParams()
    tuned_params.dataset_name = self.dataset_name
    tuned_params.desired_density = self.desired_features_per_megapixel
    tuned_params.params.CopyFrom(extractor_params)
    iwutil.SaveProtoToUriOrDie(tuned_params, tuned_params_output_uri)
    return


#class CreateCbirBowIndexFlow(core.Flow):
#  def __init__(self, resource_prefix, bow, visual_vocab):
#    super(CreateCbirBowIndexFlow,self).__init__()
#    self.AddInput('bow', bow)
#    self.AddInput('visual_vocab', visual_vocab)
#    self.index_base_uri = "%s/bow_index" % resource_prefix
#    self.AddOutput('bow_index', core.DirectoryResource(self, self.index_base_uri) )        
#    self.binary = '%s/%s' % (os.path.dirname(__file__), 'create_inria_bow_index')    
#    return
#  
#  def Run(self):             
#    bow_uri = self.GetInput('bow').GetUri()
#    vv_uri = self.GetInput('visual_vocab').GetUri()
#    temp_ivf_filepath = tempfile.mkdtemp()
#    cmd = '%s --bow_uri=%s --vv_uri=%s  --ivf_uri=%s' % ( self.binary, bow_uri, vv_uri, 'local://' + temp_ivf_filepath )
#    print cmd    
#    output = mr.RunAndGetOutput(cmd, shell=True)
#    assert(os.path.exists(temp_ivf_filepath + '/index.ivf'))
#    assert(os.path.exists(temp_ivf_filepath + '/index.ivfids'))
#    py_pert.Remove(self.index_base_uri)
#    mr.CopyUri('local://' + temp_ivf_filepath + '/index.ivf', self.index_base_uri + '/index.ivf')
#    mr.CopyUri('local://' + temp_ivf_filepath + '/index.ivfids', self.index_base_uri + '/index.ivfids')
#    assert(self.GetOutput().Exists())
#    # dir contents must also exist
#    assert(py_pert.Exists(self.index_base_uri + '/index.ivf'))
#    assert(py_pert.Exists(self.index_base_uri + '/index.ivfids'))
#    shutil.rmtree(temp_ivf_filepath, ignore_errors=True)
#    return True   

class CreateCbirBowIndexFlow(core.Flow):
  def __init__(self, resource_prefix, bow, visual_vocab, cbir_bow_params):
    super(CreateCbirBowIndexFlow,self).__init__()
    self.AddInput('bow', bow)
    self.AddInput('visual_vocab', visual_vocab)
    self.index_base_uri = "%s/bow_index" % resource_prefix
    self.AddOutput('bow_index', core.DirectoryResource(self, self.index_base_uri))
    self.cbir_bow_params = cbir_bow_params            
    return
  
  def Run(self):             
    bow_uri = self.GetInput('bow').GetUri()
    reader = py_pert.StringTableReader()    
    CHECK(reader.Open(bow_uri))    
    visual_vocab_size = self.cbir_bow_params.visual_vocab_size
    num_docs = reader.Entries()
    index = None
    if self.cbir_bow_params.implementation == 'inria':
      index = py_inria.InriaIndex()
    elif self.cbir_bow_params.implementation == 'ctis':
      index = py_ctis.CtisIndex()
      index.StartCreate(visual_vocab_size, num_docs)
    else:
      LOG(FATAL, 'unexpected')  
    
    #vv_uri = self.GetInput('visual_vocab').GetUri()
    temp_ivf_filepath = tempfile.mkdtemp()
        
    bag_of_words = bow_pb2.BagOfWords()
    progress = iwutil.MakeProgressBar(reader.Entries())
    for i, (key, value) in enumerate(reader):
      image_id = iwutil.KeyToUint64(key)
      bag_of_words.ParseFromString(value)
      index.Add(image_id, bag_of_words)
      progress.update(i)
    
    index.Save(temp_ivf_filepath)
    
    
    py_pert.Remove(self.index_base_uri)
    mr.CopyUri('local://' + temp_ivf_filepath , self.index_base_uri)    
    CHECK(py_pert.Exists(self.index_base_uri + '/index.ivf'))
    CHECK(py_pert.Exists(self.index_base_uri + '/index.ivfids'))
    
    shutil.rmtree(temp_ivf_filepath, ignore_errors=True)
    return True     

class BowCbirQueryIndexFlow(core.PipesFlow):  
  def __init__(self, resource_prefix, bow, bow_index, cbir_bow_params ):
    super(BowCbirQueryIndexFlow,self).__init__()
    self.SetPipesBinary(__file__, 'mr_cbir_bow_query_index')    
    number_of_cores_per_machine = 7.0
    ram_per_machine_gb = 6.0
    min_ram_per_mapper_gb = 6.0
    mappers_per_machine = max(1, int(ram_per_machine_gb/min_ram_per_mapper_gb))
    CHECK_LE(mappers_per_machine, number_of_cores_per_machine)
    self.desired_splits_per_core = 4/number_of_cores_per_machine
    self.force_max_map_slots_per_node = mappers_per_machine
    print 'desired_splits_per_core: %d' %  self.desired_splits_per_core
    print 'force_max_map_slots_per_node: %d' %  self.force_max_map_slots_per_node
    self.AddInput('bow', bow, is_primary=True)   
    self.AddInput('bow_index', bow_index, add_to_cache = True)
    self.AddOutput('cbir_results', core.PertResource(self, "%s/match_candidates.pert" % resource_prefix) )
    self.AddParam('cbir_bow_params', cbir_bow_params)    
    self.AddParam('mapred.tasktracker.map.tasks.maximum', 1)
    return
  


class CreateBagOfWordsFlow(core.PipesFlow):  
  def __init__(self, resource_prefix, features, visual_vocab):
    super(CreateBagOfWordsFlow, self).__init__()
    
    self.reserve_all_resources = True  # keep scheduler from running other jobs at same time
     
    # Specify resource dependencies
    self.AddInput('features', features)     
    self.AddInput('visual_vocab', visual_vocab, add_to_cache = True)    
    self.AddOutput('bow', core.PertResource(self, "%s/bow.pert" % resource_prefix) )    
        
    # Modify PipesFlow defaults
    self.SetPipesBinary(__file__, 'mr_create_bag_of_words')
    self.desired_splits_per_core = 2
    self.input_path = features.GetUri()
    self.force_max_map_slots_per_node = 4 #TODO(kheath): change this to compute slots from memory requirement instead of depending on a fixed size node and fixed size input
    return



class GetBagOfWordsVisualVocabFlow(core.Flow):  
  """ This is a hack to workaround restriction that all non-generated resources must be declared in MainFlow... Hopefully this goes away with deluge v2.0"""
  def __init__(self, features, visual_vocab_uri):
    super(GetBagOfWordsVisualVocabFlow, self).__init__()
    self.AddInput('features', features) # dummy dependancy because all flows must have an input to get scheduled... to be fixed!     
    self.AddOutput('visual_vocab', core.PertResource(self, visual_vocab_uri, is_generated=False, check_exists=True))    
    return
  
  def Run(self):
    """ this is a dummy class... doesn't need to do anything."""
    vv_uri = self.GetOutput().GetUri()
    CHECK(py_pert.Exists(vv_uri), 'expected uri to exist: %s' % (vv_uri))
    self.is_done = True # hack: needed because the timestamp condition doesn't hold here!
    return
     

class CbirMetaFlow(object):
  def __init__(self, feature_uri_base, features, cbir_params):
    self.cbir_results = None
    
    if cbir_params.HasField('full'):      
      p = cbir_params.full
      # Create Full Representation CBIR results
      cbir_uri_base = '%s/cbir/%s/' % (feature_uri_base, iwutil.HashProto(p))
      feature_count_flow = CountFeaturesFlow(feature_uri_base, features)
      feature_counts = feature_count_flow.GetOutput()      
      create_index_flow = CbirCreateIndexShardsFlow(cbir_uri_base, features, feature_counts, p.num_index_shards, p.flann)
      index_shards = create_index_flow.GetIndexShards()    
      query_index_flow = CbirQueryIndexMetaFlow(cbir_uri_base, features, index_shards, p.num_neighbors_per_index_shard, p.num_index_shards)
      score_results_flow = CbirScoreResultsFlow(cbir_uri_base, query_index_flow.output, feature_counts, p.scorer)
      self.cbir_results = score_results_flow.GetOutput()
    elif cbir_params.HasField('bow'):
      p = cbir_params.bow      
      vv_flow = GetBagOfWordsVisualVocabFlow(features, p.visual_vocabulary_uri)      
      visual_vocab = vv_flow.GetOutput()
      cbir_uri_base = '%s/cbir/%s/' % (feature_uri_base, iwutil.HashProto(p))
      create_bow_flow = CreateBagOfWordsFlow(cbir_uri_base, features, visual_vocab)      
      bow = create_bow_flow.GetOutput()
      create_bow_index_flow = CreateCbirBowIndexFlow(cbir_uri_base, bow, visual_vocab, p)     
      bow_index = create_bow_index_flow.GetOutput()      
      query_index_flow = BowCbirQueryIndexFlow(cbir_uri_base, bow, bow_index, p)
      self.cbir_results = query_index_flow.GetOutput()      
    else:
      LOG(FATAL, 'unexpected')          
    
    return
      

class CopyPertFlow(core.Flow):  
  def __init__(self, input_uri, dst_uri):
    super(CopyPertFlow,self).__init__()
    #self.input = core.PertResource(self, input_uri, is_generated=False, check_exists=True)
    #self.AddInput('input', input)  # every flow must have an input... this is a dummy input that will trigger the generation of the proto record
    self.input_uri = input_uri
    ok, scheme, path, error = py_pert.ParseUri(self.input_uri)
    input_basename = os.path.basename(path) 
    self.AddOutput('output', core.PertResource(self, "%s/%s" % (dst_uri, input_basename) ))    
    return
    
  def Run(self):    
    output_uri = self.GetOutput().GetUri()
    py_pert.CopyUri(self.input_uri, output_uri)
    return

class PertDropValueFlow(core.PipesFlow):  
  def __init__(self, input, output_uri):
    super(PertDropValueFlow,self).__init__()
    self.SetPipesBinary(__file__, 'mr_drop_value')
    self.AddInput('input', input)   
    self.AddOutput('output', core.PertResource(self, output_uri) )    
    return  