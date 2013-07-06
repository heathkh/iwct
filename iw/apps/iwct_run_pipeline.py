#!/usr/bin/python

from iw import iw_pb2
from iw import util as iwutil
from iw.flow.app.itergraph import itergraph
from iw.flow.app.itergraph import itergraph_pb2
from iw.flow.util import image_matcher_config
from iw.flow.util import util as flowutil
from iw.matching.cbir.full import full_pb2
from snap.deluge import scheduler 
from snap.google.base import  py_base
from snap.pert import py_pert
from snap.pyglog import *
import os
from iw.vis.static.imagegraph import exporter

class RunPipelineApp(object):
  def __init__(self):
    self.local_data_directory = os.path.expanduser('~/Desktop/datasets')   
    return

  def Run(self):
    dataset_name = self.__SelectDataset()
    local_data_path = '%s/%s/' % (self.local_data_directory, dataset_name)
    local_data_uri = 'local://%s' %  (local_data_path)
    input_images_uri = '%s/photoid_to_image.pert' % (local_data_uri)
    root_uri = 'maprfs://data/itergraph/'
    num_images =  self.__GetNumImages(input_images_uri)
    dataset_uri = '%s/%s' % (root_uri, dataset_name)
    self.__UploadData(input_images_uri, dataset_uri)
    params = self.__SetupParams(num_images, dataset_name, root_uri)
    merged_matches, photoids = self.__MatchImages(params, dataset_uri)
    results_uri = self.__DownloadResults(merged_matches, dataset_uri,
                                         local_data_uri)
    self.__CreateImageGraph(merged_matches, photoids, results_uri)
    self.__ExportAsHtml(local_data_path)    

    return
  
  def __ListDatasets(self):
    return  [ name for name in os.listdir(self.local_data_directory) if
              os.path.isdir(os.path.join(self.local_data_directory, name)) ]
    
  def __SelectDataset(self):
    dataset = None
    valid_datasets = self.__ListDatasets()
    while not dataset:
      print ''
      print 'available datasets:'
      print ''
      for dataset in valid_datasets:
        print dataset
      dataset = raw_input('Select a dataset to process: ')     
      if dataset not in valid_datasets:
        print 'invalid dataset: %s' % (dataset)
        dataset = None
    return dataset
  
  def __GetNumImages(self, input_images_uri):
    reader = py_pert.StringTableReader()
    CHECK(reader.Open(input_images_uri))
    return reader.Entries()
  
  def __UploadData(self, input_images_uri, root_uri):
    flow = flowutil.CopyPertFlow(input_images_uri, root_uri)
    CHECK(scheduler.FlowScheduler(flow).RunSequential())
    return
  
  def __DownloadResults(self, merged_matches, dataset_uri, local_data_uri):
    merged_matches_uri = merged_matches.GetUri()
    results_uri = '%s/results/' % (local_data_uri)    
    flow = flowutil.CopyPertFlow(merged_matches_uri, results_uri)
    CHECK(scheduler.FlowScheduler(flow).RunSequential())
    
    photoids_uri = '%s/photoid.pert' % dataset_uri
    flow = flowutil.CopyPertFlow(photoids_uri, results_uri)
    CHECK(scheduler.FlowScheduler(flow).RunSequential())
    return results_uri
  
  def __CreateImageGraph(self, merged_matches, photoids, results_uri):
    flow = flowutil.MatchesToImageGraphFlow(results_uri, merged_matches, 
                                            photoids)
    CHECK(scheduler.FlowScheduler(flow).RunSequential())
    return 
  
  def __ExportAsHtml(self, local_data_path):
    images_uri = 'local://%s/photoid_to_image.pert' % (local_data_path)
    matches_uri = 'local://%s/results/merged_matches.pert' % (local_data_path)
    image_graph_uri = 'local://%s/results/image_graph.pert' % (local_data_path)
    tide_uri = 'local://%s/objectid_to_object.pert' % (local_data_path)
    if not py_pert.Exists(tide_uri):
      tide_uri = None
      
    html_export_path = '%s/results/html/' % local_data_path 
    if not os.path.exists(html_export_path):
      os.mkdir(html_export_path)    
    exporter.ImageGraphExporter(html_export_path, images_uri, matches_uri, 
                                image_graph_uri, tide_uri).Run()
    return
  
  def __SetupParams(self, num_images, dataset_name, base_uri):
    
    feature_type = 'usift'
    desired_features_per_megapixel = 3000
    root_sift_normalization = True  
    tune_flow = flowutil.TuneFeatureExtractorDensityFlow(base_uri, dataset_name,
      feature_type, desired_features_per_megapixel)
    CHECK(scheduler.FlowScheduler(tune_flow).RunSequential())
    tuned_feature_extractor_params = iw_pb2.DensityTunedFeatureExtractorParams()
    iwutil.LoadProtoFromUriOrDie(tuned_feature_extractor_params, 
                                 tune_flow.GetOutput().GetUri())      
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
    
    # Create template for a "cbir rank" phase
    # TODO: compute phase size and number from dataset size
    local_best_phase = itergraph_pb2.PhaseParams()  
    local_best_phase.max_match_candidates = num_images*2
    local_best_phase.max_vertex_degree = 100
    local_best_phase.max_match_batch_size = 100
    local_best_phase.max_image_replication_factor = 100
    local_best_phase.type = itergraph_pb2.PhaseParams.PHASE_TYPE_CBIR_RANK
    
    for _ in range(5):
      new_phase = params.phase.add()
      new_phase.CopyFrom(local_best_phase)
    
    CHECK(params.IsInitialized(), str(params))
    return params

  def __MatchImages(self, params, base_uri):
    flow = itergraph.MainFlow(base_uri, params)
    CHECK(scheduler.FlowScheduler(flow).RunSequential())
    return flow.merged_matches, flow.photoids

if __name__ == "__main__":      
  app = RunPipelineApp()
  app.Run()
 
