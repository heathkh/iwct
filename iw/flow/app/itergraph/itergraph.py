#!/usr/bin/python

from deluge import core
from deluge import mr
from pyglog import *
from pert import py_pert
from iw import util as iwutil
#from iw.flow.util import motif
from iw.flow.util import util
from iw.flow.util import image_matcher_config
from iw import iw_pb2
from iw.flow.app.itergraph import itergraph_pb2
import subprocess
import time


#from tide import tide_pb2

def ExecuteCmd(cmd, quiet=False):
  result = None
  if quiet:    
    with open(os.devnull, "w") as fnull:    
      result = subprocess.call(cmd, shell=True, stdout = fnull, stderr = fnull)
  else:
    result = subprocess.call(cmd, shell=True)
  return result

def CheckIsString(input):
  CHECK(isinstance(input, str), 'expected str but got: %s' % type(input))
  return


def LoadObjectFromUri(uri):  
  return iwutil.LoadObject(mr.UriToNfsPath(uri))

def SaveObjectToUri(obj, uri):  
  return iwutil.SaveObject(obj, mr.UriToNfsPath(uri))
        
class ImageGraphEdge(object):
  def __init__(self, image_a_id, image_b_id, doc_dist, match_phase):
    self.image_a_id = image_a_id
    self.image_b_id = image_b_id
    self.doc_dist = doc_dist
    self.match_phase = match_phase
    return


class IterGraphState(object):
  def __init__(self):
    self.edges = []
    self.vertex_degrees = {}    
    self.prev_attempted_edges = set()
    return
  
  def PreviouslyAttempted(self, image_a_id, image_b_id):
    return (image_a_id, image_b_id) in self.prev_attempted_edges
  
  def AddFailedEdge(self, image_a_id, image_b_id):
    self.prev_attempted_edges.add((image_a_id, image_b_id))        
    return
  
  def AddSuccesfulEdge(self, image_a_id, image_b_id, doc_dist, match_phase):
    self.prev_attempted_edges.add((image_a_id, image_b_id))
    self.edges.append(ImageGraphEdge(image_a_id, image_b_id, doc_dist, match_phase))
    self._IncrementVertexDegree(image_a_id)
    self._IncrementVertexDegree(image_b_id)     
    return
  
  def _IncrementVertexDegree(self, image_id):
    if image_id not in self.vertex_degrees:
      self.vertex_degrees[image_id] = 0    
    self.vertex_degrees[image_id] += 1
    return
  
  def GetDegree(self, image_id):
    degree = 0
    if image_id in self.vertex_degrees:
      degree = self.vertex_degrees[image_id]
    return degree
 
#  def SaveAsEval3Graph(self, uri):
#    writer = py_pert.ProtoTableWriter()
#    eval_edge = eval3_pb2.Edge()
#    writer.Open(eval_edge, uri, 1)
#    
#    for i, edge in enumerate(self.edges):
#      eval_edge.image_a_id = edge.image_a_id
#      eval_edge.image_b_id = edge.image_b_id
#      writer.Add(iwutil.Uint64ToKey(i), eval_edge.SerializeToString())
#    return
 

def GetPhaseImageGraphUri(base_uri, phase_index):
  uri = '%s/phase%03d/itergraph_state.pickle' % (base_uri, phase_index)
  return str(uri)


def GetPhaseMatchesUri(base_uri, phase_index):
  uri = '%s/phase%03d/merged_matches.pert' % (base_uri, phase_index)
  return str(uri)

def GetPhaseBaseUri(base_uri, phase_index):
  phase_base_uri = '%s/phase%03d/' % (base_uri, phase_index+1) # note convention dir name = phase num+1
  return phase_base_uri


def CheckInputPreconditions(input_images_uri):
  reader = py_pert.StringTableReader()
  CHECK(reader.Open(input_images_uri))
  CHECK(reader.IsSorted(), 'input images must be sorted')
  # TODO(heathkh): also need to check that there are no repeated keys... doing this locally is slow since we must read all the data
  return


class MainFlow(core.Flow):
  def __init__(self, root_uri, params):
    super(MainFlow,self).__init__()
    self.params = params
    input_images = core.PertResource(self, '%s/photoid_to_image.pert' % (root_uri), check_exists=True, is_generated = False)
    CheckInputPreconditions(input_images.GetUri())    
    photoids_flow = util.PertDropValueFlow(input_images, '%s/photoid.pert' % root_uri)
    self.photoids = photoids_flow.GetOutput()
    # extract features
    feature_uri_base = '%s/%s/' % (root_uri, iwutil.HashProto(self.params.tuned_feature_extractor_params))
    feature_flow = util.ExtractFeaturesFlow(feature_uri_base, input_images, self.params.tuned_feature_extractor_params.params)
    features = feature_flow.GetOutput()
    cbir_params = params.cbir
    cbir_uri_base = '%s/cbir/%s/' % (feature_uri_base, iwutil.HashProto(cbir_params))
    cbir_flow = util.CbirMetaFlow(feature_uri_base, features, cbir_params)
    cbir_results = cbir_flow.cbir_results
    itergraph_uri_base = "%s/itergraph/%s/" % (cbir_uri_base, iwutil.HashProto(self.params))
    build_graph_flow = BuildIterativeGraphFlow(itergraph_uri_base,  features,
                                               cbir_results, self.params)                                                                                                                                             
    self.merged_matches = build_graph_flow.merged_matches
    return



# class CreateMotifExpansionMatchCandidatesMetaFlow(object):
#   def __init__(self, base_uri, phase_index, itergraph_params, tide, features, prev_phase_match_results):    
#     super(CreateMotifExpansionMatchCandidatesMetaFlow, self).__init__()
#     phase_params = itergraph_params.phase[phase_index]
#     phase_base_uri = GetPhaseBaseUri(base_uri, phase_index)
#     motif_base_uri = '%s/motif/' % (phase_base_uri)
#     motif_background_features_uri = phase_params.motif_expansion.background_features_uri.encode('ascii','ignore') # protobuf returns unicodes but we need strings    
#     CHECK_GE(phase_index, 1, 'Can not do a motif expansion as first phase')    
#     # merge the matches of all previous phases
#     #prev_phase_match_uris = [GetPhaseMatchesUri(base_uri, prev_phase_index) for  prev_phase_index in range(phase_index)]
#     merge_phase_matches_flow = util.MergeMatchesFlow(motif_base_uri, prev_phase_match_results)
#     matches = merge_phase_matches_flow.GetOutput()
#     matches_to_ig_flow = util.MatchesToImageGraphFlow(motif_base_uri, matches, tide)
#     image_graph = matches_to_ig_flow.GetOutput()
#     create_motif_cluster_edges_flow = motif.CreateMotifClusters(motif_base_uri, image_graph, matches)    
#     extract_motif_matches = motif.ExtractMotifMatches(motif_base_uri, features, matches, create_motif_cluster_edges_flow.GetOutput())
#     motif_matches = extract_motif_matches.GetOutput()
#     extract_motifs = motif.ExtractMotifFeatures(motif_base_uri, features, motif_matches)
#     motifs = extract_motifs.GetOutput()
#     create_motif_index = motif.CreateMotifIndex(motif_base_uri, motifs, motif_background_features_uri, itergraph_params.cbir.flann)
#     motif_cbir_index = create_motif_index.GetOutput('motif_index')    
#     query_motif_index = motif.QueryMotifIndex(motif_base_uri, features, motif_cbir_index, itergraph_params.cbir.scorer)
#     motif_cbir_results = query_motif_index.GetOutput()    
#     create_motif_expansion_candidates = motif.CreateMotifExpansionCandidates(motif_base_uri, motifs, image_graph, motif_cbir_results)    
#     ordering_method = 'INCREASING_BY_CBIR_RANK'
#     sort_candidates_flow = util.SortMatchCandidatesFlow(motif_base_uri, ordering_method, create_motif_expansion_candidates.GetOutput())
#     self.candidates = sort_candidates_flow.GetOutput()      
#     return
#     


class InitIterativeGraphFlow(core.Flow):
  def __init__(self, base_uri, cbir_results):
    super(InitIterativeGraphFlow,self).__init__()    
    self.AddInput('cbir_results', cbir_results) # TODO(kheath): remove need for dummy input... we must have at least one or we never generate our output!!!
    self.AddOutput('initial_state', core.FileResource(self, GetPhaseImageGraphUri(base_uri, 0)))
    return
    
  def Run(self):
    SaveObjectToUri(IterGraphState(), self.GetOutput().GetUri())
    return

class BuildIterativeGraphFlow(object):
  def __init__(self, base_uri, features, cbir_results, itergraph_params):    
    CheckIsString(base_uri)
    CHECK(py_pert.IsValidUri(base_uri))
    
    canidates_by_cbir_score_flow =  util.CreateOrderedMatchCandidatesMetaFlow(base_uri, 'DECREASING_BY_CBIR_SCORE', cbir_results)
    canidates_by_cbir_rank_flow =  util.CreateOrderedMatchCandidatesMetaFlow(base_uri, 'INCREASING_BY_CBIR_RANK', cbir_results)
    
    init_iter_flow = InitIterativeGraphFlow(base_uri, cbir_results)
    prev_state = init_iter_flow.GetOutput() 
    phase_match_resources = []
    for phase_index, phase_params in enumerate(itergraph_params.phase):   
      candidates = None
      if phase_params.type == itergraph_pb2.PhaseParams.PHASE_TYPE_CBIR_SCORE:
        candidates = canidates_by_cbir_score_flow.candidates
      elif phase_params.type == itergraph_pb2.PhaseParams.PHASE_TYPE_CBIR_RANK:
        candidates = canidates_by_cbir_rank_flow.candidates
#       elif phase_params.type == itergraph_pb2.PhaseParams.PHASE_TYPE_MOTIF_EXPANSION:
#         motif_expansion_flow = CreateMotifExpansionMatchCandidatesMetaFlow(base_uri, phase_index, itergraph_params, tide, features, phase_match_resources)
#         candidates = motif_expansion_flow.candidates   
      else:
        LOG(FATAL, 'unexpected')  
      
      cur_phase = PhaseFlow(base_uri, itergraph_params, phase_index, candidates, prev_state, features)
      phase_match_resources.append(cur_phase.matches)
      prev_state = cur_phase.itergraph_state
    
    merge_phase_matches_flow = util.MergeMatchesFlow(base_uri,  phase_match_resources)   
    merged_matches =  merge_phase_matches_flow.GetOutput()
    util.RecordParamsFlow(base_uri, itergraph_params, merged_matches)
    self.itergraph_state = prev_state
    self.merged_matches = merged_matches
    return
  
  
class PhaseFlow(object):
  def __init__(self, base_uri, itergraph_params, phase_index, candidates, prev_state, features):
    phase_base_uri = GetPhaseBaseUri(base_uri, phase_index)
    self.phase_index = phase_index
    # planning
    #phase_params = itergraph_params.phase[phase_index]  
    planning_flow = ItergraphMatchBatchPlanningFlow(phase_base_uri, itergraph_params, phase_index, candidates, prev_state, features)
    match_plan = planning_flow.GetOutput('sorted_match_batches')
    
    # matching
    matching_flow = util.MatchBatchesFlow(phase_base_uri, features, match_plan, itergraph_params.image_matcher_config, phase_index)
    matches = [matching_flow.GetOutput()]
    # merging (needed for graph update)
    merge_matches_flow = util.MergeMatchesFlow(phase_base_uri, matches)
    # graph update
    update_flow = IterGraphStateUpdateFlow(phase_index, phase_base_uri, prev_state, merge_matches_flow.GetOutput())    
    self.matches = merge_matches_flow.GetOutput()
    self.itergraph_state = update_flow.GetOutput('itergraph_state')
    return
    
  
class ItergraphMatchBatchPlanningFlow(core.Flow):
  def __init__(self, phase_base_uri, itergraph_params, phase_index, candidates, prev_state, features):
    super(ItergraphMatchBatchPlanningFlow,self).__init__()
    self.features = features
    self.AddInput('candidates', candidates)
    self.AddInput('prev_state', prev_state)
    self.AddOutput('sorted_match_batches', core.PertResource(self, '%s/sorted_match_batches.pert' % (phase_base_uri)))
    phase = itergraph_params.phase[phase_index]
    self.max_candidates_per_phase = phase.max_match_candidates
    self.max_vertex_degree = phase.max_vertex_degree
    self.max_batch_size = phase.max_match_batch_size
    self.max_replication_factor = phase.max_image_replication_factor
    self.match_groups = {}
    self.num_replications = {}
    return
  
  def _GetNumReplications(self, image_id):
    replications = 0
    if image_id in self.num_replications:
      replications = self.num_replications[image_id]
    return replications
  
  def _IncrementReplications(self, image_id):
    if image_id not in self.num_replications:
      self.num_replications[image_id] = 0
    self.num_replications[image_id] += 1
    return
  
  def Run(self):
    LOG(INFO, 'waiting to let running processes give up memory... I need a lot and may not get enough if we rush things...')
    time.sleep(30)
    itergraph_state = LoadObjectFromUri(self.GetInput('prev_state').GetUri())
    reader = py_pert.StringTableReader()
    CHECK(reader.Open(self.GetInput('candidates').GetUri()))
    self.match_groups = {}
    num_selected_candidates = 0
    
    pbar = iwutil.MakeProgressBar(self.max_candidates_per_phase)
    num_edges_skipped_max_degree_constraint = 0
    num_edges_skipped_max_replication_constraint = 0   
    prev_score = -float('inf')
    
    for ordering_key, candidate_pair_data in reader:      
      image_a_id, image_b_id = iwutil.ParseUint64KeyPair(candidate_pair_data)
      if itergraph_state.PreviouslyAttempted(image_a_id, image_b_id):
        #print 'skipping previous attempted edge'
        continue
      # check precondition... candidates pert is sorted (increasing by rank or by negative cbir score)
      score = iwutil.KeyToDouble(ordering_key)
      CHECK_GE(score, prev_score) 
      prev_score = score
      
      if image_a_id not in self.match_groups:
        self.match_groups[image_a_id] = []
                
      match_group_size = len(self.match_groups[image_a_id])
      
      if match_group_size < self.max_batch_size:
        # test vertex degree condition
        degree_a = itergraph_state.GetDegree(image_a_id)
        degree_b = itergraph_state.GetDegree(image_b_id)
        
        # version 1: skip candidate edge if either of the vertices has many edges           
        #if degree_a < self.max_vertex_degree and degree_b < self.max_vertex_degree:
          
        # version 2: skip candidate edge only if both of the vertices have many edges           
        if degree_a < self.max_vertex_degree or degree_b < self.max_vertex_degree:  
          # test max replication condition
          num_replications = self._GetNumReplications(image_b_id)
          if num_replications < self.max_replication_factor:                
            self._IncrementReplications(image_b_id)
            self.match_groups[image_a_id].append(image_b_id)
            num_selected_candidates += 1
            pbar.update(num_selected_candidates)
          else:
            num_edges_skipped_max_replication_constraint += 1
        else:
          num_edges_skipped_max_degree_constraint += 1 
    
      
      if num_selected_candidates >= self.max_candidates_per_phase:
        break
    
    pbar.finish()
    
    print ''
    print ''
    print 'num_edges_skipped_max_replication_constraint: %d' % (num_edges_skipped_max_replication_constraint)  
    print 'num_edges_skipped_max_degree_constraint: %d' % (num_edges_skipped_max_degree_constraint)
    print ''
    print ''
      
    # write out the match plan (must be sorted by key for future join stage)
    metadata_entries = []
        
    for batch_id, (batch_primary_image, batch_image_ids) in enumerate(self.match_groups.iteritems()):
      if len(batch_image_ids) == 0:
        continue
      batch_name = iwutil.Uint64ToKey(batch_id)
      CHECK(batch_name)
      CHECK(len(batch_name))
      match_batch_metadata = iw_pb2.MatchBatchMetadata()
      match_batch_metadata.image_id = batch_primary_image
      match_batch_metadata.batch_name = batch_name
      match_batch_metadata.is_primary = True
      metadata_entries.append( match_batch_metadata )
      
      for image_id in batch_image_ids:
        next_metadata = iw_pb2.MatchBatchMetadata() 
        next_metadata.image_id = image_id
        next_metadata.batch_name = batch_name
        next_metadata.is_primary = False
        metadata_entries.append( next_metadata )
    
    # image_id will be the key of output, so need to sort by image_id    
    metadata_entries.sort(key= lambda m : m.image_id)
    match_batches_uri = self.GetOutput('sorted_match_batches').GetUri()    
    
    # TODO(heathkh): "closing" doesn't flush to disk... this is a bug!    
#    match_plan_writer = py_pert.ProtoTableWriter()
#    num_shards_features = py_pert.GetNumShards(self.features.GetUri())      
#    CHECK(match_plan_writer.Open(iw_pb2.MatchBatchMetadata(), match_batches_uri, num_shards_features))      
#    for metadata in metadata_entries:
#      CHECK(metadata.IsInitialized())
#      key = iwutil.Uint64ToKey(metadata.image_id)      
#      CHECK(match_plan_writer.Add(key, metadata.SerializeToString()))    
#    match_plan_writer.Close()
    
    # TODO(kheath):   Work around for above bug is to run a MR stage to reshard 
    tmp_match_batches_uri = self.GetOutput('sorted_match_batches').GetUri() + '_to_be_sharded'
    match_plan_writer = py_pert.ProtoTableWriter()
    num_shards_features = py_pert.GetNumShards(self.features.GetUri())    
    CHECK(match_plan_writer.Open(iw_pb2.MatchBatchMetadata(), tmp_match_batches_uri, 1))
    
    for metadata in metadata_entries:
      CHECK(metadata.IsInitialized())      
      CHECK(match_plan_writer.Add(iwutil.Uint64ToKey(metadata.image_id), metadata.SerializeToString()))    
    match_plan_writer.Close()

    # manually reshard
    pertedit_bin = 'pertedit'
    cmd = '%s --input %s --output %s --new_block_size_mb=10 --num_output_shards=%d' % (pertedit_bin, tmp_match_batches_uri, match_batches_uri, num_shards_features) 
    print cmd  
    CHECK_EQ(ExecuteCmd(cmd), 0)
    
    CHECK(py_pert.Exists(match_batches_uri))
    
    ok, fp = py_pert.GetShardSetFingerprint(match_batches_uri)
    CHECK(ok)
    CHECK(len(fp), 32)
    CHECK_NE(fp, 'd41d8cd98f00b204e9800998ecf8427e', 'invalid hash of empty string')
    
    return
        

class IterGraphStateUpdateFlow(core.Flow):
  def __init__(self, phase, phase_base_uri, prev_state, match_results):
    super(IterGraphStateUpdateFlow,self).__init__()
    self.phase = phase
    self.AddInput('prev_state', prev_state)
    self.AddInput('match_results', match_results)
    self.AddOutput('itergraph_state', core.FileResource(self, '%s/itergraph_state.pickle' % (phase_base_uri)))
    
    return
   
  def Run(self):
    itergraph_state = LoadObjectFromUri(self.GetInput('prev_state').GetUri())
    if True: # hack to put this block in it's own scope to force release of memory resources      
      reader = py_pert.StringTableReader()
      CHECK(reader.Open(self.GetInput('match_results').GetUri()))
      match_result = iw_pb2.GeometricMatchResult()
      num_entries = reader.Entries()
      if num_entries:
        pbar = iwutil.MakeProgressBar(num_entries)
        for i, (k,v) in enumerate(reader):
          pbar.update(i)
          match_result.ParseFromString(v)
          success = False
          for match in match_result.matches:
            if match.nfa < -20:
              success = True
          if success:
            itergraph_state.AddSuccesfulEdge(match_result.image_a_id, match_result.image_b_id, match_result.properties.score,  self.phase)        
          else:
            itergraph_state.AddFailedEdge(match_result.image_a_id, match_result.image_b_id)
      print 'edges: %d' % (len(itergraph_state.edges))
    SaveObjectToUri(itergraph_state, self.GetOutput('itergraph_state').GetUri())
    return
    

#def EvalImageGraph(itergraph_state_uri, tide_uri):
#  # compute edge stats
#  num_within_cluster, num_cross_cluster = CountCorrectIncorrectEdges(itergraph_state_uri, tide_uri)
#  
#  # compute label prop performance
#  itergraph_state = LoadObjectFromUri(itergraph_state_uri)
#  eval_graph_uri = 'local://tmp/eval3_graph.pert'
#  itergraph_state.SaveAsEval3Graph(eval_graph_uri)
#  
#  num_training_images = 100
#  num_trials = 4
#  evaluation = py_eval3.EvaluationRunner(eval_graph_uri, tide_uri, num_training_images, num_trials)
#  ok, result = evaluation.Run()
#  CHECK(ok)
#  
#  return num_within_cluster, num_cross_cluster, result
#  
#
#class EvalImageGraphFlow(core.Flow):
#  def __init__(self, base_uri, itergraph_state, tide):
#    super(EvalImageGraphFlow,self).__init__()
#    self.base_uri = base_uri
#    self.AddInput('itergraph_state', itergraph_state)
#    self.AddInput('tide', tide)
#    
#    self.AddOutput('eval', core.FileResource(self, '%s/eval.txt' % (base_uri)))
#    
#    return
#    
#
#  def Run(self):
#    # compute edge stats
#    itergraph_state_uri = self.GetInput('itergraph_state').GetUri()
#    tide_uri = self.GetInput('tide').GetUri()
#    
#    num_within_cluster, num_cross_cluster, result = EvalImageGraph(itergraph_state_uri, tide_uri)
#    
#    lines = []
#    lines.append('num_within_cluster: %d' % (num_within_cluster))
#    lines.append('num_cross_cluster: %d' % (num_cross_cluster))
#    lines.append('fraction within cluster: %f' % (float(num_within_cluster)/ (num_within_cluster + num_cross_cluster)))
#    lines.append(str(result))
#    report = '\n'.join(lines)
#    print report
#    
#    path = mr.UriToNfsPath(self.GetOutput('eval').GetUri())
#    f = open(path, 'w')
#    f.write(report)    
#    
#    return
#  



#class BasicMatchBatchPlanningFlow(core.Flow):
#  def __init__(self, base_uri, candidates, max_batch_size, max_replication_factor, num_shards_features):
#    super(BasicMatchBatchPlanningFlow,self).__init__()
#    
#    self.AddInput('candidates', candidates)
#    self.AddOutput('sorted_match_batches', core.PertResource(self, '%s/sorted_match_batches.pert' % (base_uri)))
#    
#    self.max_batch_size = max_batch_size
#    self.max_replication_factor = max_replication_factor
#    
#    self.num_shards_features = num_shards_features
#    self.match_groups = {}
#    self.num_replications = {}
#    return
#  
#  def _GetNumReplications(self, image_id):
#    replications = 0
#    if image_id in self.num_replications:
#      replications = self.num_replications[image_id]
#    return replications
#  
#  def _IncrementReplications(self, image_id):
#    if image_id not in self.num_replications:
#      self.num_replications[image_id] = 0
#    self.num_replications[image_id] += 1
#    return
#  
#  def Run(self):
#    reader = py_pert.StringTableReader()
#    reader.Open(self.GetInput('candidates').GetUri())
#    
#    self.match_groups = {}
#    num_selected_candidates = 0
#    prev_score = -1e6
#    
#    widgets = [Percentage(), ' ', Bar(), ' ', ETA()]
#    pbar = ProgressBar(widgets=widgets, maxval=reader.Entries()).start()
#   
#    num_edges_skipped_max_replication_constraint = 0   
#    for i, (k, v) in enumerate(reader):
#      image_a_id, image_b_id = ParseUint64KeyPair(v)
#      
#      # check precondition... pert is sorted by scores
#      score = iwutil.KeyToDouble(k)
#      CHECK_GE(score, prev_score) 
#      prev_score = score
#      
#      if image_a_id not in self.match_groups:
#        self.match_groups[image_a_id] = []
#                
#      match_group_size = len(self.match_groups[image_a_id])
#      
#      if match_group_size < self.max_batch_size:
#        # test max replication condition
#        num_replications = self._GetNumReplications(image_b_id)
#        if num_replications < self.max_replication_factor:                
#          self._IncrementReplications(image_b_id)
#          self.match_groups[image_a_id].append(image_b_id)
#          num_selected_candidates += 1
#          pbar.update(num_selected_candidates)
#        else:
#          num_edges_skipped_max_replication_constraint += 1
#      
#    
#    print 'num_edges_skipped_max_replication_constraint: %d' % (num_edges_skipped_max_replication_constraint)  
#      
#    # write out the match plan (must be sorted by key for future join stage)
#    metadata_entries = []
#        
#    for batch_id, (batch_primary_image, batch_image_ids) in enumerate(self.match_groups.iteritems()):
#      
#      if not batch_image_ids:
#        continue
#      
#      batch_name = iwutil.Uint64ToKey(batch_id)
#      match_batch_metadata = iw_pb2.MatchBatchMetadata()
#      match_batch_metadata.image_id = batch_primary_image
#      match_batch_metadata.batch_name = batch_name
#      match_batch_metadata.is_primary = True
#      metadata_entries.append( match_batch_metadata )
#      
#      for image_id in batch_image_ids:
#        next_metadata = iw_pb2.MatchBatchMetadata()
#        next_metadata.image_id = image_id
#        next_metadata.batch_name = batch_name
#        next_metadata.is_primary = False
#        metadata_entries.append( next_metadata )
#    
#    # image_id will be the key of output (since we are about to join by image_id), so need to sort by image_id    
#    metadata_entries.sort(key= lambda m : iwutil.Uint64ToKey(m.image_id))
#    match_plan_writer = py_pert.ProtoTableWriter()
#    uri = self.GetOutput('sorted_match_batches').GetUri()
#    CHECK(match_plan_writer.Open(iw_pb2.MatchBatchMetadata(), uri, self.num_shards_features), 'failed to open %s' % (uri))  # to do join with features, must be sharded same way as features
#    for metadata in metadata_entries:
#      match_plan_writer.Add(iwutil.Uint64ToKey(metadata.image_id), metadata.SerializeToString())
#    match_plan_writer.Close()
#    
#    return

#def EnsureParentPathExists(f):
#  d = os.path.dirname(f)
#  if not os.path.exists(d):
#    os.makedirs(d)
#  return

#def ParseUint64KeyPair(key):
#  CHECK_EQ(len(key), 16)
#  id_a = iwutil.KeyToUint64(key[0:8])
#  id_b = iwutil.KeyToUint64(key[8:16])
#  return id_a, id_b





#class TideObject(object):
#  def __init__(self):
#    self.id = None
#    self.name = None
#    self.image_ids = []
#    return
#    
#  def LoadFromProto(self, id, tide_object_proto):
#    self.id = id
#    self.name = tide_object_proto.name
#    for photo in tide_object_proto.photos:
#      self.image_ids.append(photo.id)
#    return
#
#
#class TideDataset():
#  def __init__(self, tide_uri):
#    self.tideid_to_tideobject = {}
#    self.imageid_to_objectid = {}
#    
#    # load list of images that belong to each tide object    
#    tide_reader = py_pert.StringTableReader()
#    tide_reader.Open(tide_uri)
#    for index, (k, v) in enumerate(tide_reader):                                  
#      tide_object = tide_pb2.Object()
#      tide_object.ParseFromString(v)
#      obj = TideObject()
#      obj.LoadFromProto(index, tide_object) 
#      self.tideid_to_tideobject[obj.id] = obj
#
#    for tideid, tideobject in self.tideid_to_tideobject.iteritems():
#      object_id = tideobject.id
#      for image_id in tideobject.image_ids:
#        self.imageid_to_objectid[image_id] = object_id
#    return
#  
#  def KnownImages(self, image_a_id, image_b_id):
#    return image_a_id in self.imageid_to_objectid and image_b_id in self.imageid_to_objectid
#  
#  def EdgeWithinCluster(self, image_a_id, image_b_id):
#    CHECK(self.KnownImages(image_a_id, image_b_id))
#    object_a = self.imageid_to_objectid[image_a_id]
#    object_b = self.imageid_to_objectid[image_b_id]    
#    return object_a == object_b
#    

#
#def CountCorrectIncorrectEdges(itergraph_state_uri, tide_uri):
#  itergraph_state = LoadObjectFromUri(itergraph_state_uri)
#  tide = TideDataset(tide_uri)
#  
#  num_within_cluster = 0
#  num_cross_cluster = 0
#  
#  object_to_num_within_cluster = {}
#  for tide_id in tide.tideid_to_tideobject.iterkeys():
#    object_to_num_within_cluster[tide_id] = 0
#  
#  
#  for edge in itergraph_state.edges:
#    if tide.EdgeWithinCluster(edge.image_a_id, edge.image_b_id):
#      num_within_cluster += 1
#      object_to_num_within_cluster[tide.imageid_to_objectid[edge.image_a_id]] += 1
#    else:
#      num_cross_cluster += 1
#  
#  for tide_id, num in object_to_num_within_cluster.iteritems():
#    print '%s: %d' % (tide.tideid_to_tideobject[tide_id].name, num)
#    
#
#  return num_within_cluster, num_cross_cluster

#def GetPhaseBaseUri(base_uri,  phase):
#    return base_uri + '/phase%03d/' % (phase)    