#!/usr/bin/python

import time
import tornado.ioloop
import tornado.web
import json
from snap.pyglog import *
from iw import util as iwutil
from iw.matching.cbir import cbir_pb2
from iw.flow.app.itergraph import itergraph_pb2
from iw.flow.app.lpbench import lpbench_pb2
from iw.flow.app.lpbench import resultdb
from iw.eval.labelprop.eval1 import py_eval1
from iw.eval.labelprop.eval2 import py_eval2
from iw import visutil
from snap.pyglog import *

def FriendlyFeatureType(p):
  params = p.tuned_feature_extractor_params.params
  feature_type = None
  if params.HasField('ocv_sift_params'):
    if params.ocv_sift_params.upright:
      feature_type = 'usift'
    else:
      feature_type = 'sift'
  elif params.HasField('vgg_affine_sift_params'):
    feature_type = 'ahess'
  else:
    LOG(FATAL, 'error')
  return feature_type
  
  
def GetMatchingPhaseSqueezeTimes(r):
  """ for n phases, returns the n+1 vector of squeeze times of start and end of each phase"""
  matching_phase_squeeze_times = []
  provenance_list =  list(r.matches_provenance_list)
  provenance_list.sort(key = lambda p : p.start_time_sec)
      
  total_job_run_time_sec = 0
  prev_end_time_sec = 0
  
  for p in provenance_list:
    CHECK_GE(p.start_time_sec,  prev_end_time_sec) # our method of adding run times assumes no two jobs overlap... need more complex method otherwise
    prev_end_time_sec = p.end_time_sec        
    CHECK_GE(p.end_time_sec, p.start_time_sec) # check invariant
    job_run_time_sec = p.end_time_sec - p.start_time_sec
    print total_job_run_time_sec, p.flow, p.name
    total_job_run_time_sec += job_run_time_sec
  
    if p.flow == 'SortMatchCandidatesFlow':
      CHECK_EQ(len(matching_phase_squeeze_times), 0)
      matching_phase_squeeze_times.append(total_job_run_time_sec)
    elif p.flow == 'MatchBatchesFlow':  
      matching_phase_squeeze_times.append(total_job_run_time_sec)
  
  return matching_phase_squeeze_times
  

def CreateRowData():
  LOG(INFO, 'fetching row data from s3...')
  rows = []
  db = resultdb.ResultDatabase()
  rows = []      
  for i, r in enumerate(db.GetResults()):
    row = {}
    row['index'] = i
    
    time_series_data = []
    matching_phase_squeeze_times = GetMatchingPhaseSqueezeTimes(r)
    print matching_phase_squeeze_times
    #time_series_data.append({'precision': 0, 'recall': 0, 'squeeze_time_end_sec' : matching_phase_squeeze_times[0]})
    
    
    for phase_index, phase in enumerate(r.eval1.phases):
      print phase_index
      point = {}
      point['precision'] = phase.precision.mean
      point['recall'] = phase.recall.mean          
      point['squeeze_time_end_sec'] = matching_phase_squeeze_times[phase_index+1]
      time_series_data.append(point)
    
    row['time_series'] = time_series_data
    row['feature'] = FriendlyFeatureType(r.config.iter_graph)
    row['fpm'] = r.config.iter_graph.tuned_feature_extractor_params.desired_density
    
    cbir_type = None
    if r.config.iter_graph.cbir.HasField('bow'):
      cbir_type = 'bow'
    elif r.config.iter_graph.cbir.HasField('full'):
      cbir_type = 'full'
    else:
      print r.config.iter_graph.cbir
      LOG(FATAL, 'unexpected')    
    
    print cbir_type
    row['cbir_type'] = cbir_type
      
    
    #row['cbir.num_neighbors_per_index_shard'] = r.config.iter_graph.cbir.num_neighbors_per_index_shard
    #row['cbir.scorer.correspondence_filter'] = cbir_pb2._QUERYSCORERPARAMS_CORRESPONDENCEFILTER.values_by_number[r.config.iter_graph.cbir.scorer.correspondence_filter].name 
    #row['cbir.flann.kdtree.trees'] = r.config.iter_graph.cbir.flann.kdtree_params.trees
    row['proto'] = str(r.config.iter_graph)
    
    
#    for phase in r.config.iter_graph.phase:
#      if phase.type == itergraph_pb2.PhaseParams.PHASE_TYPE_CBIR_RANK:
#        num_matches_cbir_rank += phase.max_match_candidates
#      elif phase.type == itergraph_pb2.PhaseParams.PHASE_TYPE_CBIR_SCORE:
#        num_matches_cbir_score += phase.max_match_candidates  
#      elif phase.type == itergraph_pb2.PhaseParams.PHASE_TYPE_MOTIF_EXPANSION:
#        num_matches_motif_expansion += phase.max_match_candidates
        
    print row
    rows.append(row)  
  return rows

def GetCachedRowData():
  cache_file = os.path.abspath(os.path.dirname(__file__)) + '/cached_rows.pickle'
  rows = None
  if not os.path.exists(cache_file):
    rows = CreateRowData()
    iwutil.SaveObject(rows, cache_file)
  else:
    rows = iwutil.LoadObject(cache_file)
  CHECK(rows)  
  return rows     

class MainHandler(tornado.web.RequestHandler):    
  def initialize(self, app):
    self.app = app      
    return            
  
  def get(self):
    rows = GetCachedRowData()
    params = {}
    params['rows'] = json.dumps(rows)   
    self.render('main.html', **params)
    return


class ViewerApp:
  def __init__(self):
    
    self.application = tornado.web.Application([
      (r"/", MainHandler, dict(app=self)),                        
      ], debug=True)    
    
    static_path =  os.path.dirname(os.path.realpath(__file__)) + '/static/'
    self.application = tornado.web.Application([        
        (r'/static/(.*)', tornado.web.StaticFileHandler, {'path': static_path}),
        (r"/", MainHandler, dict(app=self)),        
      ], debug=True)  
    return
  
  def Run(self):
    port = 8888
    while True:
      try:
        self.application.listen(port)
        break
      except:
        print 'trying port: %d' % (port)
        time.sleep(1)
        port +=1
    print 'server running at: http://localhost:%d' % (port)      
    tornado.ioloop.IOLoop.instance().start()
    return

if __name__ == "__main__":      
  app = ViewerApp()
  app.Run()
 