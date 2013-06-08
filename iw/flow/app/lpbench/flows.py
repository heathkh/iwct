#!/usr/bin/python

from snap.deluge import core
from snap.deluge import mr
from snap.pyglog import *
#from snap.pert import py_pert
from iw import util as iwutil
#from iw.flow.util import motif
from iw.flow.util import util
from iw.flow.util import image_matcher_config
from iw import iw_pb2
from iw.flow.app.itergraph import itergraph
from iw.flow.app.itergraph import itergraph_pb2
import time
from tide import tide_pb2

def EnsureString(value):  
  if isinstance(value, str):
    return value
  elif isinstance(value, unicode):
    return value.encode('ascii','ignore')
  else:
    CHECK(FATAL, 'Expected a string, but got: %s' % (value.__class__.__name__))
  return


class MainFlow(core.Flow):
  def __init__(self, lpbench_root_uri, config):
    super(MainFlow,self).__init__()
    root_uri = '%s/%s' % (lpbench_root_uri, EnsureString(config.dataset_name))    
    input_images = core.PertResource(self, '%s/photoid_to_image.pert' % (root_uri), check_exists=True, is_generated = False)    
    tide = core.PertResource(self, '%s/objectid_to_object.pert' % (root_uri), check_exists=True, is_generated = False)    
    params = config.iter_graph
    
    # extract features
    feature_uri_base = '%s/%s/' % (root_uri, iwutil.HashProto(params.tuned_feature_extractor_params))
    feature_flow = util.ExtractFeaturesFlow(feature_uri_base, input_images, params.tuned_feature_extractor_params.params)
    features = feature_flow.GetOutput()
    
    cbir_params = params.cbir
    cbir_uri_base = '%s/cbir/%s/' % (feature_uri_base, iwutil.HashProto(cbir_params))
    
    cbir_flow = util.CbirMetaFlow(feature_uri_base, features, cbir_params)
    cbir_results = cbir_flow.cbir_results
    
    itergraph_uri_base = "%s/itergraph/%s/" % (cbir_uri_base, iwutil.HashProto(params))
    build_graph_flow = itergraph.BuildIterativeGraphFlow(itergraph_uri_base, 
                                               features,
                                               cbir_results,
                                               params,
                                               tide)
    matches = build_graph_flow.merged_matches
    
    # eval match graph    
    matches_to_ig_flow = util.MatchesToImageGraphFlow(itergraph_uri_base, matches, tide)
    image_graph = matches_to_ig_flow.GetOutput()
    eval1_flow = util.LabelpropEval1Flow(itergraph_uri_base, image_graph, tide)
    
    matches_to_irg_flow = util.MatchesToImageRegionGraphFlow(itergraph_uri_base, matches, tide)
    image_region_graph = matches_to_irg_flow.GetOutput()
    eval2_flow = util.LabelpropEval2Flow(itergraph_uri_base, image_region_graph, tide)
    
    self.matches = matches
    self.eval1 = eval1_flow.GetOutput()
    self.eval2 = eval2_flow.GetOutput()
    return

  def Run(self):
    LOG(INFO, 'I do nothing')
    return