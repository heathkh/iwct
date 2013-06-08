#!/usr/bin/python
from deluge import core
from pyglog import *
from pert import py_pert
from iw.flow.util import util

class MainFlow(core.Flow):
  def __init__(self, root_uri, matches_root_uri):
    super(MainFlow,self).__init__()
    tide = core.PertResource(self, '%s/objectid_to_object.pert' % (root_uri), check_exists = True, is_generated = False)    
    matches = core.PertResource(self, '%s/merged_matches.pert' % (matches_root_uri), check_exists = True, is_generated = False)    
    
    matches_to_ig_flow = util.MatchesToImageGraphFlow(matches_root_uri, matches, tide)
    image_graph = matches_to_ig_flow.GetOutput()
    eval1_flow = util.LabelpropEval1Flow(matches_root_uri, image_graph, tide)
    
    matches_to_irg_flow = util.MatchesToImageRegionGraphFlow(matches_root_uri, matches, tide)
    image_region_graph = matches_to_irg_flow.GetOutput()
    eval2_flow = util.LabelpropEval2Flow(matches_root_uri, image_region_graph, tide)
    return
    