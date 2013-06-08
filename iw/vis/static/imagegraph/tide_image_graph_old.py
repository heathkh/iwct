#!/usr/bin/python

import os
#from pert import py_pert
from snap.google.base import py_base
from iw import iw_pb2
from iw import py_imagegraph
from tide import tide
from tide import tide_pb2
from pert import py_pert
from pyglog import *
import string
import json
import cPickle as pickle
import bisect
from iw import py_imagegraph
import networkx as nx


def GetCachedTideImageGraph(image_graph_uri, tide_uri):
  hash_str = ''
  for uri in [image_graph_uri, tide_uri]:
    ok, scheme, path, error = py_pert.ParseUri(tide_uri)    
    hash_str += (uri + str(os.path.getmtime(path)) + str(os.path.getsize(path)))    
  hash_val = hash(hash_str)
  cache_path = './tide_image_graph.%s.pickle' % (hash_val)
  
  tide_image_graph = None
  if os.path.exists(cache_path):
    f = open(cache_path, 'r')
    tide_image_graph = pickle.load(f)
  else:
    tide_image_graph = TideImageGraph(image_graph_uri, tide_uri)
    f = open(cache_path, 'w')
    pickle.dump(tide_image_graph, f, protocol=2)            
  return tide_image_graph



def bfs_edges(G,source, max_depth=None):
    """Produce edges in a breadth-first-search starting at source."""
    # Based on http://www.ics.uci.edu/~eppstein/PADS/BFS.py
    # by D. Eppstein, July 2004.
    visited=set([source])
    depth_of_node = {}
    depth_of_node[source] = 0 
    stack = [(source,iter(G[source]))]
    while stack:
        parent,children = stack[0]
        try:
            child = next(children)
            if child not in visited:
                depth = depth_of_node[parent] + 1
                if not max_depth or depth < max_depth: 
                  yield parent,child
                  visited.add(child)
                  depth_of_node[child] = depth
                  stack.append((child,iter(G[child])))
        except StopIteration:
            stack.pop(0)

class TideImageGraph(object):
  """ The image graph annotated with tide info. """
  
  def __init__(self, image_graph_uri, tide_uri):
    self.tide = tide.GetCachedTideDataset(tide_uri)    
    ok, image_graph_data = py_imagegraph.LoadImageGraph(image_graph_uri)
    CHECK(ok) 
    
    self.graph = nx.Graph()
    self.graph.add_nodes_from(range(0, len(image_graph_data.vertices)))
    
    self.vertexid_to_imageid = [v.image_id for v in image_graph_data.vertices]
    
    self.imageid_to_vertexid = {}
    for vertex_id, image_id in enumerate(self.vertexid_to_imageid):
      self.imageid_to_vertexid[image_id] = vertex_id
    
    for node_id in self.graph.nodes_iter():
      image_id = self.vertexid_to_imageid[node_id]
      node = self.graph.node[node_id]
      node['image_id'] = image_id
      node['tide_label'] = self.tide.GetLabel(image_id)    
    
    for edge in image_graph_data.edges:
      self.graph.add_edge(edge.src, edge.dst, weight = edge.weight, nfa = edge.nfa)
    
    # load tide annotation for vertices
    
        
    return  
  
  def VertexIdToImageId(self, vertex_id):
    CHECK_LE(vertex_id, len(self.vertexid_to_imageid))    
    return self.vertexid_to_imageid[vertex_id]
  
  def ImageIdToVertexId(self, image_id):    
    return self.imageid_to_vertexid[image_id]
  
  
  def GetNeighborhoodOfImage(self, image_id, num_hops, max_vertex_count):
    """ Returns the up to max_vertex_count neighbors from a BFS expansion around
        image_id.  Returns a network X graph to which a layout can be applied."""
        
    # do bfs from source image
    vertex_id = self.ImageIdToVertexId(image_id)
    neighbors = set()
    for e in bfs_edges(self.graph, vertex_id, num_hops):
      neighbors.add(e[0])
      neighbors.add(e[1])
      if (len(neighbors) >= max_vertex_count):
        break
    
    #print 'creating subgraph'
    neighborhood = self.graph.subgraph(list(neighbors))
    #print 'done'
    return neighborhood  
    
  
  def GetObjectSubgraph(self, object_id):
    CHECK(object_id in self.tide.objectid_to_object, 'unexpected')
    obj = self.tide.objectid_to_object[object_id]
    object_vertices = [self.ImageIdToVertexId(image_id) for image_id in (obj.pos_image_ids + obj.none_image_ids + obj.neg_image_ids) if image_id < 1e16]
    
    well_matched_vertices = []
    for vertex in object_vertices:
      for src, dst in self.graph.edges(vertex):
        nfa = self.graph.get_edge_data(src, dst,)['nfa']
        if nfa < -10.0:
          well_matched_vertices.append(vertex)
          break
    
    print 'creating subgraph'
    subgraph = self.graph.subgraph(well_matched_vertices)
    
    LOG(INFO, 'starting layout')
    pos = nx.spring_layout(subgraph)
    LOG(INFO, 'done layout')
    print 'done'
    return subgraph      
    
  
  