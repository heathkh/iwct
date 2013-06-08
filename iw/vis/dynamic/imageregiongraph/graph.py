#!/usr/bin/python

import os
#from pert import py_pert
from snap.google.base import py_base
from iw import iw_pb2
from iw import py_imagegraph
from tide import tide
from tide import tide_pb2
from snap.pert import py_pert
import string
import json
import cPickle as pickle
import bisect
from iw import py_imagegraph
import networkx as nx
from iw import iw_pb2
from snap.pyglog import *
from iw import util as iwutil


def GetCachedGraph(irg_uri, tide_uri):
  hash_str = ''
  for uri in [irg_uri, tide_uri]:
    ok, scheme, path, error = py_pert.ParseUri(tide_uri)    
    hash_str += (uri + str(os.path.getmtime(path)) + str(os.path.getsize(path)))    
  hash_val = hash(hash_str)
  cache_path = './graph.%s.pickle' % (hash_val)
  graph = None
  if os.path.exists(cache_path):
    f = open(cache_path, 'r')
    graph = pickle.load(f)
  else:
    graph = Graph(irg_uri, tide_uri)
    f = open(cache_path, 'w')
    pickle.dump(graph, f, protocol=2)            
  return graph


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



def LoadImageRegionGraph(uri):
  irg = iw_pb2.ImageRegionGraph()
  reader = py_pert.StringTableReader()
  CHECK(reader.Open(uri))
  ok, tmp = reader.GetMetadata("num_edges")
  num_edges = long(tmp);
  ok, tmp = reader.GetMetadata("num_vertices")
  CHECK(ok, "this doesn't appear to be a irg uri: %s" % (uri))
  num_vertices = long(tmp)
  CHECK_EQ(reader.Entries(), num_edges + num_vertices)
  
  progress = iwutil.MakeProgressBar(num_edges)
  # load edges
  for i in range(num_edges):
    ok, key, value = reader.Next()
    CHECK(ok)
    CHECK_EQ(key[0], 'e')
    irg.edge.add().ParseFromString(value)
    progress.update(i)

  # load vertices
  progress = iwutil.MakeProgressBar(num_vertices)
  for i in range(num_vertices):
    ok, key, value = reader.Next()
    CHECK(ok)
    CHECK_EQ(key[0], 'v')
    irg.vertex.add().ParseFromString(value)
    progress.update(i)
    
  return irg

class Graph(object):
  """ The image region graph annotated with tide info. """
  def __init__(self, irg_uri, tide_uri):
    self.tide = tide.GetCachedTideDataset(tide_uri)    
    irg = LoadImageRegionGraph(irg_uri)
    self.graph = nx.Graph()
    self.graph.add_nodes_from(range(0, len(irg.vertex)))    
    for vertex_id in self.graph.nodes_iter():
      node = self.graph.node[vertex_id]
      v = irg.vertex[vertex_id]
      image_id = v.image_id
      node['image_id'] = image_id
      node['tide_label'] = self.tide.GetLabel(image_id)
      bb = v.bounding_box    
      node['bounding_box'] = [bb.x1, bb.y1, bb.x2, bb.y2]
      node['type'] = v.type     
    for edge in irg.edge:
      self.graph.add_edge(edge.src, edge.dst, weight = edge.weight)        
    return  
  
  def GetNeighborhood(self, vertex_id, num_hops, max_vertex_count):
    """ Returns the up to max_vertex_count neighbors from a BFS expansion around
        image_id.  Returns a network X graph to which a layout can be applied."""
    # do bfs from source image
    neighbors = set()
    for e in bfs_edges(self.graph, vertex_id, num_hops):
      neighbors.add(e[0])
      neighbors.add(e[1])
      if (len(neighbors) >= max_vertex_count):
        break
    print 'creating subgraph'
    neighborhood = self.graph.subgraph(list(neighbors))
    print 'done'
    return neighborhood  
    
    
    
  
  