#!/usr/bin/python

import os
import igraph
import cPickle as pickle
from snap.google.base import py_base
from iw import iw_pb2
from iw import py_imagegraph
from tide import tide_pb2
from tide import tide
from pert import py_pert
from pyglog import *
from iw import py_imagegraph
from iw import util as iwutil

def GetCachedTideImageGraph(base_uri, image_graph_uri, tide_uri):
  hash_str = ''
  for uri in [image_graph_uri, tide_uri]:
    if uri: 
      ok, scheme, path, msg = py_pert.ParseUri(uri)
      hash_str += (uri + str(os.path.getmtime(path)) + str(os.path.getsize(path)))    
  hash_val = hash(hash_str)
  cache_path = '%s/tide_image_graph.%s.pickle' % (base_uri, hash_val)
  
  tide_image_graph = None
  if False and os.path.exists(cache_path):
    f = open(cache_path, 'r')
    tide_image_graph = pickle.load(f)
  else:
    tide_image_graph = TideImageGraph(image_graph_uri, tide_uri)
    f = open(cache_path, 'w')
    pickle.dump(tide_image_graph, f, protocol=2)            
  return tide_image_graph


class TideImageGraph(object):
  """ The image graph annotated with tide info. """
  def __init__(self, image_graph_uri, tide_uri):
    
    self.tide = None
    if tide_uri:
      #self.tide = tide.GetCachedTideDataset(tide_uri)
      self.tide = tide.TideDataset(tide_uri)
          
    ok, image_graph_data = py_imagegraph.LoadImageGraph(image_graph_uri)
    CHECK(ok) 
    num_images = len(image_graph_data.vertices)
    
    # load proto data into an igraph graph object
    LOG(INFO, 'loading verts')
    g = igraph.Graph(num_images, directed=False)
    self.vertexid_to_imageid = [v.image_id for v in image_graph_data.vertices]
    self.imageid_to_vertexid = {}
    for vertex_id, image_id in enumerate(self.vertexid_to_imageid):
      self.imageid_to_vertexid[image_id] = vertex_id
    
    for node_id, node in enumerate(image_graph_data.vertices):
      image_id = self.vertexid_to_imageid[node_id]
      node = g.vs[node_id]      
      node['image_id'] = image_id
      
      if self.tide:
        label_data = self.tide.GetLabel(image_id)
        if label_data != None:        
          node['object_id'] = label_data.object_id
          node['label'] = label_data.label      
    
    num_edges = len(image_graph_data.edges)
    progress = iwutil.MakeProgressBar(num_edges)
    prev_edges = set()
    LOG(INFO, 'loading edges')
    edge_list = []
    edge_weight_values = []
    edge_nfa_values = []
    for edge_id, edge in enumerate(image_graph_data.edges):
      if (edge.src, edge.dst) in prev_edges:        
        continue
      if edge.weight < 0.15:
        continue
      new_edge = (edge.src, edge.dst)
      prev_edges.add(new_edge)
      edge_list.append(new_edge)
      edge_weight_values.append(edge.weight)
      edge_nfa_values.append(edge.nfa)
      progress.update(edge_id)
      
    LOG(INFO, 'adding edges')               
    g.add_edges(edge_list)
    
    LOG(INFO, 'adding edge properties')
    g.es["weight_as_similarity"] = edge_weight_values
    g.es["nfa"] = edge_nfa_values
      
    self.graph = g    
    return  
  
  def VertexIdToImageId(self, vertex_id):
    CHECK_LE(vertex_id, len(self.vertexid_to_imageid))    
    return self.vertexid_to_imageid[vertex_id]
  
  def ImageIdToVertexId(self, image_id):    
    return self.imageid_to_vertexid[image_id]
  
  def GetImageIds(self,):    
    return self.imageid_to_vertexid.iterkeys()
  
  
  def GetImageDegree(self, image_id):    
    vertex_id = self.ImageIdToVertexId(image_id)
    return self.graph.degree(vertex_id)
  

  def GetClustering(self, base_path):
    cache_path = '%s/cluster_results.pickle' % (base_path)
    results = None
    if os.path.exists(cache_path):      
      results = iwutil.LoadObject(cache_path)
    else:
      results = self.ComputeClustering()
      iwutil.SaveObject(results, cache_path)        
    return results
  
  def ComputeClustering(self):  
    """ Computes clusters. """
    LOG(INFO, 'Computing clustering')  
    # remove any singleton nodes for efficiency
    #g = self.graph.induced_subgraph(self.graph.vs.select(_degree_gt=0))
    dend = self.graph.community_walktrap( weights='weight_as_similarity', steps=4)
    #dend = self.graph.community_fastgreedy( weights='weight_as_similarity')
    clustering = dend.as_clustering()
    LOG(INFO, 'done clustering')
    return clustering
    
  def GetNeighborhoodOfImage(self, image_id, num_hops, max_vertex_count):
    """ Returns the up to max_vertex_count neighbors from a BFS expansion around
        image_id.  Returns a subg graph"""
    # do bfs from source image
    vertex_id = self.ImageIdToVertexId(image_id)
    neighbors = set()
    for vertex, depth, parent  in self.graph.bfsiter(vertex_id, advanced=True):
      if depth > num_hops:
        break
      neighbors.add(vertex)
      if (len(neighbors) >= max_vertex_count):
        break
    neighborhood = self.graph.subgraph(list(neighbors))
    return neighborhood      
  
  