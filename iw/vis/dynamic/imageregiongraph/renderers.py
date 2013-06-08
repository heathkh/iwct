from pert import py_pert
from snap.pyglog import *
from snap.google.base import py_base
import base64
from iw import iw_pb2
import json
import string
from iw import visutil

def JpegToDataUrl(jpeg_data):
  dataurl = 'data:image/jpeg;base64,' + base64.b64encode(jpeg_data)
  return dataurl


class GraphNeighborhoodRenderer(object):
  """ Draws a force directed layout of a part of the image region graph around a given vertex. """
  def __init__(self, graph):
    self.graph = graph
    return
  
  def RenderHtml(self, center_vertex_id, num_hops, max_num_neighbors, size_pixels):
    html = ''
    neighborhood_graph = self.graph.GetNeighborhood(center_vertex_id, num_hops, max_num_neighbors)
    
    # generate json that maps the node index to the node name (here the image id) and node group (here the tide object id)
    node_data = []
    vertexid_to_nodeid = {}
    for node_id, vertex_id in enumerate(neighborhood_graph.nodes()):
      vertexid_to_nodeid[vertex_id] = node_id
      #CHECK(vertex_id in neighborhood_graph.node, 'vertex_id not found: %d' % (vertex_id))
      node = neighborhood_graph.node[vertex_id]
      print node
      node_data.append({'vertex_id': vertex_id, 
                        'image_id': str(node['image_id']), # need to make this a string in python because javascript can't handle 64bit integers
                        'tide_object_id' : node['tide_label'].object_id,
                        'tide_label' : node['tide_label'].label,
                        'bounding_box' : node['bounding_box'],
                        'type' : node['type']                                  
                         })

    edge_data = []
    for v_a, v_b in neighborhood_graph.edges():
      node_a = vertexid_to_nodeid[v_a]
      node_b = vertexid_to_nodeid[v_b]      
      weight = neighborhood_graph.edge[v_a][v_b]['weight']
      edge_data.append({'source': node_a, 'target' : node_b, 'weight' : weight })

    tide_data = {}
    num_tide_objects = len(self.graph.tide.objectid_to_object)
    colors =  visutil.GenerateDistinctColorsCSS(num_tide_objects)
    for i, obj in enumerate(self.graph.tide.objectid_to_object.itervalues()):
      tide_data[obj.id] =  {'name' :  obj.name, 'color' : colors[i]}
      
    params = {'central_region_id' : center_vertex_id,
              'node_json': json.dumps(node_data),
              'edge_json' : json.dumps(edge_data),
              'tide_json' : json.dumps(tide_data),
              'size' : size_pixels,
              'title' : 'neighborhood of %d' % (center_vertex_id)              
              }
    
    html = open('header.html').read()
    html += open('style.html').read()
    
    html += string.Template(open('graph.html').read()).substitute(params)
    html += open('footer.html').read()
    return html      
  
  
