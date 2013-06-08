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

class MatchRenderer(object):
  """ Renders and SVG diagram of the result of matching a pair of images. """
  def __init__(self, matches_uri, images_uri):
    
    # ensure matches is a merged version with only one shard (otherwise we have mem problems)
    #shard_uris = py_pert.GetShardUris(matches_uri)
    #num_shards = len(shard_uris)
    #CHECK_EQ(num_shards, 1, 'expectred merged matches pert with 1 shard but got %d shards' % num_shards) 
    self.match_reader = py_pert.StringTableReader()
    CHECK(self.match_reader.Open(matches_uri))
    
    # open images table
    
    self.image_reader = py_pert.StringTableReader()
    CHECK(self.image_reader.Open(images_uri))
    
    return
  
  
  def RenderSvg(self, image_a_id, image_b_id):
    svg = None
    # look up match for this image pair
    if image_a_id > image_b_id:
      image_a_id, image_b_id = (image_b_id, image_a_id)     
    CHECK_LT(image_a_id, image_b_id)
    match_result = iw_pb2.GeometricMatchResult()
    match_key = py_base.Uint64ToKey(image_a_id) + py_base.Uint64ToKey(image_b_id)
    ok, value = self.match_reader.Find(match_key)
    CHECK(ok, 'cant find key: %d %d' % (image_a_id, image_b_id) )
    match_result.ParseFromString(value)
    
    # get image data
    id_to_jpeg = self._BatchGetImages([image_a_id, image_b_id])
    image_a = id_to_jpeg[image_a_id]
    image_b = id_to_jpeg[image_b_id]
    
    # render svg
    width = image_a.width + image_b.width
    height = max(image_a.height, image_b.height)
    
    
    svg = """<?xml version="1.0" standalone="no"?><!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">"""
    svg += """<style> line { opacity: 0.25;} line:hover { opacity: 1.0;} </style>"""
    svg += '<svg width="%d" height="%d" version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">' % (width, height)
    svg += '<image x="%dpx" y="%dpx" width="%dpx" height="%dpx" xlink:href="%s"> </image> \n' % (0, 0, image_a.width, image_a.height, JpegToDataUrl(image_a.data));    
    svg += '<image x="%dpx" y="%dpx" width="%dpx" height="%dpx" xlink:href="%s"> </image> \n' % (image_a.width, 0, image_b.width, image_b.height, JpegToDataUrl(image_b.data));
    
    
    colors = ['yellow', 'red', 'blue', ]
    match_info = []
    for match_index, match in enumerate(match_result.matches):
      match_info.append(match.nfa)
      color = colors[match_index%len(colors)]
      for correspondence_index, c in enumerate(match.correspondences):
        a_x = c.a.pos.x
        a_y = c.a.pos.y
        b_x = c.b.pos.x + image_a.width
        b_y = c.b.pos.y
        #svg += '<line x1="%d" y1="%d" x2="%d" y2="%d" style="stroke:%s;stroke-width:2"/>\n' % (x1,y1,x2,y2, color)
        
        left_pt_id = "lp%d" %(correspondence_index)
        right_pt_id = "rp%d" % (correspondence_index)

        svg += "<circle id=\"%s\" cx=\"%f\" cy=\"%f\" r=\"3\" stroke=\"black\" stroke-width=\"0\" fill=\"%s\"/>\n" % (left_pt_id, a_x, a_y, color)
        svg += "<circle id=\"%s\" cx=\"%f\" cy=\"%f\" r=\"3\" stroke=\"black\" stroke-width=\"0\" fill=\"%s\"/>\n" % (right_pt_id, b_x, b_y, color)

        svg += "<circle id=\"%s_support\" cx=\"%f\" cy=\"%f\" r=\"%f\" stroke-width=\"5\" fill=\"none\" opacity=\"0.5\" stroke=\"%s\" >\n" % (left_pt_id, a_x, a_y, c.a.radius, color)
        svg += "<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>" % (left_pt_id, left_pt_id)
        svg += "<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>" % (right_pt_id, right_pt_id)
        svg += "</circle>"

        svg += "<circle id=\"%s_support\" cx=\"%f\" cy=\"%f\" r=\"%f\" stroke-width=\"5\" fill=\"none\" opacity=\"0.5\" stroke=\"%s\" >\n" % ( right_pt_id, b_x, b_y, c.b.radius, color)
        svg += "<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>" %(left_pt_id, left_pt_id)
        svg += "<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>" %(right_pt_id, right_pt_id)
        svg += "</circle>"

        svg += "<line x1=\"%f\" y1=\"%f\" x2=\"%f\" y2=\"%f\" style=\"stroke:rgb(255,0,0);stroke-width:2\" visibility=\"hidden\">" % (a_x, a_y, b_x, b_y)
        svg += "<set attributeName=\"visibility\" from=\"hidden\" to=\"visible\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>" % ( left_pt_id, left_pt_id)
        svg += "<set attributeName=\"visibility\" from=\"hidden\" to=\"visible\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>" % (right_pt_id, right_pt_id)
        svg += "</line>"
        
    svg += '</svg>'
    
    for info in match_info:
      svg += '<pre>nfa: %s</pre>' % (info)
    
    return svg
  
  def _BatchGetImages(self, image_ids):      
    """ Does efficient batch lookup of images returning a dict mapping image_id to raw jpeg data. """
    id_to_jpeg = {}
    image_ids.sort()
    
    for image_id in image_ids:
      ok, value = self.image_reader.Find(py_base.Uint64ToKey(image_id))
      CHECK(ok)
      jpeg = iw_pb2.JpegImage()
      jpeg.ParseFromString(value)
      CHECK(jpeg.IsInitialized())
      id_to_jpeg[image_id] = jpeg
    
    return id_to_jpeg 
  
      

class TideImageGraphRenderer(object):
  """ Draws a force directed layout of a part of the image graph around a given image. """
  
  def __init__(self, graph):
    self.graph = graph
    return
  
  def RenderHtml(self, neighborhood_graph, size_pixels, central_image_id = None, title = ''):
    html = ''
    
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
                         })

    edge_data = []
    for v_a, v_b in neighborhood_graph.edges():
      node_a = vertexid_to_nodeid[v_a]
      node_b = vertexid_to_nodeid[v_b]      
      weight = neighborhood_graph.edge[v_a][v_b]['weight']
      nfa = neighborhood_graph.edge[v_a][v_b]['nfa']
      edge_data.append({'source': node_a, 'target' : node_b, 'weight' : weight, 'nfa' : nfa })

    tide_data = {}
    num_tide_objects = len(self.graph.tide.objectid_to_object)
    colors =  visutil.GenerateDistinctColorsCSS(num_tide_objects)
    for i, obj in enumerate(self.graph.tide.objectid_to_object.itervalues()):
      tide_data[obj.id] =  {'name' :  obj.name, 'color' : colors[i]}
      
    
    params = {'central_image_id' : central_image_id,
              'node_json': json.dumps(node_data),
              'edge_json' : json.dumps(edge_data),
              'tide_json' : json.dumps(tide_data),
              'size' : size_pixels,
              'title' : title              
              }
    
    html = open('header.html').read()
    html += open('style.html').read()
    
    html += string.Template(open('graph.html').read()).substitute(params)
    html += open('footer.html').read()
    return html      
  
  
