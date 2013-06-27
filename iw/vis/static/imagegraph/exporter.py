#!/usr/bin/python

from iw.vis.static.imagegraph import tide_image_graph
#from iw.vis.export import renderers
from snap.pert import py_pert
from snap.pyglog import *
from snap.google.base import py_base
from iw import iw_pb2
from iw import util as iwutil
import os
import operator
#import zlib
import json
#import string
import shutil
from jinja2 import Template
from jinja2 import Environment, PackageLoader


def JSPad(image_id):
  return '%020d' % (image_id) # pad with zeros so string sort = numerical sort (using assumption image_id is 64bit)


class ImageGraphExporter:
  def __init__(self, output_path, images_uri, matches_uri, image_graph_uri, tide_uri = None):
    self.output_path = output_path
    self.images_uri = images_uri
    self.matches_uri = matches_uri
    self.image_graph_uri = image_graph_uri
    self.tide_uri = tide_uri
    self.image_graph = tide_image_graph.GetCachedTideImageGraph(self.output_path, self.image_graph_uri, self.tide_uri)
    self.imageid_to_size = {}
    print 'done loading'
    return
  
  def Run(self):      
    self.ExportJavaScript()
    self.ExportImages()    
    self.ExportMatchesJson()    
    self.ExportGraphJson()
    self.ExportClusterSummary()
    self.ExportGraphViewer()
    return
  
  def ExportJavaScript(self):
    js_dir = '%s/js/' % os.path.dirname(os.path.abspath(__file__))
    base_path = '%s/js/' % (self.output_path)
    if not os.path.exists(base_path):
      shutil.copytree(js_dir, base_path)
    return
    
  
  def ExportGraphViewer(self):
    base_path = '%s/imagegraph.html' % (self.output_path)
    tide_data = None
    colors = []
    tide = self.image_graph.tide
    if tide:
      tide_data = {}
      num_tide_objects = len(tide.objectid_to_object)
      colors = iwutil.GenerateDistinctColorsCSS(num_tide_objects)      
      for i, obj in enumerate(tide.objectid_to_object.itervalues()):
        tide_data[obj.id] =  {'name' :  obj.name, 'color' : colors[i]}
    env = Environment(loader=PackageLoader('iw.vis.static.imagegraph', '.'))
    template = env.get_template('graph_template.html')
    params = {'tide_data' : json.dumps(tide_data) }
    html = template.render(params)
    open(base_path, 'w').write(html)
    return
  
  def ExportClusterSummary(self):
    base_path = '%s/index.html' % self.output_path
    #if os.path.exists(base_path):
    #  return
    LOG(INFO, 'exporting cluster summary...')
    cluster_membership = []    
    for cluster in self.image_graph.GetClustering(self.output_path):
      cluster_membership.append(cluster)
    cluster_membership.sort(key = lambda n : len(n), reverse=True)
    cluster_info = []
    for cluster_members in cluster_membership:
      cluster_size = len(cluster_members)
      if cluster_size < 5:
        break
      cluster_subgraph = self.image_graph.graph.subgraph(cluster_members)
      centrality = cluster_subgraph.betweenness(directed=False, cutoff=None)
      index, value = max(enumerate(centrality), key=operator.itemgetter(1))
      central_vertex = cluster_subgraph.vs[index]
      #print cluster_size, central_vertex['image_id']
      cluster_info.append((cluster_size, JSPad(central_vertex['image_id'])))  # need str because js can't handle 64 bit int
    env = Environment(loader=PackageLoader('iw.vis.static.imagegraph', '.'))
    template = env.get_template('index_template.html')
    CHECK(template)
    params = {'cluster_info' : cluster_info }
    html = template.render(params)
    open(base_path, 'w').write(html)
    return
  

  def ExportGraphJson(self):
    base_path = '%s/graph/' % self.output_path
    if os.path.exists(base_path):
      return
    LOG(INFO, 'exporting graph json...')
    os.mkdir(base_path)
    g = self.image_graph.graph
    progress = iwutil.MakeProgressBar(g.vcount())
    for i, vertex in enumerate(g.vs):
      vertex_index = vertex.index
      image_id = vertex['image_id']
      
      
      data = {'image_id': JSPad(image_id)} # need str because js can't handle 64 bit int
      neighbors = [JSPad(n['image_id']) for n in vertex.neighbors()]
      weights = [g.es[g.get_eid(vertex_index, n.index)]['weight_as_similarity'] for n in vertex.neighbors()]
      
      data['neighbors'] = neighbors
      
      tide_label = None
      try:
        tide_label = self.image_graph.tide.imageid_to_label[image_id]
      except:
        pass
      if tide_label:
        data['tide_object_id'] = tide_label.object_id
        data['tide_object_label'] = tide_label.label
                
      data['weights'] = weights      
      filename = '%s/%s.json' % (base_path, JSPad(image_id))
      open(filename, 'w').write(json.dumps(data))
      progress.update(i)
    return
  
  def ExportImages(self):
    image_size_cache_filename = '%s/images/size_cache.pickle' % self.output_path
    if os.path.exists(image_size_cache_filename):
      self.imageid_to_size = iwutil.LoadObject(image_size_cache_filename)
      return
    base_path = '%s/images/' % (self.output_path)
    os.mkdir(base_path)
    LOG(INFO, 'exporting images...')
    reader = py_pert.StringTableReader()
    CHECK(reader.Open(self.images_uri))    
    jpeg_image = iw_pb2.JpegImage()
    progress = iwutil.MakeProgressBar(reader.Entries())
    for i, (k,v) in enumerate(reader):      
      image_id = py_base.KeyToUint64(k)
      jpeg_image.ParseFromString(v)      
      filename = '%s/%s.jpg' % (base_path, JSPad(image_id))
      f = open(filename, 'wb')
      f.write(jpeg_image.data)
      f.close()
      self.imageid_to_size[image_id] = (jpeg_image.width, jpeg_image.height)
      progress.update(i)          
    iwutil.SaveObject(self.imageid_to_size, image_size_cache_filename)    
    return
  
  
  def ExportMatchesJson(self):
    base_path = '%s/matches/' % (self.output_path)
    if os.path.exists(base_path):
      return
    LOG(INFO, 'exporting match json...')
    os.mkdir(base_path)
    reader = py_pert.StringTableReader()
    CHECK(reader.Open(self.matches_uri))    
    progress = iwutil.MakeProgressBar(reader.Entries())
    match_result = iw_pb2.GeometricMatchResult()
    for i, (k,v) in enumerate(reader):
      image_a_id, image_b_id = iwutil.ParseUint64KeyPair(k)      
      match_result.ParseFromString(v)
      if not match_result.matches:
        continue
      filename = '%s/%s_%s.json' % (base_path, JSPad(image_a_id), JSPad(image_b_id))
      f = open(filename, 'w')
      data = {}
      
      CHECK_LT(image_a_id, image_b_id)
      
      data['image_a'] = JSPad(image_a_id) # use string because js can't handle 64bit int
      data['image_b'] = JSPad(image_b_id) # use string because js can't handle 64bit int
      data['image_a_size'] = self.imageid_to_size[image_a_id]
      data['image_b_size'] = self.imageid_to_size[image_b_id]
      
      matches = []
      for match in match_result.matches:
        for c in match.correspondences:
          match_info = [c.a.pos.x, c.a.pos.y, c.a.radius, c.b.pos.x, c.b.pos.y, c.b.radius]
          matches.append(match_info)
      data['matches'] = matches    
      f.write(json.dumps(data))
      progress.update(i)           
    return
  
  
  
  
#  def ExportMatchImages(self):
#    LOG(INFO, 'exporting match images...')
#    try:
#      os.mkdir('./matches/')
#    except:
#      pass
#    
#    reader = py_pert.StringTableReader()
#    CHECK(reader.Open(self.matches_uri))    
#    progress = iwutil.MakeProgressBar(reader.Entries())
#    match_result = iw_pb2.GeometricMatchResult()
#    for i, (k,v) in enumerate(reader):
#      image_a_id, image_b_id = iwutil.ParseUint64KeyPair(k)      
#      match_result.ParseFromString(v)
#      if not match_result.matches:
#        continue
#      filename = './matches/%d_%d.svgz' % (image_a_id, image_b_id)
#      f = open(filename, 'w')
#      svg = self.RenderSvg(match_result)
#      svgz = zlib.compress(svg)
#      f.write(svgz)
#      progress.update(i)           
#
#    return
#  
#  def RenderSvg(self, match_result):
#    svg = None
#    image_a_id = match_result.image_a_id
#    image_b_id = match_result.image_b_id
#    
#    CHECK(image_a_id in self.imageid_to_size)
#    CHECK(image_b_id in self.imageid_to_size)
#    image_a_width, image_a_height = self.imageid_to_size[image_a_id]
#    image_b_width, image_b_height = self.imageid_to_size[image_b_id]
#    
#    # render svg
#    width = image_a_width + image_b_width
#    height = max(image_a_height, image_b_height)
#    svg = """<?xml version="1.0" standalone="no"?><!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">"""
#    #svg += """<style> line { opacity: 0.25;} line:hover { opacity: 1.0;} </style>"""
#    svg += '<svg width="%d" height="%d" version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">' % (width, height)
#    svg += '<image x="%dpx" y="%dpx" width="%dpx" height="%dpx" xlink:href="../images/%s.jpg"> </image> \n' % (0, 0, image_a_width, image_a_height, image_a_id);    
#    svg += '<image x="%dpx" y="%dpx" width="%dpx" height="%dpx" xlink:href="../images/%s.jpg"> </image> \n' % (image_a_width, 0, image_b_width, image_b_height, image_b_id);
#    
#    colors = ['yellow', 'red', 'blue', ]
#    match_info = []
#    for match_index, match in enumerate(match_result.matches):
#      match_info.append(match.nfa)
#      color = colors[match_index%len(colors)]
#      for correspondence_index, c in enumerate(match.correspondences):
#        a_x = c.a.pos.x
#        a_y = c.a.pos.y
#        b_x = c.b.pos.x + image_a_width
#        b_y = c.b.pos.y
#        #svg += '<line x1="%d" y1="%d" x2="%d" y2="%d" style="stroke:%s;stroke-width:2"/>\n' % (x1,y1,x2,y2, color)
#        
#        left_pt_id = "lp%d" %(correspondence_index)
#        right_pt_id = "rp%d" % (correspondence_index)
#
#        svg += "<circle id=\"%s\" cx=\"%f\" cy=\"%f\" r=\"3\" stroke=\"black\" stroke-width=\"0\" fill=\"%s\"/>\n" % (left_pt_id, a_x, a_y, color)
#        svg += "<circle id=\"%s\" cx=\"%f\" cy=\"%f\" r=\"3\" stroke=\"black\" stroke-width=\"0\" fill=\"%s\"/>\n" % (right_pt_id, b_x, b_y, color)
#
#        svg += "<circle id=\"%s_support\" cx=\"%f\" cy=\"%f\" r=\"%f\" stroke-width=\"5\" fill=\"none\" opacity=\"0.5\" stroke=\"%s\" >\n" % (left_pt_id, a_x, a_y, c.a.radius, color)
#        svg += "<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>" % (left_pt_id, left_pt_id)
#        svg += "<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>" % (right_pt_id, right_pt_id)
#        svg += "</circle>"
#
#        svg += "<circle id=\"%s_support\" cx=\"%f\" cy=\"%f\" r=\"%f\" stroke-width=\"5\" fill=\"none\" opacity=\"0.5\" stroke=\"%s\" >\n" % ( right_pt_id, b_x, b_y, c.b.radius, color)
#        svg += "<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>" %(left_pt_id, left_pt_id)
#        svg += "<set attributeName=\"opacity\" from=\"0.5\" to=\"1.0\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>" %(right_pt_id, right_pt_id)
#        svg += "</circle>"
#
#        svg += "<line x1=\"%f\" y1=\"%f\" x2=\"%f\" y2=\"%f\" style=\"stroke:rgb(255,0,0);stroke-width:2\" visibility=\"hidden\">" % (a_x, a_y, b_x, b_y)
#        svg += "<set attributeName=\"visibility\" from=\"hidden\" to=\"visible\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>" % ( left_pt_id, left_pt_id)
#        svg += "<set attributeName=\"visibility\" from=\"hidden\" to=\"visible\" begin=\"%s.mouseover\" end=\"%s.mouseout\"/>" % (right_pt_id, right_pt_id)
#        svg += "</line>"
#        
#    svg += '</svg>'
#    
#    return svg
  

if __name__ == "__main__":
  
  
    #dataset_name = 'tide_v08_distractors'
    #base_uri = 'local:////home/ubuntu/Desktop/vol-7f209e0c/itergraph/%s' % dataset_name
    #self.images_uri = '%s/cropped_scaled_photoid_to_image.pert' % (base_uri)
    #self.matches_uri = '%s/usift/cbir/9e6c60d825c5a9814e19c0a735747011/itergraph/0ffd08c74629993310fd73f6461673af/merged_matches.pert' % (base_uri)
    #self.image_graph_uri = '%s/usift/cbir/9e6c60d825c5a9814e19c0a735747011/itergraph/0ffd08c74629993310fd73f6461673af/image_graph.pert' % (base_uri)
    #self.tide_uri = '%s/objectid_to_object.pert' % (base_uri)
    
    
#    dataset_name = 'tide_v12'
#    base_uri = 'local:///home/ubuntu/Desktop/vol-0449ca74/itergraph/%s' % dataset_name
#    self.images_uri = '%s/photoid_to_image.pert' % (base_uri)
#    self.tide_uri = '%s/objectid_to_object.pert' % (base_uri)
#    self.matches_uri = '%s/c76fcf997f9a6083b766e7f30f97f604/cbir/e65781a3f9cdfcd58aadf92f99a2a838/itergraph/830f8900789f6e3e9029c5c7509cd2a0/merged_matches.pert' % (base_uri)
#    self.image_graph_uri = '%s/c76fcf997f9a6083b766e7f30f97f604/cbir/e65781a3f9cdfcd58aadf92f99a2a838/itergraph/830f8900789f6e3e9029c5c7509cd2a0/image_graph.pert' % (base_uri)
    
    
#    dataset_name = 'tide_v18_mixed'
#    base_uri = 'local:///home/ubuntu/Desktop/vol-0449ca74/itergraph/%s' % dataset_name
#    self.images_uri = '%s/photoid_to_image.pert' % (base_uri)
#    self.tide_uri = '%s/objectid_to_object.pert' % (base_uri)
#    self.matches_uri = '%s/e1059d38bd73546eea4e2f6ca9ec0966/cbir/494617b7c40f9dce9faa2f15e3de0041/itergraph/097fed13ba6240199b9e7e1862da58f1/merged_matches.pert' % (base_uri)
#    self.image_graph_uri = '%s/e1059d38bd73546eea4e2f6ca9ec0966/cbir/494617b7c40f9dce9faa2f15e3de0041/itergraph/097fed13ba6240199b9e7e1862da58f1/image_graph.pert' % (base_uri)
    
#    dataset_name = 'tide_v17'
#    base_uri = 'local:///home/ubuntu/Desktop/vol-0449ca74/itergraph/%s' % dataset_name
#    self.images_uri = '%s/photoid_to_image.pert' % (base_uri)
#    self.tide_uri = '%s/objectid_to_object.pert' % (base_uri)
#    self.matches_uri = '%s/7192be9ebd89da866039ed7cbb3d6011/cbir/494617b7c40f9dce9faa2f15e3de0041/itergraph/a6a2155b8d4f91f6114cebb36efd1d5a/merged_matches.pert' % (base_uri)
#    self.image_graph_uri = '%s/7192be9ebd89da866039ed7cbb3d6011/cbir/494617b7c40f9dce9faa2f15e3de0041/itergraph/a6a2155b8d4f91f6114cebb36efd1d5a/image_graph.pert' % (base_uri)
  
  app = Exporter(images_uri, matches_uri, image_graph_uri, tide_uri)
  app.Run()
    
  
