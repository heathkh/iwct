#!/usr/bin/python
#from iw import iw_pb2
#import sys
import tornado.ioloop
import tornado.web
#import os
from iw.eval.labelprop.eval2 import eval2_pb2
from iw.eval.labelprop.eval2 import py_eval2
from iw import py_imageregiongraph
from iw import iw_pb2
from snap.pert import py_pert
#from tide import tide_pb2 
#from string import Template
from snap.pyglog import *
from iw import visutil
import json
import numpy as np
from tide import tide

from iw import util as iwutil


class ImageRegionGraphReader(object):
  def __init__(self, uri):
    self.reader = py_pert.StringTableShardReader()
    CHECK(self.reader.Open(uri + '/part-00000'))
    ok, tmp = self.reader.GetMetadata("num_edges")
    self.num_edges = long(tmp);
    ok, tmp = self.reader.GetMetadata("num_vertices")
    CHECK(ok, "this doesn't appear to be a irg uri: %s" % (uri))
    self.num_vertices = long(tmp)
    CHECK_EQ(self.reader.Entries(), self.num_edges + self.num_vertices)
    return
  
  def BatchGetRegions(self, region_indices):
     region_indices.sort()
     vertex_dict = {}
     offset = self.num_edges
     for index in region_indices:       
       ok, key, value = self.reader.GetIndex(index + offset)
       CHECK(ok)
       CHECK_EQ(key[0], 'v')
       CHECK_EQ(long(key[1:]),  index)
       vertex = iw_pb2.ImageRegionGraph.Vertex()
       vertex.ParseFromString(value)
       vertex_dict[index] = vertex
     return vertex_dict
         

#def LoadImageRegionGraph(uri):
#  irg = iw_pb2.ImageRegionGraph()
#  reader = py_pert.StringTableReader()
#  CHECK(reader.Open(uri))
#  ok, tmp = reader.GetMetadata("num_edges")
#  num_edges = long(tmp);
#  ok, tmp = reader.GetMetadata("num_vertices")
#  CHECK(ok, "this doesn't appear to be a irg uri: %s" % (uri))
#  num_vertices = long(tmp)
#  CHECK_EQ(reader.Entries(), num_edges + num_vertices)
#  
#  progress = iwutil.MakeProgressBar(num_edges)
#  # load edges
#  for i in range(num_edges):
#    ok, key, value = reader.Next()
#    CHECK(ok)
#    CHECK_EQ(key[0], 'e')
#    irg.edge.add().ParseFromString(value)
#    progress.update(i)
#
#  # load vertices
#  progress = iwutil.MakeProgressBar(num_vertices)
#  for i in range(num_vertices):
#    ok, key, value = reader.Next()
#    CHECK(ok)
#    CHECK_EQ(key[0], 'v')
#    irg.vertex.add().ParseFromString(value)
#    progress.update(i)
#  
#  return irg


class MainHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app      
      return            
    
    def get(self):
      params = {'app' : self.app }
      self.render('main.html', **params)
      return


class ConfusionMatrixHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app      
      return    
    
    def get(self):            
      pretty_label_names = [ l.replace('_', ' ') for l in self.app.result.label_names]            
      # generate json that maps the node index to the node name (here the image id) and node group (here the tide object id)
      result = self.app.result
      num_labels = self.app.num_labels
      matrix_headers = []
      for i in range(num_labels):        
        matrix_headers.append({'name': pretty_label_names[i], 'precision' : result.object_precision.mean[i], 'recall' : result.object_recall.mean[i] })
  
      # compute row and col sums
      cm = np.mat(result.confusion_matrix.mean).reshape(num_labels,num_labels)
      max_val = np.max(cm)      
      row_sums = np.squeeze(np.array(np.sum(cm, 1)))
      col_sums = np.squeeze(np.array(np.sum(cm, 0)))
      
      # generate confusion matrix (with different normalizations)
      matrix_unnorm = []
      matrix_precision = []
      matrix_recall = []      
      matrix_std = []
      for row_id in range(num_labels):
        row_unnorm = []
        row_precision = []
        row_recall = []
        row_std = []
        for col_id in range(num_labels):
          mean_image_count = result.confusion_matrix.mean[num_labels*row_id + col_id]
          std_image_count = result.confusion_matrix.std[num_labels*row_id + col_id]
          row_std.append(std_image_count)
          value_unnormalized = mean_image_count
          value_precision = 0
          value_recall = 0
          if row_id < num_labels-1:
            value_precision = mean_image_count / row_sums[row_id]          
          if col_id < num_labels-1:
            value_recall = mean_image_count / col_sums[col_id]        
          row_unnorm.append(value_unnormalized)
          row_precision.append(value_precision)
          row_recall.append(value_recall)          
        matrix_unnorm.append(row_unnorm)
        matrix_precision.append(row_precision)
        matrix_recall.append(row_recall)
        matrix_std.append(row_std)
      
      size = 800
      params = { 'size' : size,
                 'matrix_headers' : json.dumps(matrix_headers), 
                 'matrix_unnorm' : json.dumps(matrix_unnorm),
                 'matrix_precision' : json.dumps(matrix_precision),
                 'matrix_recall' : json.dumps(matrix_recall),
                 'matrix_std' : json.dumps(matrix_std),
                }
      self.render('confusion_matrix.html', **params)
      return
    

class ConfusionMatrixCellHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app      
      return    
    
    def RenderRegionSvg(self, region):
      image_id = region.image_id
      gt_bb = self.app.tide.GetBoundingBoxes(image_id)
      
      jpeg = self.imageid_to_jpeg[image_id]
      bb = region.bounding_box

      max_width = 150.0
      max_height = 150.0
      scaling_w = float(max_width)/jpeg.width      
      scaling_h = float(max_height)/jpeg.height
      
      scaling = min(scaling_w, scaling_h)
      
      svg_width = scaling * jpeg.width
      svg_height =  scaling * jpeg.height
      
      svg = """<?xml version="1.0" standalone="no"?><!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">"""
      svg += """<style> line { opacity: 0.25;} line:hover { opacity: 1.0;} </style>"""
      svg += '<svg width="%d" height="%d" version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">' % (svg_width, svg_height)
      svg += '<g transform="scale(%f)">' % (scaling)
      #svg += '<svg width="100%%" height="100%%" viewBox="0 0 150 150" version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">'
      svg += '<image x="%dpx" y="%dpx" width="%dpx" height="%dpx" xlink:href="%s"> </image> \n' % (0, 0, jpeg.width, jpeg.height, visutil.JpegToDataUrl(jpeg.data));    
      svg += '<rect x="0" y="0" width="%f" height="%f" fill="white" fill-opacity="0.25"/>\n' % (jpeg.width, jpeg.height)
      svg += '<rect x="%f" y="%f" width="%f" height="%f" stroke="red" stroke-width="8" fill="none"/>\n' % (bb.x1, bb.y1, bb.x2-bb.x1, bb.y2-bb.y1)
      
      
      for bb in gt_bb: 
        svg += '<rect x="%f" y="%f" width="%f" height="%f" stroke="green" stroke-width="8" fill="none"/>\n' % (bb.x1, bb.y1, bb.x2-bb.x1, bb.y2-bb.y1)
      svg += '</g>'
      svg += '</svg>'
      return svg
    
    def get(self):      
      # parse arguments
      row = int(self.get_argument('row'))
      col = int(self.get_argument('col'))
      result = self.app.result
      predicted_label = result.label_names[row]
      actual_label = result.label_names[col]
            
      # get list of vertices in that cell
      region_label_data = result.confusion_matrix_item_freq.rows[row].cells[col].items
      region_label_data = region_label_data[0:150] # limit to top 150 regions
      region_indices = [ region.item_id for region in region_label_data]
      regions_dict = self.app.irg_reader.BatchGetRegions(region_indices)
      image_ids = [ region.image_id for region in regions_dict.itervalues()]
      
      if self.app.image_loader == None:
        LOG(INFO, 'loading images...')
        self.app.image_loader = visutil.BatchImageLoader(self.app.images_uri)        
      self.imageid_to_jpeg = self.app.image_loader.BatchGetImages(image_ids)
      
      regionid_to_svg = {}
      for region_id, region in regions_dict.iteritems():
        regionid_to_svg[region_id] = self.RenderRegionSvg(region)
      
      params = { 'actual_label' : actual_label,
                 'predicted_label' : predicted_label,
                 'region_label_data' : region_label_data,
                 'regions_dict' : regions_dict,
                 'regionid_to_svg' : regionid_to_svg,                 
                 'labelid_to_labelnames' : result.label_names
                } 
      self.render('confusion_matrix_cell.html', **params)      
      return


class ViewerApp:
  def __init__(self):
    
    #self.images_uri = 'local://media/vol-0449ca74/itergraph/tide_v14/cropped_scaled_photoid_to_image_randomaccess.pert'
    #self.result_uri = 'local://media/vol-0449ca74/itergraph/tide_v14/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/6b54a6007b3260b250a2671023dea0f0/labelprop_eval2.pert'
    #self.irg_uri = 'local://media/vol-0449ca74/itergraph/tide_v14/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/6b54a6007b3260b250a2671023dea0f0/image_region_graph.pert'
    
#    self.images_uri = 'local://media/vol-0449ca74/itergraph/tide_v14_mixed/photoid_to_image.pert'
#    self.tide_uri = 'local://media/vol-0449ca74/itergraph/tide_v14_mixed/objectid_to_object.pert'
#    self.result_uri = 'local://media/vol-0449ca74/itergraph/tide_v14_mixed/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/e4baab98c10a434d90d092c71ecb566c/labelprop_eval2.pert'
#    self.irg_uri = 'local://media/vol-0449ca74/itergraph/tide_v14_mixed/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/e4baab98c10a434d90d092c71ecb566c/image_region_graph.pert' 
    
#    self.images_uri = 'local://media/vol-0449ca74/itergraph/tide_v14_mixed_v2/photoid_to_image.pert'
#    self.tide_uri = 'local://media/vol-0449ca74/itergraph/tide_v14_mixed_v2/objectid_to_object.pert'
#    self.result_uri = 'local://media/vol-0449ca74/itergraph/tide_v14_mixed_v2/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/e4baab98c10a434d90d092c71ecb566c/labelprop_eval2.pert'
#    self.irg_uri = 'local://media/vol-0449ca74/itergraph/tide_v14_mixed_v2/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/e4baab98c10a434d90d092c71ecb566c/image_region_graph.pert'

#    self.images_uri = 'local://media/vol-0449ca74/itergraph/tide_v16_mixed/photoid_to_image.pert'
#    self.tide_uri = 'local://media/vol-0449ca74/itergraph/tide_v16_mixed/objectid_to_object.pert'
#    self.result_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v16_mixed/8183e99d6c9424eb5bbe029e16c05453/cbir/494617b7c40f9dce9faa2f15e3de0041/itergraph/e4a1dd52c08ec99117aa7813ab66e38b/labelprop_eval2.pert'
#    self.irg_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v16_mixed/8183e99d6c9424eb5bbe029e16c05453/cbir/494617b7c40f9dce9faa2f15e3de0041/itergraph/e4a1dd52c08ec99117aa7813ab66e38b/image_region_graph.pert'

    #self.images_uri = 'local://home/ubuntu/Desktop/my.cluster.com/data/itergraph/tide_v12/photoid_to_image.pert'
    #self.tide_uri = 'local://home/ubuntu/Desktop/my.cluster.com/data/itergraph/tide_v12/objectid_to_object.pert'
    #self.result_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v12/c76fcf997f9a6083b766e7f30f97f604/cbir/e65781a3f9cdfcd58aadf92f99a2a838/itergraph/830f8900789f6e3e9029c5c7509cd2a0/labelprop_eval2.pert'
    #self.irg_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v12/c76fcf997f9a6083b766e7f30f97f604/cbir/e65781a3f9cdfcd58aadf92f99a2a838/itergraph/830f8900789f6e3e9029c5c7509cd2a0/image_region_graph.pert'
    
#    self.images_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/tide_v08_distractors/cropped_scaled_photoid_to_image.pert'
#    self.tide_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/tide_v08_distractors/objectid_to_object.pert'
#    self.result_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/tide_v08_distractors/usift/cbir/9e6c60d825c5a9814e19c0a735747011/itergraph/0ffd08c74629993310fd73f6461673af/labelprop_eval2.pert'
#    self.irg_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/tide_v08_distractors/usift/cbir/9e6c60d825c5a9814e19c0a735747011/itergraph/0ffd08c74629993310fd73f6461673af/image_region_graph.pert'

#    self.images_uri = 'local://media/vol-0449ca74/itergraph/tide_v08/photoid_to_image.pert'
#    self.tide_uri = 'local://media/vol-0449ca74/itergraph/tide_v08/objectid_to_object.pert'
#    self.result_uri = 'local://media/vol-0449ca74/itergraph/tide_v08/usift/cbir/d27df409ad95e12823feed0c658eabeb/itergraph/1b8ba7a00a9d1cd558716ce882e8408f/labelprop_eval2.pert'
#    self.irg_uri = 'local://media/vol-0449ca74/itergraph/tide_v08/usift/cbir/d27df409ad95e12823feed0c658eabeb/itergraph/1b8ba7a00a9d1cd558716ce882e8408f/image_region_graph.pert'
        
    
    self.images_uri = 'local://media/vol-0449ca74/itergraph/tide_v17/photoid_to_image.pert'
    self.tide_uri = 'local://media/vol-0449ca74/itergraph/tide_v17/objectid_to_object.pert'
    self.result_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v17/7192be9ebd89da866039ed7cbb3d6011/cbir/494617b7c40f9dce9faa2f15e3de0041/itergraph/a6a2155b8d4f91f6114cebb36efd1d5a/labelprop_eval2.pert'
    self.irg_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v17/7192be9ebd89da866039ed7cbb3d6011/cbir/494617b7c40f9dce9faa2f15e3de0041/itergraph/a6a2155b8d4f91f6114cebb36efd1d5a/image_region_graph.pert'
    
    #load label prop results file
    self.result = py_eval2.LoadResultOrDie(self.result_uri)
    #self.irg = LoadImageRegionGraph(self.irg_uri)
    self.irg_reader = ImageRegionGraphReader(self.irg_uri)
    self.tide = tide.TideDataset(self.tide_uri)
    #CHECK(ok)
    
    # Validate format is correct
    cm = self.result.confusion_matrix
    CHECK_EQ(len(cm.mean), cm.rows*cm.cols)
    
    self.num_labels = len(self.result.label_names)
    self.image_loader = None
    self.application = tornado.web.Application([
      (r"/", MainHandler, dict(app=self)),
      (r"/confusionmatrix/", ConfusionMatrixHandler, dict(app=self)),
      (r"/confusionmatrixcell", ConfusionMatrixCellHandler, dict(app=self)),              
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

  def RowSum(self, row_index):
    mean = self.result.confusion_matrix.mean    
    sum = 0
    for col_index in range(self.num_labels):
      sum += mean[row_index*self.num_labels + col_index]      
    return sum
  
  def ColSum(self, col_index):
    mean = self.result.confusion_matrix.mean    
    sum = 0
    for row_index in range(self.num_labels):
      sum += mean[row_index*self.num_labels + col_index]      
    return sum
    

if __name__ == "__main__":      
  app = ViewerApp()
  app.Run()
 