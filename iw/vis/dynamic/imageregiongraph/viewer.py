#!/usr/bin/python

import tornado.ioloop
import tornado.web
from iw.vis.imageregiongraph import graph
from iw.vis.imageregiongraph import renderers
from pert import py_pert
from snap.google.base import py_base
import base64
from iw import iw_pb2
from iw import visutil
import json
import string
import time


    
class MainHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app      
      return            
    
    def get(self):      
      initial_vertex_id = 953911 
      params = {'app' : self.app,
                'initial_vertex_id' : initial_vertex_id }
      self.render('main.html', **params)
      return    


class GraphHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app      
      return            
    
    def get(self):
      vertex_id = long(self.get_argument('vertex_id'))
      num_hops = long(self.get_argument('num_hops', 3))      
      max_num_neighbors = long(self.get_argument('max_num_neighbors', 100))
      size_pixels = 600
      html = self.app.graph_render.RenderHtml(vertex_id, num_hops, max_num_neighbors, size_pixels)      
      self.write(html)
      return


class ImageHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app
      return    
    
    def get(self):          
      image_id = long(self.get_argument('id'))
      jpeg = self.app.image_loader.GetImage(image_id)
      self.set_header("Content-Type", "image/jpeg")
      self.write(jpeg.data)
      return


class ViewerApp:
  def __init__(self):    
    dataset_name = 'tide_v08_distractors'
    base_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/%s' % dataset_name
    self.images_uri = '%s/cropped_scaled_photoid_to_image.pert' % (base_uri)
    self.matches_uri = '%s/usift/cbir/9e6c60d825c5a9814e19c0a735747011/itergraph/0ffd08c74629993310fd73f6461673af/merged_matches.pert' % (base_uri)
    self.irg_uri = '%s/usift/cbir/9e6c60d825c5a9814e19c0a735747011/itergraph/0ffd08c74629993310fd73f6461673af/image_region_graph.pert' % (base_uri)
    self.tide_uri = '%s/objectid_to_object.pert' % (base_uri)
    
    self.graph = graph.GetCachedGraph(self.irg_uri, self.tide_uri)
    self.image_loader = visutil.BatchImageLoader(self.images_uri)
    self.graph_render = renderers.GraphNeighborhoodRenderer(self.graph)
    print 'done loading'
    
    self.application = tornado.web.Application([
        (r"/", MainHandler, dict(app=self)),
        (r"/graph", GraphHandler, dict(app=self)),
        #(r"/vertex", VertexHandler, dict(app=self)),
        #(r"/edge", EdgeHandler, dict(app=self)),
        (r"/image", ImageHandler, dict(app=self)),
      ], debug=True)
    
    return
  
  def run(self):
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
  app.run()  
  
