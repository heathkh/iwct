#!/usr/bin/python

import tornado.ioloop
import tornado.web

from iw.vis.imagegraph import tide_image_graph
from iw.vis.imagegraph import renderers

from pert import py_pert
from pyglog import *
from snap.google.base import py_base
import base64
from iw import iw_pb2
from iw import visutil
import json
import string

    
class MainHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app      
      return            
    
    def get(self):      
      initial_image_id = self.app.image_graph.vertexid_to_imageid[0]
      params = {'app' : self.app,
                'initial_image_id' : initial_image_id,
                'objects_dict' : self.app.image_graph.tide.objectid_to_object }
      self.render('main.html', **params)
      return    

class NeighborhoodGraphHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app      
      return            
    
    def get(self):
      image_id = long(self.get_argument('image_id'))
      num_hops = long(self.get_argument('num_hops', 3))      
      max_num_neighbors = long(self.get_argument('max_num_neighbors', 100))
      size_pixels = 600
      neighborhood_graph = self.app.image_graph.GetNeighborhoodOfImage(image_id, num_hops, max_num_neighbors)
      html = self.app.graph_render.RenderHtml(neighborhood_graph, size_pixels, central_image_id = image_id, title = 'neighborhood of %d' % (image_id))      
      self.write(html)
      return
    
    
class ObjectSubgraphHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app      
      return            
    
    def get(self):
      object_id = long(self.get_argument('object_id'))
      size_pixels = 600
      subgraph = self.app.image_graph.GetObjectSubgraph(object_id)
      html = self.app.graph_render.RenderHtml(subgraph, size_pixels,  title = 'object subgraph of %d' % (object_id))      
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


class MatchHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app
      return    
    
    def get(self):          
      image_a_id = long(self.get_argument('image_a'))
      image_b_id = long(self.get_argument('image_b'))
      
      if image_b_id < image_a_id:
        image_a_id, image_b_id = image_b_id, image_a_id 
      
      svg = self.app.match_renderer.RenderSvg(image_a_id, image_b_id)
      self.write(svg)
      
      return


class ViewerApp:
  def __init__(self):    
    
    
    dataset_name = 'tide_v08_distractors'
    base_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/%s' % dataset_name
    self.images_uri = '%s/cropped_scaled_photoid_to_image.pert' % (base_uri)
    self.matches_uri = '%s/usift/cbir/9e6c60d825c5a9814e19c0a735747011/itergraph/0ffd08c74629993310fd73f6461673af/merged_matches.pert' % (base_uri)
    self.image_graph_uri = '%s/usift/cbir/9e6c60d825c5a9814e19c0a735747011/itergraph/0ffd08c74629993310fd73f6461673af/image_graph.pert' % (base_uri)
    self.tide_uri = '%s/objectid_to_object.pert' % (base_uri)
    
    
#    dataset_name = 'tide_v08'
#    base_uri = 'local://media/vol-0449ca74/itergraph/%s' % dataset_name
#    self.images_uri = '%s/cropped_scaled_photoid_to_image.pert' % (base_uri)
#    self.matches_uri = '%s/usift/cbir/d27df409ad95e12823feed0c658eabeb/itergraph/1b8ba7a00a9d1cd558716ce882e8408f/merged_matches.pert' % (base_uri)
#    self.image_graph_uri = '%s/usift/cbir/d27df409ad95e12823feed0c658eabeb/itergraph/1b8ba7a00a9d1cd558716ce882e8408f/image_graph.pert' % (base_uri)
#    self.tide_uri = '%s/objectid_to_object.pert' % (base_uri)

#    dataset_name = 'tide_v14_mixed_v2'
#    base_uri = 'local://media/vol-0449ca74/itergraph/%s' % dataset_name
#    self.images_uri = '%s/photoid_to_image.pert' % (base_uri)
#    self.matches_uri = '%s/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/e4baab98c10a434d90d092c71ecb566c/merged_matches.pert' % (base_uri)
#    self.image_graph_uri = '%s/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/e4baab98c10a434d90d092c71ecb566c/image_graph.pert' % (base_uri)
#    self.tide_uri = '%s/objectid_to_object.pert' % (base_uri)
    
    
    self.image_graph = tide_image_graph.GetCachedTideImageGraph(self.image_graph_uri, self.tide_uri)
    self.image_loader = visutil.BatchImageLoader(self.images_uri)
    self.graph_render = renderers.TideImageGraphRenderer(self.image_graph)
    self.match_renderer = renderers.MatchRenderer(self.matches_uri, self.images_uri)
    
    print 'done loading'
    
    self.application = tornado.web.Application([
        (r"/", MainHandler, dict(app=self)),
        (r"/neighborhood", NeighborhoodGraphHandler, dict(app=self)),
        (r"/objectsubgraph", ObjectSubgraphHandler, dict(app=self)),
        (r"/match", MatchHandler, dict(app=self)),
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
  
