#!/usr/bin/python

import json
import numpy as np
import tornado.ioloop
import tornado.web
from snap.pyglog import *
from iw import visutil
from tide import tide




class ImageHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app      
      return    
    
    def get(self):      
      # parse arguments
      image_id = self.get_argument('id', default=None)
      
      if image_id:
        #jpeg = self.app.GetImageLoader().GetImage(int(image_id))
        #self.set_header("Content-Type", "image/jpeg")
        #self.write(jpeg.data)
        svg = self.RenderSvg(int(image_id))
        self.write(svg)
      else:
        params = { 'tide_dataset' : self.app.tide_dataset} 
        self.render('tide_index.html', **params)      
      return
    
    def RenderSvg(self, image_id):
      jpeg = self.app.GetImageLoader().GetImage(image_id)
      bounding_boxes = self.app.tide_dataset.GetBoundingBoxes(image_id)
      
      max_width = 600.0
      max_height = 600.0
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
      
      for bb in bounding_boxes:
        print bb.x1
        print bb.y1
        print bb.x2
        print bb.y2
        svg += '<rect x="%f" y="%f" width="%f" height="%f" stroke="green" stroke-width="4" fill="none"/>\n' % (bb.x1, bb.y1, bb.x2-bb.x1, bb.y2-bb.y1)
      svg += '</g>'
      svg += '</svg>'
      return svg


class ViewerApp:
  def __init__(self):
    
    base_uri = 'local:///home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v18_mixed'
    
    self.images_uri = '%s/photoid_to_image.pert' % (base_uri)
    self.tide_uri = '%s/objectid_to_object.pert' % (base_uri)

    self.tide_dataset = tide.TideDataset(self.tide_uri)
    self.image_loader = None
    self.application = tornado.web.Application([
      (r"/", ImageHandler, dict(app=self)),              
      ], debug=True)    
    return
  
  def GetImageLoader(self):
    if self.image_loader == None:
      LOG(INFO, 'loading images...')
      self.image_loader = visutil.BatchImageLoader(self.images_uri)
    return self.image_loader 
  
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

  

if __name__ == "__main__":      
  app = ViewerApp()
  app.Run()
 