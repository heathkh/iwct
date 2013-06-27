#!/usr/bin/python

import tornado.ioloop
import tornado.web
from snap.pyglog import *
from snap.google.base import py_base
from iw import iw_pb2
import json
import string
from snap.deluge import provenance
from protobuf_to_dict import protobuf_to_dict
    
class MainHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app
      return            
    
    def get(self):      
      params = {'app' : self.app}
      self.render('main.html', **params)
      return    

class ResourceHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app      
      return            
    
    def get(self):
      fingerprint = self.get_argument('fingerprint')
      CHECK_EQ(len(fingerprint), 32, 'invalid fingerprint: %s' % fingerprint)      
      provenance_list = provenance.BuildResourceProvenanceList(fingerprint)
      provenance_list.sort(key = lambda p : p.start_time_sec)
      
      total_job_run_time_sec = 0
      prev_end_time_sec = 0
      items = []
      for p in provenance_list:
        CHECK_GE(p.start_time_sec,  prev_end_time_sec) # our method of adding run times assumes no two jobs overlap... need more complex method otherwise
        prev_end_time_sec = p.end_time_sec        
        CHECK_GE(p.end_time_sec, p.start_time_sec) # check invariant
        job_run_time_sec = p.end_time_sec - p.start_time_sec
        print total_job_run_time_sec
        
        item_data = protobuf_to_dict(p) 
        item_data['squeeze_time_start_sec'] = total_job_run_time_sec
        total_job_run_time_sec += job_run_time_sec
        item_data['squeeze_time_end_sec'] = total_job_run_time_sec
        items.append(item_data)
      
      provenance_json = json.dumps(items)
      print provenance_json
      params = {'provenance_json' : provenance_json
                 }
      
      self.render('resource.html', **params)      
      return


class ViewerApp:
  def __init__(self):   
    print 'done loading'
    
    static_path =  os.path.dirname(os.path.realpath(__file__)) + '/static/'
    print static_path
    self.application = tornado.web.Application([        
        (r'/static/(.*)', tornado.web.StaticFileHandler, {'path': static_path}),
        (r"/resource", ResourceHandler, dict(app=self)),
        (r"/", MainHandler, dict(app=self)),        
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
  
