#!/usr/bin/python
from snap.pyglog import *
from snap.deluge import s3dict
from iw.flow.app.lpbench import lpbench_pb2

class ResultDatabase(object):
  def __init__(self):
    self.result_db = s3dict.S3ProtoDictionary('lpbench_results',
                                          lpbench_pb2.Configuration(),
                                          lpbench_pb2.ConfigurationResult())    
                                          
    return
  
  def ResultReady(self, config):
    return self.result_db.Contains(config)    
      
  def SetResult(self, config, result):
    self.result_db.Set(config, result)
    return
  
  def GetResults(self):
    results = []
    for k, result in self.result_db.iteritems():
      results.append(result)
    return results
    
  
  
    
    
    
  
