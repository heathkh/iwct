#!/usr/bin/python

import os
from deluge.core import Flow
from deluge.core import PertResource
from deluge.core import FlowScheduler
from pert import py_pert 


class ExtractFeaturesFlow(Flow):  
  def __init__(self, images, feature_type, resource_prefix):
    super(ExtractFeaturesFlow,self).__init__()
    self.AddInput('images', images)    
    self.feature_type = feature_type    
    features_resource = PertResource(self, "%s/features.txt" % resource_prefix)
    self.AddOutput('features', features_resource )
    return
  
  def Run(self):
    uri = self.outputs['features'].GetUri()    
    writer = py_pert.StringTableWriter()
    writer.Open(uri, py_pert.WriterOptions("memcmp"))    
    return 
    

class GenerateVisualVocabFlow(Flow):
  def __init__(self, features, vv_size, resource_prefix):
    super(GenerateVisualVocabFlow,self).__init__()
    self.AddInput('features', features)        
    self.AddOutput('vv', PertResource(self, "%s/vv.txt" % resource_prefix) )
    self.vv_size = vv_size
    return
  
  def Run(self):
    uri = self.outputs['vv'].GetUri()    
    writer = py_pert.StringTableWriter()
    writer.Open(uri, py_pert.WriterOptions("memcmp"))    
    return
  
  
class GenerateBagOfWordsFlow(Flow):
  def __init__(self, features, vv, resource_prefix):
    super(GenerateBagOfWordsFlow,self).__init__()
    self.AddInput('features', features)
    self.AddInput('vv', vv)                
    self.AddOutput('bow', PertResource(self, "%s/bow.txt" % resource_prefix) )    
    return
  
  def Run(self):
    uri = self.outputs['bow'].GetUri()    
    writer = py_pert.StringTableWriter()
    writer.Open(uri, py_pert.WriterOptions("memcmp"))    
    return


class GenerateDebugMatchGraphFlow(Flow):      
  def __init__(self, features, vv, resource_prefix):
    super(GenerateDebugMatchGraphFlow,self).__init__()    
    self.AddInput('features', features)
    self.AddInput('vv', vv)                
    self.AddOutput('mg', PertResource(self, "%s/mg.txt" % resource_prefix))
    
    self.bow_flow = GenerateBagOfWordsFlow(features, vv, resource_prefix)
    
    self.AddInput('bow', self.bow_flow.GetOutput())
    
    return
  
  def Run(self):
    uri = self.outputs['mg'].GetUri()    
    writer = py_pert.StringTableWriter()
    writer.Open(uri, py_pert.WriterOptions("memcmp"))    
    return        



class MyTestFlow(Flow):
  def __init__(self, root_uri):
    Flow.__init__(self)
    
    feature_types = ['usift']
    
    vv_sizes = [1000]
    images = PertResource(self, '%s/images.txt' % (root_uri))    
    assert(images.Exists())
    
    for feature_type in feature_types:
      resource_uri_prefix = "%s/%s/" % (root_uri, feature_type)
      feature_flow = ExtractFeaturesFlow(images, feature_type, resource_uri_prefix)
      for vv_size in vv_sizes:
        resource_uri_prefix = "%s/%s/%d" % (root_uri, feature_type, vv_size)        
        visualvocab_flow = GenerateVisualVocabFlow(feature_flow.GetOutput(), vv_size, resource_uri_prefix)
        matchgraph_flow = GenerateDebugMatchGraphFlow(feature_flow.GetOutput(), visualvocab_flow.GetOutput(), resource_uri_prefix)
  

def main():
  
  #root_uri = 'local://home/ubuntu/Desktop/test'
  root_uri = 'maprfs://test'
  test_flow = MyTestFlow(root_uri)
    
  scheduler = FlowScheduler(test_flow)
  scheduler.RunDebug()  
    
  return 0    

if __name__ == "__main__":        
  main()
    
    
    
    
    
  
