#!/usr/bin/python

from snap.pyglog import *
from iw.eval.labelprop.eval2 import eval2_pb2
from iw.eval.labelprop.eval1 import eval1_pb2
from snap.pert import py_pert
from iw import util as iwutil
from jinja2 import Template
from jinja2 import Environment, PackageLoader
import math
import numpy

beta = 1.0




def ComputeFScore(precision, recall, beta = 1.0):
  eps = 1e-16
  f = (1.0+beta*beta)*precision*recall/(beta*beta*precision + recall + eps)
  return f




class Table(object):
  class Row(object):
    def __init__(self, name, precision, recall):
      self.name = name
      self.precision = precision
      self.recall = recall
      self.f_score = eval.ComputeFScore(self.precision, self.recall, beta)
      return
  
  def __init__(self, param_data_uri):
    self.rows = []
    self.rigid_objects = ['starbucks_logo', 'prius', 'nasa_spaceshuttle', 'starwars_r2d2', 'kfcsanders_logo', 'british_telephone_booth', 'csx_locomotive', 'thinker', 'kindle', 'superman', 'vw_bug', 'parking_meter', 'violin']
    self.nonrigid_objects = ['monarch_butterfly','peacock','pineapple','giraffe','mallard_duck','ladybug','pug','bull_terrier','elephant','artichoke']
    
    reader = py_pert.StringTableReader()
    CHECK(reader.Open(param_data_uri))
    CHECK_EQ(reader.Entries(), 1)
    result = eval2_pb2.Result()    
    for k,v in reader:
      result.ParseFromString(v)      
    
    for i in range(len(result.label_names)):
      name = result.label_names[i]
      if name not in self.rigid_objects:
        continue
      CHECK(name in self.rigid_objects or name in self.nonrigid_objects) 
      self.rows.append(self.Row(result.label_names[i], result.object_precision.mean[i], result.object_recall.mean[i]))
      
    self.rows.sort(key = lambda r: r.f_score, reverse=True)
    
    return
  
  
  def MeanFScore(self):
    v = [row.f_score for row in self.rows]    
    return numpy.mean(v)
  
  def MeanPrecision(self):
    v = [row.precision for row in self.rows]    
    return numpy.mean(v)
  
  def MeanRecall(self):
    v = [row.recall for row in self.rows]    
    return numpy.mean(v)
      
    
  def RenderTex(self):
    env = Environment(loader=PackageLoader('iw.eval.labelprop.eval2.tex_table', '.'))
    template = env.get_template('table_frag_template.txt')
    CHECK(template)
    params = {'table' : self }
    return template.render(params)


def main():      
  data_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/tide_v08_distractors/usift/cbir/9e6c60d825c5a9814e19c0a735747011/itergraph/0ffd08c74629993310fd73f6461673af/labelprop_eval2.pert'
  table = Table(data_uri)
  print 'MeanFScore: %f' % (table.MeanFScore())
  #print 'MeanFScore: %f' % (eval.ComputeFScore(0.475882, 0.486570, beta))
  open('eval2_object_results_table.tex','w').write(table.RenderTex())
  return 0

if __name__ == "__main__":        
  main()
    
    
    
    
    
  
