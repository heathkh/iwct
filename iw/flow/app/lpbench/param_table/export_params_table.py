#!/usr/bin/python

from snap.pyglog import *
from iw.flow.app.lpbench import lpbench_pb2
from iw.flow.app.itergraph import itergraph_pb2
from iw.flow.app.lpbench import resultdb
from iw.eval.labelprop.eval1 import py_eval1
from iw.eval.labelprop.eval2 import py_eval2
from protobuf_to_dict import protobuf_to_dict
import json
from iw import visutil
from snap.pert import py_pert
from iw import util as iwutil
from jinja2 import Template
from jinja2 import Environment, PackageLoader
import math
from collections import Mapping, Set, Sequence
# dual python 2/3 compatability, inspired by the "six" library
string_types = (str, unicode) if str is bytes else (str, bytes)
iteritems = lambda mapping: getattr(mapping, 'iteritems', mapping.items)()


def objwalk(obj, path=(), memo=None):
    if memo is None:
        memo = set()
    iterator = None
    if isinstance(obj, Mapping):
        iterator = iteritems
    elif isinstance(obj, (Sequence, Set)) and not isinstance(obj, string_types):
        iterator = enumerate
    if iterator: 
        if id(obj) not in memo:
            memo.add(id(obj))
            for path_component, value in iterator(obj):
                for result in objwalk(value, path + (path_component,), memo):
                    yield result
            memo.remove(id(obj))
    else:       
        yield path, obj
        

def FlattenProtoProperties(proto):
  """ Converts a list of protobuffers into a flattened list of property path and unique value sets """    
  properties = {}
  for path_tuple, val in objwalk(protobuf_to_dict(proto, use_enum_labels=True)):
    path = ''
    for element in path_tuple:
      if path:
        path += '.'
      path += str(element)
    properties[path] = val
  return properties


def AddToTree(tokens, value, parent_node):
  num_tokens = len(tokens)
  #print tokens
  if num_tokens == 0:    
    parent_node.append(value)
  elif num_tokens > 0:
    cur_token = tokens.pop(0)
    if cur_token not in parent_node:
      if num_tokens == 1:
        parent_node[cur_token] = []
      else:      
        parent_node[cur_token] = {}
    new_parent = parent_node[cur_token]
    AddToTree(tokens, value, new_parent)
  return  


class ParamTable(object):
  class Row(object):
    def __init__(self, depth, name, description, column_data):
      self.depth = depth
      self.name = name
      self.description = description
      self.column_data = column_data
      return
  
  def __init__(self, param_data_uri):
    self.num_configs = None
    self.rows = []
    self.configs_per_page = 10
    self.num_pages = None
    
    self.exclude_rows = ['corresondence_filter', 'type','dataset_name', 'visual_vocabulary_uri', 'max_image_replication_factor', 'max_match_batch_size', 'max_vertex_degree', 'dataset name']
    reader =  py_pert.StringTableReader()
    CHECK(reader.Open(param_data_uri))
    configs = []
    progress = iwutil.MakeProgressBar(reader.Entries())
    result = lpbench_pb2.ConfigurationResult()
    for i, (k,v) in enumerate(reader):
      result.ParseFromString(v)
      config = itergraph_pb2.IterGraphParams()
      config.CopyFrom(result.config.iter_graph)
      configs.append(config)
      progress.update(i) 
      #if i > 0:
      #  break    
    
    self.__BuildTable(configs)
    return
  
  def __BuildTable(self, configs):
    self.num_configs = len(configs)                
    self.num_pages = int(math.ceil(float(self.num_configs)/self.configs_per_page))
    print 'num configs: %d' % (self.num_configs)
    print 'num pages: %d' % (self.num_pages)    
    tree = {}
    for config_index, config in enumerate(configs):
      flat_prop = FlattenProtoProperties(config)
      for key, value in flat_prop.iteritems():
        tokens = key.split('.')
        t = (config_index, value)
        AddToTree(tokens, t, tree)    
    self.__GenerateRows(tree)
    return

  def __GenerateRows(self, node, depth = 0, parent_name=None):
    has_children = isinstance(node, dict)
    if parent_name:
      name = parent_name
      
      tweaks = [('max_correspondence_scale_deviation', 'max_scale_deviation'), ('num_neighbors_per_index_shard', 'neighbors_per_shard')]
      for tweak in tweaks:            
        name = name.replace(tweak[0],tweak[1])
      
      row_items = ['-']*self.num_configs        
      if not has_children:
        sparse_row_items = node
        for col, val in sparse_row_items:
          scrubbed_val = 'None'
          
          if name == 'desired_density':
            val = int(val)
          
          if isinstance(val, (float)):
            scrubbed_val = '%.*f' % (2, val)
          else:
            scrubbed_val = str(val)
          
          tweaks = [('FLANN_INDEX_', ''), ('PHASE_TYPE_', ''), ('None', '-'), ('_', ' ')]
          
          for tweak in tweaks:            
            scrubbed_val = scrubbed_val.replace(tweak[0],tweak[1])
          
          scrubbed_val = scrubbed_val.lower()
          #print scrubbed_val
          row_items[col] = scrubbed_val         
      
      
      if name in self.exclude_rows:
        return
              
      description = 'desc here'  
      self.rows.append( ParamTable.Row(depth, name, description, row_items) )
    if has_children:
      it = iter(sorted(node.iteritems()))
      for parent_name, child in it:
        self.__GenerateRows(child, depth+1, parent_name)
    return
  
  def configs_on_page(self, page_index):
    start = self.configs_per_page*page_index 
    end = min(start + self.configs_per_page, self.num_configs)
    return range(start,end)
    
    
  def RenderTex(self):
    env = Environment(loader=PackageLoader('iw.flow.app.lpbench.param_table', '.'))
    template = env.get_template('table_frag_template.txt')
    CHECK(template)
    params = {'table' : self }    
    return template.render(params)


def main():      
  param_data_uri = 'local://%s/../lpbench_data.pert' % (os.path.abspath(os.path.dirname(__file__)))
  table = ParamTable(param_data_uri)
  table_tex_frag = table.RenderTex()  
  open('appendix_pipeline_params_table.tex','w').write(table_tex_frag)
  return 0

if __name__ == "__main__":        
  main()
    
    
    
    
    
  
