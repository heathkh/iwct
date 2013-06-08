#!/usr/bin/python

from snap.pyglog import *
from iw.flow.app.lpbench import lpbench_pb2
from iw.flow.app.lpbench import resultdb
from iw.eval.labelprop.eval1 import py_eval1
from iw.eval.labelprop.eval2 import py_eval2
from protobuf_to_dict import protobuf_to_dict
import json
from iw import visutil

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
    #elif isinstance(obj, (Sequence, Set)) and not isinstance(obj, string_types):
    #    iterator = enumerate
    if iterator: 
        if id(obj) not in memo:
            memo.add(id(obj))
            for path_component, value in iterator(obj):
                for result in objwalk(value, path + (path_component,), memo):
                    yield result
            memo.remove(id(obj))
    else:       
        yield path, obj
        
def GenerateColorMap(values):
  assert(len(values))
  sorted_values = list(values)
  sorted_values.sort()
  colors = visutil.GenerateDistinctColors(len(values))
  color_map = {}
  for i, value in enumerate(sorted_values):
    color_map[value] = colors[i]    
  return color_map
            

def FlattenProtoProperties(proto):
  """ Converts a list of protobuffers into a flattened list of property path and unique value sets """    
  properties = {}
  for path_tuple, val in objwalk(protobuf_to_dict(proto)):
    path = ''
    for element in path_tuple:
      if path:
        path += '.'
      path += element
          
    properties[path] = val
    
  return properties

def AggregateProtoProperties(protos):
  """ Converts a list of protobuffers into a flattened list of property path and unique value sets """
  property_sets = {}
  for proto in protos: 
    print str(protobuf_to_dict(proto))   
    for path_tuple, val in objwalk(protobuf_to_dict(proto)):
      path = ''
      for element in path_tuple:
        if path:
          path += '.'
        path += str(element)
            
      if path not in property_sets:
        property_sets[path] = set()
      property_sets[path].add(val)
      
  return property_sets

def main():      
  db = resultdb.ResultDatabase()

  results = []      
  for i, result in enumerate(db.GetResults()):
    result.eval1.object_precision.Clear()
    result.eval1.object_recall.Clear()
    result.eval1.confusion_matrix.Clear()
    result.eval1.confusion_matrix_item_freq.Clear()
    result.eval2.Clear()
    results.append(protobuf_to_dict(result))
  f = open('lpbench_results.json', 'w')  
  f.write(json.dumps(results))
  f.close()    


#  configs = [r.config for r in db.GetResults()]
#  property_sets = AggregateProtoProperties(configs)  
#  property_names = []
#  color_maps = {}
#  for property, values in property_sets.iteritems():
#    property_names.append(property)
#    #color_map = GenerateColorMap(values)
#    #color_maps[property] = color_map    
#    print property  
    
    
  
  return 0

if __name__ == "__main__":        
  main()
    
    
    
    
    
  
