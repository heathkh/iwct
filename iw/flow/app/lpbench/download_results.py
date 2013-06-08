#!/usr/bin/python

from snap.pyglog import *
from iw.flow.app.lpbench import resultdb
from iw.matching.cbir import cbir_pb2
from iw.flow.app.itergraph import itergraph_pb2
from iw.flow.app.lpbench import lpbench_pb2
from snap.pert import py_pert
from iw import util as iwutil
def main():      

  db = resultdb.ResultDatabase()  
  writer = py_pert.ProtoTableWriter()
  
  result = lpbench_pb2.ConfigurationResult()
  cache_uri = 'local://%s/lpbench_data.pert' % (os.path.abspath(os.path.dirname(__file__)))
  print cache_uri 
  writer.Open(result, cache_uri, 1)
  results = db.GetResults()
  progress = iwutil.MakeProgressBar(len(results))
  print 'num results: %d' % (len(results))
  
  for i, result in enumerate(results):
    writer.Add(iwutil.Uint64ToKey(i), result.SerializeToString())
    progress.update(i)
  writer.Close()
    
  return 0

if __name__ == "__main__":        
  main()
    
    
    
    
    
  
