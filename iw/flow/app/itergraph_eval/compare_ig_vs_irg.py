#!/usr/bin/python

from snap.pyglog import *
from iw.eval.labelprop.eval2 import eval2_pb2
from iw.eval.labelprop.eval1 import eval1_pb2
from snap.pert import py_pert



def ComputeFScore(precision, recall, beta = 1.0):
  eps = 1e-16
  f = (1.0+beta*beta)*precision*recall/(beta*beta*precision + recall + eps)
  return f




def ReadPrecisionRecallEval2(base_uri):
  reader = py_pert.StringTableReader()
  CHECK(reader.Open(base_uri ))
  CHECK_EQ(reader.Entries(), 1)
  result = eval2_pb2.Result()
  for k,v in reader:
    result.ParseFromString(v)
  return result.precision.mean, result.recall.mean    
  
def ReadPrecisionRecallEval1(base_uri):
  reader = py_pert.StringTableReader()
  CHECK(reader.Open(base_uri))
  CHECK_EQ(reader.Entries(), 1)
  result = eval1_pb2.Result()
  for k,v in reader:
    result.ParseFromString(v)
  CHECK(result.IsInitialized())
  return result.phases[-1].precision.mean, result.phases[-1].recall.mean    

def main():      
  #base_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v14_mixed_v2/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/e4baab98c10a434d90d092c71ecb566c/'
  base_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v16_mixed/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/e4baab98c10a434d90d092c71ecb566c/'
  ig_p, ig_r = ReadPrecisionRecallEval1(base_uri + '/labelprop_eval1.pert')  
  ig_f = eval.ComputeFScore(ig_p, ig_r)
  irg_p, irg_r = ReadPrecisionRecallEval2(base_uri + '/labelprop_eval2.pert')
  irg_f = eval.ComputeFScore(irg_p, irg_r)
  print ' ig: precision %f recall: %f  f-score: %f' % (ig_p, ig_r, ig_f)
  print 'irg: precision %f recall: %f  f-score: %f' % (irg_p, irg_r, irg_f)
  return 0

if __name__ == "__main__":        
  main()
    
    
    
    
    
  
