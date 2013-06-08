#!/usr/bin/python

""" Tests inria BOW implementation."""

import os
from iw import iw_pb2
from iw.matching.cbir.bow import test_util
from iw.matching.cbir.bow import bow_pb2
from iw.matching.cbir.bow.ctis import py_ctis

def main():
  data_path = os.path.abspath(os.path.curdir)
  params = bow_pb2.BowCbirParams()
  params.visual_vocabulary_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/clust_flickr60_k100000.pert'
  params.num_candidates = 20
  index = py_ctis.CtisIndex()
  visual_vocab_size = 100000
  num_docs = 100
  index.StartCreate(visual_vocab_size, num_docs)
  tester = test_util.BowCbirTester(data_path, params, index, force_clean = False)
  tester.Run()
  return 0

if __name__ == "__main__":
  main()
  
  
