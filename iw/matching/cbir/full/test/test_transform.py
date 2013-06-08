#!/usr/bin/python
import unittest
from pyglog import *
from iw.matching.cbir import py_cbir
from iw.matching.cbir import cbir_pb2
from iw import iw_pb2

def CreateKeypoint( x, y, radius):
      k = cbir_pb2.Keypoint()
      k.x = x
      k.y = y
      k.radius = radius
      return k

class TestSmoothFilter(unittest.TestCase):
    
    def test_basic(self):
      
      k1a = CreateKeypoint(10, 10, 5)
      k1b = CreateKeypoint(120, 70, 10)
      
      k2a = CreateKeypoint(20, 20, 15)
      k2b = CreateKeypoint(140, 90, 30)
      
      t1 = py_cbir.ScaleTranslateTransform(k1a, k1b)      
      t1.Print()      
      error = t1.ComputeTransferError(k2a, k2b)
      self.assertEqual(error, 0)      
      
      t2 = py_cbir.ScaleTranslateTransform(k2a, k2b)      
      t2.Print()      
      error = t2.ComputeTransferError(k1a, k1b)
      self.assertEqual(error, 0)
      return
    

if __name__ == '__main__':
    unittest.main()
