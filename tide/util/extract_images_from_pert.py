#!/usr/bin/env python

from pert import py_pert
from iw import iw_pb2
from tide import tide_pb2
import glob
import os
from pyglog import *
from ext.google.strings import py_strings

def main():
  
  images_pert_uril = 'local:///media/ebs/4a4b34/tide_v13/photoid_to_image.pert'
  images_to_extract = [2071492, 2087400, 2112291, 2102113, 2080088, 2083122, 2107730]
  
  reader = py_pert.StringTableReader()
  CHECK(reader.Open(images_pert_uril))
    
  for image_id in images_to_extract:
    ok, data = reader.Find(py_strings.Uint64ToKey(image_id))
    CHECK(ok)
    jpeg_image = iw_pb2.JpegImage()
    jpeg_image.ParseFromString(data)
    filename = '%d.jpg' % (image_id)
    f = open(filename, 'wb')
    f.write(jpeg_image.data)    
  
  return
  
  

if __name__ == "__main__":
   main()



