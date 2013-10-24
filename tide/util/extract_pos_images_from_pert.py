#!/usr/bin/env python

from pert import py_pert
from iw import iw_pb2
from tide import tide_pb2
from tide import tide
from snap.google.base import py_base
import glob
import os
from pyglog import *
#from snap.google.base import py_strings

def main():
  
  base_uri = 'local://home/ubuntu/Desktop/datasets/tide_v12/'  
  tide_uri = '%s/objectid_to_object.pert' % (base_uri)
                     
  dataset = tide.TideDataset(tide_uri)
  
  print tide
  
  pos_imageids = []
  
  imageid_to_objectname = {}
  
  for id, obj in dataset.objectid_to_object.iteritems():
    print obj.name
    pos_imageids.extend(obj.pos_image_ids)
    for image_id in obj.pos_image_ids:
      imageid_to_objectname[image_id] = obj.name

  # sort for efficient access to pert    
  pos_imageids.sort()
  
  images_pert_uri = '%s/photoid_to_image.pert' % (base_uri)
  
  reader = py_pert.StringTableReader()
  CHECK(reader.Open(images_pert_uri))
    
  for image_id in pos_imageids:
    ok, data = reader.Find(py_base.Uint64ToKey(image_id))
    CHECK(ok)
    jpeg_image = iw_pb2.JpegImage()
    jpeg_image.ParseFromString(data)
    objectname = imageid_to_objectname[image_id]
    dirname = './extracted/%s' % (objectname)    
    filename = '%s/%d.jpg' % (dirname, image_id)
    
    if not os.path.exists(dirname):
      os.makedirs(dirname)
      
    f = open(filename, 'wb')
    f.write(jpeg_image.data)    
  
  return
  
  

if __name__ == "__main__":
   main()



