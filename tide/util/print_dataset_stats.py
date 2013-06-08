#!/usr/bin/env python


from snap.pert import py_pert
from iw import iw_pb2
from tide import tide_pb2
import numpy
import math

def Area(bb):
  dx = bb.x2-bb.x1
  dy = bb.y2-bb.y1
  return dx*dy

def ComputeObjectAreaStats(obj):
  areas = []
  for photo in obj.photos:
    for region in photo.regions:
      areas.append(Area(region))
  mean = numpy.mean(areas)
  std = numpy.std(areas)
  return mean, std


def main():
  dataset_root = '/home/ubuntu/Desktop/vol-0449ca74/itergraph/'
  dataset_name = 'tide_v08'
  filename = 'local://%s/%s/objectid_to_object.pert' % (dataset_root, dataset_name)
  
  reader = py_pert.StringTableReader()
  reader.Open(filename)
  
  obj = tide_pb2.Object()
  total = 0
  
  items = []
  for k, v in reader:
    obj.ParseFromString(v)
    #print obj.name
    #print obj.purity
    #print len(obj.photos)
    
    area_mean, area_std = ComputeObjectAreaStats(obj)
    items.append((obj.name, area_mean))
    total += len(obj.photos)
  
  items.sort(key = lambda i: i[1])
  for item in items:
    #print item[0], math.sqrt(item[1])
    print '%0.3f %s' % (item[1]/(640*480), item[0])
    #print "key %s obj %s" % (k, obj)
  
  print 'total: %d' % total

if __name__ == "__main__":
   main()



