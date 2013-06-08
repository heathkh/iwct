#!/usr/bin/python
""" Image Web python eval utility functions."""

from snap.pyglog import *
from snap.pert import py_pert 
from iw import iw_pb2
from iw.matching.cbir import cbir_pb2
from iw import util as iwutil
#from tide import tide_pb2
import tide_pb2
import numpy as np
import cPickle as pickle
from collections import defaultdict

class BoundingBox(object):
  def __init__(self, region):
    self.x1 = region.x1
    self.y1 = region.y1
    self.x2 = region.x2
    self.y2 = region.y2
    
    CHECK_LE(self.x1, self.x2)
    CHECK_LE(self.y1, self.y2)
    return


class TideObject(object):
  def __init__(self):
    self.id = None
    self.name = None
    self.pos_image_ids = []
    self.neg_image_ids = []
    self.none_image_ids = []    
    self.posimageid_to_boundingboxes = defaultdict(list)
    return
   
  def GetImageIds(self):
    ids = []
    ids.extend(self.pos_image_ids)
    ids.extend(self.neg_image_ids)
    ids.extend(self.none_image_ids)
    return ids
    
  def LoadFromProto(self, id, tide_object_proto):
    self.id = id
    self.name = tide_object_proto.name
    for photo in tide_object_proto.photos:
      if photo.label == tide_pb2.POSITIVE:
        self.pos_image_ids.append(photo.id)
        for region in photo.regions:
          self.posimageid_to_boundingboxes[photo.id].append(BoundingBox(region))
      elif photo.label == tide_pb2.NEGATIVE:
        self.neg_image_ids.append(photo.id)
      elif photo.label == tide_pb2.NONE:
        self.none_image_ids.append(photo.id)
      else:
        LOG(FATAL, 'unkown label type %s' % (photo.label))      
    return
    

class TideLabel(object):
  def __init__(self, object_id, label):
    self.object_id = object_id
    self.label = label
    return 

def GetCachedTideDataset(tide_uri):
  ok, scheme, path, error = py_pert.ParseUri(tide_uri)    
  # check if we pickled the object previously
  hash_str = (tide_uri + str(os.path.getmtime(path)) + str(os.path.getsize(path)))
  hash_val = hash(hash_str)
  cache_path = './tide.%s.pickle' % (hash_val)
  
  tide_dataset = None
  if os.path.exists(cache_path):
    f = open(cache_path, 'r')
    tide_dataset = pickle.load(f)
  else:
    tide_dataset = TideDataset(tide_uri)
    f = open(cache_path, 'w')
    pickle.dump(tide_dataset, f, protocol=2)            
  return tide_dataset

class TideDataset(object):
  def __init__(self, tide_uri):
    self.objectid_to_object = {}
    self.imageid_to_label = {}
    LOG(INFO, 'starting to load tide dataset...')
    # load list of images that belong to each tide object    
    tide_reader = py_pert.StringTableReader()
    tide_reader.Open(tide_uri)
    for index, (k, v) in enumerate(tide_reader):                                  
      tide_object = tide_pb2.Object()
      tide_object.ParseFromString(v)
      CHECK(tide_object.IsInitialized())
      obj = TideObject()
      obj.LoadFromProto(index, tide_object) 
      self.objectid_to_object[obj.id] = obj

    for obj in self.objectid_to_object.itervalues():       
      for image_id in obj.pos_image_ids:
        self.imageid_to_label[image_id] = TideLabel(obj.id, 'pos');
      for image_id in obj.neg_image_ids:
        self.imageid_to_label[image_id] = TideLabel(obj.id, 'neg');
      for image_id in obj.none_image_ids:
        self.imageid_to_label[image_id] = TideLabel(obj.id, 'none');
        
    LOG(INFO, 'done loading tide dataset...')    
    return
  
  def GetLabel(self, image_id):
    label = None
    if image_id in self.imageid_to_label:
      label = self.imageid_to_label[image_id] 
    return label
  
  
  def GetBoundingBoxes(self, image_id):
    bb = []
    label = self.GetLabel(image_id)
    if label:    
      bb = self.objectid_to_object[label.object_id].posimageid_to_boundingboxes[image_id]        
    return bb
  
  def NumPosImages(self, object_id):
    return len(self.objectid_to_object[object_id].pos_image_ids)
  
  def __str__(self):
    msg = '<'
    for o in self.objectid_to_object.itervalues():
      msg += '%d - %s (%d), ' % (o.id, o.name, o.size)
    msg += '>'  
    return msg
  
  
  
def ComputeCbirPerformanceStats(tide_uri, query_results_uri):
  tide = TideDataset(tide_uri)
  reader = py_pert.StringTableShardSetReader()
  CHECK(reader.Open(query_results_uri))
  results = cbir_pb2.QueryResults()
  imageid_to_ap = {}
  progress = iwutil.MakeProgressBar(reader.Entries())
  for i, (k, v) in enumerate(reader):
    progress.update(i)
    query_image_id = iwutil.KeyToUint64(k)      
    query_label = tide.GetLabel(query_image_id)
    if query_label == None or query_label.object_id == None:
      LOG(FATAL, 'unknown class for image: %d' % (query_image_id))
    query_object_id = query_label.object_id
    results.ParseFromString(v)
    query_object_num_pos = tide.NumPosImages(query_object_id)
    prev_score = float("inf")
    sum_precision = 0.0
    num_correct = 0.0
    rank = 0.0
    non_junk_results = []
    num_junk = 0
    for result in results.entries:
      label = tide.GetLabel(result.image_id)
      if label and label.object_id == query_object_id and (label.label == 'neg' or label.label == 'none'):
        num_junk += 1
      else:     
        non_junk_results.append(result)
    
    for result in non_junk_results:
      rank += 1
      CHECK_LE(result.score, prev_score)  ## check invariant: ordered decreasing by score
      prev_score = result.score
      result_label = tide.GetLabel(result.image_id)
      if not result_label or result_label.object_id == None:
        LOG(FATAL, 'unknown class: %d' % (result.image_id))
              
      if result_label.object_id == query_object_id:
        num_correct += 1.0
        precision = num_correct/rank
        sum_precision += precision
    average_precision = sum_precision/query_object_num_pos
    imageid_to_ap[query_image_id] = average_precision
  
    
  # compute mean average precision (across each object classes and across all classes)
  values = [v for v in imageid_to_ap.itervalues()]
  mean_average_precision = np.mean(values)
  
  object_mean_average_precision = {}
  for tide_object in tide.objectid_to_object.itervalues():    

    object_mean_average_precision[tide_object.name] = np.mean( [imageid_to_ap[i] for i in tide_object.GetImageIds()] )
      
  return mean_average_precision, object_mean_average_precision


class ImageSequenceReader(object):
  """ Loads sequence of distractor images. """
  def __init__(self,  images_uri):
    # open images table    
    LOG(INFO, 'opening image uri: %s' % (images_uri))
    self.image_reader = py_pert.StringTableReader()
    CHECK(self.image_reader.Open(images_uri))
    return
  
  def GetNextImage(self):
    ok, key, value = self.image_reader.Next()
    CHECK(ok)
    image_id = iwutil.KeyToUint64(key)
    jpeg = iw_pb2.JpegImage()
    jpeg.ParseFromString(value)
    CHECK(jpeg.IsInitialized())
    return image_id, jpeg
  
  def GetNumImages(self):    
    return self.image_reader.Entries()
  
  

#def ComputeMatchCandidateQuality(tide_uri, match_candidates_uri):
#  """ Compute the performance metric (ratio candidate edges in correct class, beta -- the improvement relative to random guessing). """
#  imageid_to_class, object_names = CreateImageIdToClassDict(tide_uri)
#  reader = py_pert.StringTableShardSetReader()  
#  CHECK(reader.Open(match_candidates_uri))
#  progress = iwutils.MakeProgressBar(reader.Entries())
#  num_objects = len(object_names)
#  num_correct_edges = 0
#  num_incorrect_edges = 0
#  scalar = iw_pb2.Scalar()
#  for i, (k, v) in enumerate(reader):
#    scalar.ParseFromString(v)
#    
#    if scalar.value < 4:
#      continue
#    
#    image_a_id, image_b_id = ParseUint64KeyPair(k)
#    if image_a_id in imageid_to_class and image_b_id in imageid_to_class:
#      object_name_a =  imageid_to_class[image_a_id]
#      object_name_b =  imageid_to_class[image_b_id]
#      if object_name_a == object_name_b:
#        num_correct_edges += 1
#      else:
#        num_incorrect_edges += 1
#    progress.update(i)
#    
#  ratio_correct = num_correct_edges / float(num_correct_edges + num_incorrect_edges + 1e-6)
#  beta = ratio_correct / (1.0/num_objects)      
#  return num_correct_edges, ratio_correct, beta
#  