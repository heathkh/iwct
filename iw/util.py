#!/usr/bin/python
""" Image Web python utility functions."""

from snap.pyglog import *
from snap.google.base import py_base
from progressbar import Bar, Percentage, ETA, ProgressBar #, Counter, , Percentage, ProgressBar, SimpleProgress, Timer
from iw.matching import py_featureextractor
from snap.pert import py_pert 
from PIL import Image
import StringIO
import glob
import random
from iw import iw_pb2
import os
import cPickle as pickle

import hashlib


def Uint64ToKey(i):
  return py_base.Uint64ToKey(i)

def KeyToUint64(key):
  return py_base.KeyToUint64(key)

def KeyToDouble(key):
  return py_base.KeyToDouble(key)

def ParseUint64Key(key):
  CHECK_EQ(len(key), 8)
  return py_base.KeyToUint64(key)

def ParseUint64KeyPair(key):
  CHECK_EQ(len(key), 16)
  id_a = py_base.KeyToUint64(key[0:8])
  id_b = py_base.KeyToUint64(key[8:16])
  CHECK_GT(id_a, 0)
  CHECK_GT(id_b, 0)
  return id_a, id_b


def UndirectedEdgeKey(image_index_a, image_index_b):
  CHECK_GT(image_index_a, 0)
  CHECK_GT(image_index_b, 0)
  # ensure a is smaller than b
  if image_index_a > image_index_b:
    image_index_a, image_index_b = image_index_b, image_index_a 
  CHECK_GE(image_index_b, image_index_a)
  key_pair = py_base.Uint64ToKey(image_index_a) + py_base.Uint64ToKey(image_index_b)  
  return key_pair

def MakeProgressBar(num_items):
  widgets = [Percentage(), ' ', Bar(), ' ', ETA()]  
  pbar = ProgressBar(widgets=widgets, maxval=num_items).start()
  return pbar
  
def PackImagesDirectoryToPert(src_path, output_uri, randomize = True, max_num_images = None):  
  CHECK(os.path.isdir(src_path), 'expected dir: %s' % src_path)
  filenames = glob.glob('%s/*.jpg' % src_path)  
  if randomize:
    random.shuffle(filenames) # shuffle so there is no accidental coherence of filename and order in pert... any prefix should be from a random image (otherwise might be fulled from same object type)
  else:
    filenames.sort()

  if max_num_images:
    filenames = filenames[:max_num_images]
    
  writer = py_pert.StringTableWriter()
  CHECK(writer.Open(output_uri, 1))
  progress = MakeProgressBar(len(filenames))
  for i, filename in enumerate(filenames): 
    #print filename   
    data = open(filename).read()    
    #key = py_base.FingerprintString(data)
    #key = py_base.Uint64ToKey(i)
    key = os.path.basename(filename)
    try:
      im = Image.open(StringIO.StringIO(data))
    except:
      LOG(FATAL, 'Error opening %s' % (filename))
      continue      
    width, height = im.size
    jpeg = iw_pb2.JpegImage()
    jpeg.data = data
    jpeg.width = width
    jpeg.height = height
    CHECK(jpeg.IsInitialized())
    writer.Add(key, jpeg.SerializeToString())
    progress.update(i)
      
  writer.Close()  
  return



def ExtractFeatures(feature_extractor_params, images_uri, features_uri):
  extractor = py_featureextractor.CreateFeatureExtractorOrDie(feature_extractor_params)  
  CHECK(extractor) 
  reader = py_pert.StringTableReader()
  CHECK(reader.Open(images_uri), 'can not open file: %s' % (images_uri))
  image = iw_pb2.JpegImage()
  writer = py_pert.ProtoTableWriter()
  features = iw_pb2.ImageFeatures()
  writer.Open(features, features_uri, 1)
  progress = MakeProgressBar(reader.Entries())
  for i, (k, v) in enumerate(reader):    
    image.ParseFromString(v)    
    ok, features = extractor.Run(image.data)    
    if ok:
      writer.Add(k, features.SerializeToString())
    progress.update(i)  
  return

def EnsureParentPathExists(f):
  d = os.path.dirname(f)
  if not os.path.exists(d):
    os.makedirs(d)
  return


def LoadObject(path):
  f = open(path, 'r')
  return pickle.load(f)
    
    
def SaveObject(obj, path):  
  EnsureParentPathExists(path)
  f = open(path, 'w')
  pickle.dump(obj, f, protocol=2)
  return





def HashProto(proto):
  m = hashlib.md5()
  m.update(str(proto))
  uni = m.hexdigest()
  mystr = uni.encode('ascii','ignore')    
  return mystr


def LoadProtoFromUriOrDie(template_proto, uri):
  template_proto.Clear()
  reader = py_pert.StringTableReader()
  CHECK(reader.Open(uri))
  for k, v in reader:
    template_proto.ParseFromString(v)       
    break  
  reader.Close()  
  CHECK(template_proto.IsInitialized(), 'Wrong proto type...' )
  return

def SaveProtoToUriOrDie(proto, uri):
  writer = py_pert.ProtoTableWriter()
  CHECK(writer.Open(proto, uri, 1))
  writer.Add(" ", proto.SerializeToString())
  writer.Close()  
  return


from pert import py_pert
from pyglog import *
from snap.google.base import py_base
from iw import iw_pb2
import base64
import json
import string
import colorsys

def JpegToDataUrl(jpeg_data):
  dataurl = 'data:image/jpeg;base64,' + base64.b64encode(jpeg_data)
  return dataurl


class BatchImageLoader(object):
  """ Efficiently fetches images from a pert file. """
  def __init__(self,  images_uri):
    # open images table    
    LOG(INFO, 'opening image uri: %s' % (images_uri))
    self.image_reader = py_pert.StringTableReader()
    CHECK(self.image_reader.Open(images_uri))
    return
  
  def GetImage(self, image_id):
    ok, value = self.image_reader.Find(py_base.Uint64ToKey(image_id))
    if not ok:
      return None
    jpeg = iw_pb2.JpegImage()
    jpeg.ParseFromString(value)
    CHECK(jpeg.IsInitialized())
    return jpeg
  
      
  def BatchGetImages(self, image_ids):      
    """ Does efficient batch lookup of images returning a dict mapping image_id to raw jpeg data. """
    id_to_jpeg = {}
    image_ids.sort()
    
    for image_id in image_ids:
      ok, value = self.image_reader.Find(py_base.Uint64ToKey(image_id))
      CHECK(ok)
      jpeg = iw_pb2.JpegImage()
      jpeg.ParseFromString(value)
      CHECK(jpeg.IsInitialized())
      id_to_jpeg[image_id] = jpeg
    
    return id_to_jpeg 
  
  
  def BatchGetImagesAsDataUri(self, image_ids):      
    """ Does efficient batch lookup of images returning a dict mapping image_id to raw jpeg data. """
    id_to_datauri = {}
    image_ids.sort()
    
    self.image_reader.SeekToStart()
    for image_id in image_ids:
      ok, value = self.image_reader.Find(py_base.Uint64ToKey(image_id))
      CHECK(ok, 'failed to find image_id: %d' % (image_id))
      jpeg = iw_pb2.JpegImage()
      jpeg.ParseFromString(value)
      CHECK(jpeg.IsInitialized())
      id_to_datauri[image_id] = JpegToDataUrl(jpeg.data)
    
    return id_to_datauri
  
def GenerateDistinctColorsCSS(N):
  HSV_tuples = [(x*1.0/N, 0.9, 1.0) for x in range(N)]
  RGB_tuples = map(lambda x: colorsys.hsv_to_rgb(*x), HSV_tuples)
  css_rgb_colors = []
  for rgb in RGB_tuples:
     css_rgb_colors.append('rgb(%d, %d, %d)' % (rgb[0]*255, rgb[1]*255, rgb[2]*255))
  return css_rgb_colors

