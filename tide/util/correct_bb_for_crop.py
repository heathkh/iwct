#!/usr/bin/python

from tide.util import crop
from snap.pert import py_pert
from iw import iw_pb2
from tide import tide_pb2
from snap.pyglog import *
from snap.google.base import py_base
from iw import util as iwutil
import cPickle as pickle


def GetCachedImageSizes(uri):
  ok, scheme, path, error = py_pert.ParseUri(uri)      
  # check if we pickled the object previously
  hash_str = (uri + str(os.path.getmtime(path)) + str(os.path.getsize(path)))
  hash_val = hash(hash_str)
  cache_path = './cache.%s.pickle' % (hash_val)  
  imageid_to_size = None
  if os.path.exists(cache_path):
    f = open(cache_path, 'r')
    imageid_to_size = pickle.load(f)
  else:
    imageid_to_size = GetImageSizes(uri)
    f = open(cache_path, 'w')
    pickle.dump(imageid_to_size, f, protocol=2)            
  return imageid_to_size


def GetImageSizes(uri):
  imageid_to_size = {}
  reader = py_pert.StringTableShardSetReader()
  CHECK(reader.Open(uri))
  jpeg_image = iw_pb2.JpegImage()
  progress = iwutil.MakeProgressBar(reader.Entries())    
  for i, (k,v) in enumerate(reader):
    image_id = py_base.KeyToUint64(k)
    jpeg_image.ParseFromString(v)
    imageid_to_size[image_id] = (jpeg_image.width, jpeg_image.height)
    progress.update(i)    
  return imageid_to_size

def main():
  crop_fraction = 0.05
  base_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/tide_v08_distractors/'
  orig_image_uri = '%s/photoid_to_image.pert' % (base_uri)
  tide_uri = '%s/objectid_to_object.pert' % (base_uri)
  new_tide_uri = '%s/cropped_objectid_to_object.pert' % (base_uri)
  #cropped_image_uri = '%s/cropped_scaled_photoid_to_image.pert' % (base_uri)
  
  orig_sizes_dict = GetCachedImageSizes(orig_image_uri)
  reader = py_pert.StringTableReader()
  writer = py_pert.ProtoTableWriter()
  
  tide_object = tide_pb2.Object()
  
  CHECK(writer.Open(tide_object, new_tide_uri, 1))
  CHECK(reader.Open(tide_uri))
  progress = iwutil.MakeProgressBar(reader.Entries())
  for i, (k, v) in enumerate(reader):                                    
    tide_object.ParseFromString(v)
    CHECK(tide_object.IsInitialized())
    
    # adjust the bb of all the photos
    for photo in tide_object.photos:
      CHECK(photo.id in orig_sizes_dict)
      width, height = orig_sizes_dict[photo.id]
      try:      
        crop_rect = crop.CropRect(width, height, crop_fraction)
        for region in photo.regions:                          
          bb1_x, bb1_y, bb1_w, bb1_h = region.x1, region.y1, region.x2-region.x1, region.y2-region.y1
          bb2_x, bb2_y, bb2_w, bb2_h = crop_rect.ApplyCropToRect(bb1_x, bb1_y, bb1_w, bb1_h)
          region.x1 = bb2_x
          region.y1 = bb2_y
          region.x2 = bb2_x + bb2_w
          region.y2 = bb2_y + bb2_h
      except ValueError:
        print 'crop failed, not adjusting bb'
        
        
        
    # write adjusted proto to output
    writer.Add(k, tide_object.SerializeToString())
    progress.update(i)

  
  return
  
  

if __name__ == "__main__":
   main()



