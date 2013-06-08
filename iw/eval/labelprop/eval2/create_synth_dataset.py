#!/usr/bin/python

import os
import StringIO
from PIL import Image
from iw import iw_pb2
from iw import visutil
from iw import util as iwutil
from snap.pert import py_pert
from snap.pyglog import *
from tide import tide_pb2 
from tide import tide

class DistractorImageLoader(object):
  def __init__(self, images_uri):
    self.reader = py_pert.StringTableReader()
    CHECK(self.reader.Open(images_uri))
    
  def GetNextImage(self):
    jpeg_image = iw_pb2.JpegImage()
    ok, k,v = self.reader.Next()
    CHECK(ok)
    jpeg_image.ParseFromString(v)
    CHECK(jpeg_image.IsInitialized())
    return jpeg_image
  
 
def CreateMixedJpeg(jpeg_a, jpeg_b):  
  image_a = Image.open(StringIO.StringIO(jpeg_a.data))
  image_b = Image.open(StringIO.StringIO(jpeg_b.data))
  
  width_a, height_a = image_a.size
  width_b, height_b = image_b.size
  
  # find the scaling of b it as tall as image a
  scaling_b = float(height_a)/height_b
  
  new_width_b, new_height_b = (int(scaling_b*width_b), height_a)
  
  image_b = image_b.resize((new_width_b, new_height_b))
  
  out_image = Image.new("RGB", (width_a + new_width_b, height_a), "black")
  out_image.paste(image_a, (0,0))
  out_image.paste(image_b, (width_a,0))
  
  #out_image.show()
  output = StringIO.StringIO()
  out_image.save(output, format="JPEG")
  new_jpeg = iw_pb2.JpegImage()
  new_jpeg.data = output.getvalue()
  new_jpeg.width, new_jpeg.height =out_image.size
  CHECK(new_jpeg.IsInitialized())
  return new_jpeg
  
def InitNewObject(old_object):
  new_object = tide_pb2.Object()
  new_object.id = old_object.id
  new_object.name = old_object.name
  new_object.description = old_object.description
  new_object.purity = -1
  new_object.photos.extend([photo for photo in old_object.photos if photo.label == tide_pb2.POSITIVE ])
  return new_object

def InitNoneLabels(new_object, none_image_ids):  
  for id in none_image_ids:
    photo = new_object.photos.add()
    photo.id = id
    photo.label = tide_pb2.NONE
  return

def OpenTideDataset(tide_uri):
  LOG(INFO, 'starting to load tide dataset...')
  # load list of images that belong to each tide object    
  tide_reader = py_pert.StringTableReader()
  tide_reader.Open(tide_uri)
  tide_objects = []
  for index, (k, v) in enumerate(tide_reader):                                  
    tide_object = tide_pb2.Object()
    tide_object.ParseFromString(v)
    CHECK(tide_object.IsInitialized())
    tide_objects.append(tide_object)
  return tide_objects  

def main():
#  images_uri = 'local://media/vol-0449ca74/itergraph/tide_v14/cropped_scaled_photoid_to_image_randomaccess.pert'
#  tide_uri = 'local://media/vol-0449ca74/itergraph/tide_v14/objectid_to_object.pert'
#  distractor_images_uri = 'local://media/vol-0449ca74/oxc1_100k/photoid_to_image.pert'  
#  output_base_uri = 'local://media/vol-0449ca74/itergraph/tide_v14_mixed_v2/'
  
#  images_uri = 'local://media/vol-0449ca74/itergraph/tide_v16/photoid_to_image.pert'
#  tide_uri = 'local://media/vol-0449ca74/itergraph/tide_v16/objectid_to_object.pert'
#  distractor_images_uri = 'local://media/vol-0449ca74/oxc1_100k/photoid_to_image.pert'  
#  output_base_uri = 'local://media/vol-0449ca74/itergraph/tide_v16_mixed/'

  images_uri = 'local://media/vol-0449ca74/itergraph/tide_v18/photoid_to_image.pert'
  tide_uri = 'local://media/vol-0449ca74/itergraph/tide_v18/objectid_to_object.pert'
  distractor_images_uri = 'local://media/vol-0449ca74/oxc1_100k/photoid_to_image.pert'  
  output_base_uri = 'local://media/vol-0449ca74/itergraph/tide_v18_mixed/'
  
  output_tide_uri = '%s/objectid_to_object.pert' % (output_base_uri)
  output_images_uri = '%s/photoid_to_image.pert'  % (output_base_uri)
  
  image_loader = visutil.BatchImageLoader(images_uri)  
  tide_objects = OpenTideDataset(tide_uri)
  distractor_image_loader = DistractorImageLoader(distractor_images_uri)
  CHECK_EQ(len(tide_objects), 2)
  object_a = tide_objects[0]
  object_b = tide_objects[1]
   
  new_object_a = InitNewObject(object_a) 
  new_object_b = InitNewObject(object_b)
  
  a_none_image_ids = [photo.id for photo in object_a.photos if photo.label == tide_pb2.NONE ]  
  b_none_image_ids = [photo.id for photo in object_b.photos if photo.label == tide_pb2.NONE ]  
  
  mixed_aux_images = {}
  for i, (imageid_a, imageid_b) in enumerate(zip(a_none_image_ids, b_none_image_ids)):    
    mixed_aux_images[imageid_a] = (imageid_a, imageid_b)

  mixed_image_ids = mixed_aux_images.keys()
  n = int(len(mixed_image_ids)/2.0)
  InitNoneLabels(new_object_a, mixed_image_ids[0:n])
  InitNoneLabels(new_object_b, mixed_image_ids[n:-1])
  new_objects = [new_object_a, new_object_b]
  
  image_ids = []
  for obj in new_objects:
    for photo in obj.photos:
      image_ids.append(photo.id)
  
  image_ids.sort()
  
  # write new tide pert
  tide_writer = py_pert.ProtoTableWriter()  
  tide_writer.Open(tide_pb2.Object(), output_tide_uri, 1)
  for obj in new_objects:
    tide_writer.Add(iwutil.Uint64ToKey(obj.id), obj.SerializeToString())  
  tide_writer.Close()
  
  # write new image pert
  try:
    image_writer = py_pert.ProtoTableWriter()  
    image_writer.Open(iw_pb2.JpegImage(), output_images_uri, 1)
    used_image_ids = set()
    progress = iwutil.MakeProgressBar(len(image_ids))
    for i, image_id in enumerate(image_ids):
      jpeg = None
      if image_id in mixed_aux_images:
        imageid_a, imageid_b = mixed_aux_images[image_id]
        jpeg_a = image_loader.GetImage(imageid_a)
        jpeg_b = image_loader.GetImage(imageid_b)
        if jpeg_a == None or jpeg_b == None:
          LOG(INFO, 'skipping missing jpeg') 
          continue
        jpeg = CreateMixedJpeg(jpeg_a, jpeg_b)
      else:
        distractor = None
        while True:
          distractor = distractor_image_loader.GetNextImage()
          if distractor.width > distractor.height:
            break
        CHECK(distractor)
        jpeg = CreateMixedJpeg(image_loader.GetImage(image_id), 
                               distractor)
      
      CHECK(image_id not in used_image_ids)
      CHECK(jpeg)
      image_writer.Add(iwutil.Uint64ToKey(image_id), jpeg.SerializeToString())
      used_image_ids.add(image_id)
      progress.update(i)
    image_writer.Close()
  except:
    pass  
  
                                                        
  return 
    

if __name__ == "__main__":      
  main()
 