#!/usr/bin/env python


from pert import py_pert
from iw import iw_pb2
from tide import tide_pb2
from iw import util as iwutil
from snap.pyglog import *
from tide import tide

def LoadTide(tide_uri):
  objectid_to_object = {}
  LOG(INFO, 'starting to load tide dataset...')
  # load list of images that belong to each tide object    
  tide_reader = py_pert.StringTableReader()
  CHECK(tide_reader.Open(tide_uri))
  for index, (k, v) in enumerate(tide_reader):                                  
    tide_object = tide_pb2.Object()
    tide_object.ParseFromString(v)
    CHECK(tide_object.IsInitialized())
    objectid_to_object[tide_object.id] = tide_object
  return objectid_to_object


def SaveTide(objectid_to_object, output_uri):
  writer = py_pert.ProtoTableWriter()
  CHECK(writer.Open(tide_pb2.Object(), output_uri, 1)) 
  for object_id, object in objectid_to_object.iteritems():
    writer.Add(iwutil.Uint64ToKey(object_id), object.SerializeToString())
  return

def main():
  
  dataset_root_uri = 'local://media/vol-0449ca74/itergraph/'
  input_dataset_name = 'tide_v08'
  output_dataset_name = 'tide_v08_distractors'
  distractor_images_uri = 'local://media/vol-0449ca74/oxc1_100k/photoid_to_image.pert'
  
  input_tide_uri = '%s/%s/objectid_to_object.pert' % (dataset_root_uri, input_dataset_name)
  output_tide_uri = '%s/%s/objectid_to_object.pert' % (dataset_root_uri, output_dataset_name)
  input_images_uri = '%s/%s/photoid_to_image.pert' % (dataset_root_uri, input_dataset_name)
  output_images_uri = '%s/%s/photoid_to_image.pert' % (dataset_root_uri, output_dataset_name)


  #merge images 
  CHECK(py_pert.MergeTables( [input_images_uri, distractor_images_uri], output_images_uri))
    
  objectid_to_object = LoadTide(input_tide_uri)
  distractors = tide.ImageSequenceReader(distractor_images_uri)  
  num_distractors = distractors.GetNumImages()
  num_objects = len(objectid_to_object) 
  num_distractors_per_object = int(float(num_distractors)/num_objects)
  
  # for each object
  progress = iwutil.MakeProgressBar(num_distractors)
  count = 0
  for object_id, object in objectid_to_object.iteritems():
    # augment object with distractor images
    for i in range(num_distractors_per_object):
      image_id, jpeg = distractors.GetNextImage()
      new_photo = object.photos.add()
      new_photo.id = image_id
      new_photo.label = tide_pb2.NEGATIVE
      count += 1
      progress.update(count)
  
  # save tide pert in output directory
  SaveTide(objectid_to_object, output_tide_uri)
  
  
  
  return 
  
  

if __name__ == "__main__":
   main()



