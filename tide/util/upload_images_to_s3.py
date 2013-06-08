#!/usr/bin/env python

#import glob
#import os

import math
import StringIO
from snap import boto
#from snap.boto import s3
from snap.boto.s3 import key
from snap.pert import py_pert
from iw import iw_pb2
from tide import tide_pb2
from snap.pyglog import *
from snap.google.base import py_base
from iw import util as iwutil
from iw import visutil
from PIL import Image
import multiprocessing
import time

class JobQueueWorker(multiprocessing.Process):
  def __init__(self, i, queue):
    super(JobQueueWorker, self).__init__()
    self.i = i
    self.queue = queue      
    return

  def run(self):
    print 'Worker %d started' % (self.i)        
    for runnable in iter( self.queue.get, None ):
      if runnable:
        runnable.Run()
    return           
  

class JobQueue(object):    
  def __init__(self, num_workers = 10, max_queue_size = 50):  
    self.queue = multiprocessing.Queue(max_queue_size)
    self.workers = []
    for i in range(num_workers):
      new_worker = JobQueueWorker(i, self.queue) 
      self.workers.append(new_worker)
      new_worker.start()
    return
  
  def AddJob(self, job):
    #TODO: check that job object has a Run method
    self.queue.put(job)
    return
  
  def NumPendingJobs(self):
    return self.queue.qsize()  


  def WaitForJobsDone(self):
    num_pending = self.NumPendingJobs()
    while num_pending > 0:
      print 'num_pending: %d' % (num_pending)
      time.sleep(1)
      num_pending = self.NumPendingJobs()
    
    # TODO terminate the workers
    for worker in self.workers:
      self.queue.put(None)
      
    #for worker in self.workers:
    #  worker.stop()  
    return 
        

def ResizeImage(jpeg_image_data, max_area_pixels):

  # if no resize needed, just return input jpeg data
  if max_area_pixels == None: 
    return jpeg_image_data    
  image = Image.open(StringIO.StringIO(jpeg_image_data))
  width, height = image.size
  
  CHECK_GT(width, 0)
  CHECK_GT(height, 0)
  
  area = width*height
  # if the image is below the max size, just return the input jpeg data
  if area < max_area_pixels:
    return jpeg_image_data  
  # otherwise, we need to resize image      
  scaling = math.sqrt(float(max_area_pixels)/area)
  CHECK_LT(scaling, 1.0)  
  new_width, new_height = (int(scaling*width), int(scaling*height))  
  CHECK_GT(new_width, 0)
  CHECK_GT(new_height, 0)  
  new_area = new_width * new_height  
  #LOG(INFO, '(%d, %d) -> (%d, %d)' % (width, height, new_width, new_height))   
  CHECK_LE(new_area, max_area_pixels)
  resized_image = image.resize((new_width, new_height))  
  output = StringIO.StringIO()
  resized_image.save(output, format="JPEG", quality = 60)
  resized_image_data = output.getvalue()  
  return resized_image_data


def ResizeAndUploadImage(bucket, sizes, image_id, jpeg_image_data):  
  for size_name, max_pixels in sizes.iteritems():
    filename = '%d_%s.jpg' % (image_id, size_name)
    key = None
    while True:
      try:
        key = bucket.get_key(filename)
        break
      except:
        time.sleep(2)
        pass     
    if key != None:      
      continue
    
    resized_image_data = ResizeImage(jpeg_image_data, max_pixels)   
    key = bucket.new_key(filename)
    key.set_contents_from_string(resized_image_data)
    key.set_metadata('Content-Type', 'image/jpeg')
    key.set_acl('public-read')
    #print 'filename: %s' % (filename)    
  return


class ResizeAndUploadImageJob():
  def __init__(self, bucket, sizes, image_id, jpeg_image_data):
    self.bucket = bucket
    self.sizes = sizes
    self.image_id = image_id
    self.jpeg_image_data = jpeg_image_data
    return    
  
  def Run(self):
    ResizeAndUploadImage(self.bucket, self.sizes, self.image_id, self.jpeg_image_data)
    return  

def main():
  dataset_name = 'tide_v08'
  sizes = {}
  sizes['thumbnail'] = 100*100
  sizes['small'] = 640*480   
  reset_bucket = False
  
  #dataset_base_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/%s/' % (dataset_name)
  #images_uri = '%s/cropped_scaled_photoid_to_image.pert' % (dataset_base_uri)
  images_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v14/cropped_scaled_photoid_to_image_randomaccess.pert'
  bucket_name = 'tide_image_cache'
  s3 = boto.connect_s3()
  
  bucket = s3.create_bucket(bucket_name)
  if reset_bucket:
    LOG(INFO, 'listing contents of bucket...')
    all_keys = [key.name for key in bucket.list()]
    LOG(INFO, 'deleting contents of bucket...')
    bucket.delete_keys(all_keys)      
    s3.delete_bucket(bucket_name)
    bucket = s3.create_bucket(bucket_name)
    bucket.set_acl('public-read')
  
  reader = py_pert.StringTableReader()
  CHECK(reader.Open(images_uri))    
  progress = iwutil.MakeProgressBar(reader.Entries())
  
  num_workers = 200
  max_queue_size = 200
  job_queue = JobQueue(num_workers, max_queue_size)
  for i, (key, value) in enumerate(reader):    
    image_id = py_base.KeyToUint64(key)
    jpeg_image = iw_pb2.JpegImage()
    jpeg_image.ParseFromString(value)      
    job_queue.AddJob(ResizeAndUploadImageJob(bucket, sizes, image_id, jpeg_image.data))    
    progress.update(i)    
  
  job_queue.WaitForJobsDone()
  
  return
  

if __name__ == "__main__":
   main()



