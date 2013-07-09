#!/usr/bin/python

import os
import urllib
import shutil
import logging
import tempfile
import tarfile
import zipfile
import exceptions
import fnmatch
from snap.pert import py_pert
from snap.pyglog import *
from iw import util
from snap.google.base import  py_base
import Image
import StringIO
from iw import iw_pb2
import glob
import exceptions


class DataImporterApp(object):
  def __init__(self):
    self.local_data_directory = os.path.expanduser('~/Desktop/datasets')
    return
  
  def __GetFileSizeBytes(self, url):
    info = urllib.urlopen(url).info()
    files_size_bytes = int(info['Content-Length'])
    return files_size_bytes
  
  def __GetDiskFreeSpaceBytes(self, pathname):
    "Get the free space of the filesystem containing pathname"
    stat= os.statvfs(pathname)
    # use f_bfree for superuser, or f_bavail if filesystem
    # has reserved space for superuser
    return stat.f_bfree*stat.f_bsize
    
  
  def __ImportTide(self, extract_dir, dataset_root):
    pert_filenames = []
    for root, dirnames, filenames in os.walk(extract_dir):
      for filename in dirnames:
        if fnmatch.fnmatch(filename, 'photoid_to_image.pert') or fnmatch.fnmatch(filename, 'objectid_to_object.pert'):
          pert_filenames.append(os.path.join(root, filename))
    if len(pert_filenames) != 2:
      return False
    src = pert_filenames[0]
    dst = dataset_root + '/' + os.path.basename(pert_filenames[0])
    shutil.move(src, dst)
    src = pert_filenames[1]
    dst = dataset_root + '/' + os.path.basename(pert_filenames[1])
    shutil.move(src, dst)
    return True
  
  def __ImportImageArchive(self, extract_dir, dataset_root):
    image_filenames = []
    print 'Searching for image files...'
    for root, dirnames, filenames in os.walk(extract_dir):
      for filename in filenames:
        if fnmatch.fnmatch(filename, '*.jpg') or fnmatch.fnmatch(filename, '*.jpeg'):          
          image_filenames.append(os.path.join(root, filename))      
    pert_uri = 'local://%s/photoid_to_image.pert' % (dataset_root)
    
    if len(image_filenames) <= 2:
      return False
  
    fingerprinted_path = '%s/fingerprinted/' % (extract_dir)
    os.mkdir(fingerprinted_path)
  
    print 'Fingerprinting image files...'
    progress = util.MakeProgressBar(len(image_filenames))
    # rename all files according to fingerprint
    for i, filename in enumerate(image_filenames): 
      data = open(filename).read()    
      fp = py_base.FingerprintString(data)
      dst = '%s/%064d.jpg' % (fingerprinted_path, fp) # ensure lexical sort = numeric sort = key sort
      os.rename(filename, dst)
      progress.update(i)
    
    filenames = glob.glob('%s/*.jpg' % fingerprinted_path )
    filenames.sort()
    output_uri = 'local://%s/photoid_to_image.pert' % (dataset_root) 
    
    # write to pert in sorted order
    print 'Generating image PERT file...'
    writer = py_pert.StringTableWriter()
    CHECK(writer.Open(output_uri, 1))
    progress = util.MakeProgressBar(len(filenames))
    for i, filename in enumerate(filenames): 
      data = open(filename).read()    
      key = py_base.Uint64ToKey(py_base.FingerprintString(data))      
      try:
        im = Image.open(StringIO.StringIO(data))
      except IOError as e:
        LOG(INFO, 'Error opening %s - %s' % (filename, e))
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
    return True
    
  
  def Run(self):
    
    archive_filename = None
    extract_dir = None
    dataset_name = None
    try:
      while not extract_dir:
        print 'If you do not have a dataset ready, you can use this example: '
        print 'http://graphics.stanford.edu/imagewebs/data/tide_v12.tar'
        print ''        
        provided_url = raw_input('Please enter url to dataset archive: ')
        while not dataset_name:
          dataset_name = raw_input('Please enter name for dataset (no spaces): ')
          if ' ' in dataset_name:
            dataset_name = None
        
#         provided_url = 'http://graphics.stanford.edu/imagewebs/data/british_telephone_booth.zip'
#         dataset_name = 'british_telephone_booth'
        
#         provided_url = 'http://graphics.stanford.edu/imagewebs/data/tide_v12.tar'
#         dataset_name = 'tide_v12'

        #provided_url = '/media/vol-4fcb8215/stanford_flickr_keyword.tar'
        #dataset_name = 'stanford_flickr_keyword'
      
        print 'Checking for space...'
        src_size_bytes = self.__GetFileSizeBytes(provided_url)
        tmp_size_bytes = self.__GetDiskFreeSpaceBytes('/tmp')
        dst_size_bytes = self.__GetDiskFreeSpaceBytes(self.local_data_directory)
        
        print 'src_size_bytes: %d' % (src_size_bytes)
        print 'tmp_size_bytes: %d' % (tmp_size_bytes)
        print 'dst_size_bytes: %d' % (dst_size_bytes)
        
        if src_size_bytes > tmp_size_bytes:          
          raise exceptions.RuntimeError('Source file is larger than available tmp space... ')
        
        if src_size_bytes > dst_size_bytes:          
          raise exceptions.RuntimeError('Source file is larger than available free space on destination partition... Consider expanding the root partition to make more space.  See docs here: http://image-web-cloud-tools.readthedocs.org/en/latest/manual/workstation/add_storage ')
        
        print 'Downloading...'
        extract_dir = tempfile.mkdtemp()
        #archive_filename, headers = urllib.urlretrieve(provided_url, extract_dir + '/download' )
        archive_filename, headers = urllib.urlretrieve(provided_url )
        
        print 'Unpacking archive...'
        if tarfile.is_tarfile(archive_filename):
          tar = tarfile.TarFile(archive_filename, 'r')
          tar.extractall(extract_dir)
        elif zipfile.is_zipfile(archive_filename):
          zip = zipfile.ZipFile(archive_filename, 'r')
          zip.extractall(extract_dir)
        else:
          raise exceptions.RuntimeError('not a zip or tar file!')
                
      dataset_root = '%s/%s' % (self.local_data_directory, dataset_name)
      
      if not os.path.exists(dataset_root):
        os.makedirs(dataset_root)
      
      print 'Importing archive...'
      if self.__ImportTide(extract_dir, dataset_root):
        print 'Done importing tide archive...'        
      elif self.__ImportImageArchive(extract_dir, dataset_root):
        print 'Done importing image archive...'
      else:
        print 'nothing could be imported...'
          
    except:    
      if extract_dir:
        shutil.rmtree(extract_dir, ignore_errors = True)
      raise
    
    if extract_dir:
      shutil.rmtree(extract_dir, ignore_errors = True)
    
    return

if __name__ == "__main__":      
  app = DataImporterApp()
  app.Run()
 
