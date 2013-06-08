#!/usr/bin/env python

from snap.pert import py_pert
from iw import iw_pb2
from tide import tide_pb2
import glob
import os
from snap.pyglog import *
from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, \
                        FileTransferSpeed, FormatLabel, Percentage, \
                        ProgressBar, ReverseBar, RotatingMarker, \
                        SimpleProgress, Timer
import fnmatch 

def NumLinesInFile(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def GetKeys(images_filename_cache):
  filenames_file = open(images_filename_cache, 'r')
  keys = []
  for i, filename in enumerate(filenames_file):
    filename = filename.strip()
    if len(filename) != 36:
      LOG(WARNING, 'skiping invalid hash format file: %s' % filename)
      continue
    
    # remove the '.jpg' bit
    hash_key = filename[:-4]
    keys.append(hash_key)
    
  return keys


def GlobRecursive(directory, pattern):
  for root, dirs, files in os.walk(directory):
    for basename in files:
      if fnmatch.fnmatch(basename, pattern):
        filename = os.path.join(root, basename)
        yield filename


  

def SlurpImagesToPert(src_path, output_uri):
  CHECK(os.path.isdir(src_path), 'expected dir: %s' % src_path)
  
  # generate filename cache if it doesn't yet exist
  images_filename_cache = '%s/filename_cache.txt' % (src_path)
  
  # force regen
  #if os.path.exists(images_filename_cache):
  #  os.remove(images_filename_cache)
  
  if not os.path.exists(images_filename_cache):
    print 'creating filename cache'
    # get list of files    
    filenames = list(GlobRecursive(src_path, '*.jpg'))

    filenames.sort()
    filenames_file = open(images_filename_cache, 'w')
    for filename in filenames:    
      filenames_file.write('%s\n' % filename)
    filenames_file.close()
  else:
    print 'using existing filename cache'
  
  num_files = NumLinesInFile(images_filename_cache)  
  filenames_file = open(images_filename_cache, 'r')
  
  num_shards = 1
  options = py_pert.WriterOptions()
  options.SetSorted('memcmp')
  writer = py_pert.StringTableWriter()
  writer.Open(output_uri, num_shards, options)
  widgets = ['Exporting to %s: ' % output_uri , Percentage(), ' ', Bar(),
           ' ', ETA(), ' ']
  pbar = ProgressBar(widgets=widgets, maxval=num_files).start()
  
  for i, filename in enumerate(filenames_file):
    filename = filename.strip()  
    key = filename[:-4]   # remove the '.jpg' bit
    data = open(filename).read()
    writer.Add(key, data)
    pbar.update(i)
    
  pbar.finish()    
  writer.Close()  
  return


def main():
  src_images_root = '/home/heathkh/Desktop/flickr_100k_distractor/oxc1_100k/'
  export_uri = 'local:///media/TideBackup/oxc1_100k_raw.pert'
  SlurpImagesToPert(src_images_root, export_uri)
    
  return
  
  

if __name__ == "__main__":
   main()



