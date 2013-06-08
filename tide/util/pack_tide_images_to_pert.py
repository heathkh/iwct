#!/usr/bin/env python

from pert import py_pert
from iw import iw_pb2
from tide import tide_pb2
import glob
import os
from pyglog import *

from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, \
                        FileTransferSpeed, FormatLabel, Percentage, \
                        ProgressBar, ReverseBar, RotatingMarker, \
                        SimpleProgress, Timer


def NumLinesInFile(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def BitsToMegabytes(num_bits):
  return float(num_bits * 1.19209e-7)

def BytesToMegabytes(num_bits):
  return float(num_bits * 9.53674e-7)


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


def GetSampleShardKeys(images_filename_cache, num_shards):
  all_keys = GetKeys(images_filename_cache)
  
  shard_0 = []
  shard_1 = []
  
  partitioner = py_pert.ModKeyPartitioner(num_shards)
  
  for key in all_keys:
    shard = partitioner.Partition(key)
    
    if shard == 0:
      shard_0.append(key)
    elif shard == 1:
      shard_1.append(key)
      
  return shard_0, shard_1  
      
    
  

def PackImagesDirectoryToPert(src_path, output_uri):
  CHECK(os.path.isdir(src_path), 'expected dir: %s' % src_path)
  
  # generate filename cache if it doesn't yet exist
  images_filename_cache = '%s/filename_cache.txt' % (src_path)
  
  # force regen
  #if os.path.exists(images_filename_cache):
  #  os.remove(images_filename_cache)
  
  if not os.path.exists(images_filename_cache):
    print 'creating filename cache'
    # get list of files
    filenames = glob.glob('%s/*.jpg' % src_path)
    filenames.sort()    
    filenames_file = open(images_filename_cache, 'w')
    for filename in filenames:
      filebase = os.path.basename(filename)
      filenames_file.write('%s\n' % filebase)
    filenames_file.close()
  else:
    print 'using existing filename cache'
  
  num_files = NumLinesInFile(images_filename_cache)  
  filenames_file = open(images_filename_cache, 'r')
  
  key_size_bytes = 32
  block_size_mb = 0.5
  num_shards = 10
  desired_bloom_error_rate = 0.005
  num_files_per_shard = long(float(num_files)/num_shards)
  
  num_blocks_per_shard = 4000
  index_bytes_per_block = key_size_bytes + 8*3
  index_bytes_per_shard = index_bytes_per_block * num_blocks_per_shard  
  
  sample_pos_keys, sample_neg_keys = GetSampleShardKeys(images_filename_cache, num_shards)
  
  num_bits_tuned = py_pert.TuneRequiredBits(sample_pos_keys,sample_neg_keys, desired_bloom_error_rate)*num_shards
  num_megabytes_tuned = BitsToMegabytes(num_bits_tuned)
  print 'num_megabytes_tuned: %f' % num_megabytes_tuned
  
  num_megabytes_active_blocks = block_size_mb * num_shards
  num_megabytes_indices = BytesToMegabytes(index_bytes_per_shard*num_shards)
  num_megabytes_bloom_filters = num_megabytes_tuned
  
  print 'num_megabytes_active_blocks: %f' % num_megabytes_active_blocks
  print 'num_megabytes_indices: %f' % num_megabytes_indices
  print 'num_megabytes_bloom_filters: %f' % num_megabytes_bloom_filters
  
  options = py_pert.WriterOptions()
  options.SetBlockSize(long(1048576 * block_size_mb)) # bytes per block
  options.SetSorted('memcmp')
  options.SetBloomFilterBitsPerShard(num_bits_tuned)
  writer = py_pert.StringTableWriter()
  writer.Open(output_uri, num_shards, options)
  
  widgets = ['Exporting to %s: ' % output_uri , Percentage(), ' ', Bar(),
           ' ', ETA(), ' ']
  pbar = ProgressBar(widgets=widgets, maxval=num_files).start()
  
  for i, filename in enumerate(filenames_file):
    filename = filename.strip()
    if len(filename) != 36:
      LOG(WARNING, 'skiping invalid hash format file: %s' % filename)
      continue
    
    # remove the '.jpg' bit
    hash_key = filename[:-4]
    data = open(src_path + "/" + filename).read()
    writer.Add(hash_key, data)
    pbar.update(i)
    
  pbar.finish()
    
  writer.Close()  
  return

def GetImmediateSubdirectories(dir):
    return [os.path.join(dir, name) for name in os.listdir(dir)
            if os.path.isdir(os.path.join(dir, name))]


def main():
  tide_images_root = '/home/heathkh/Desktop/tide/objects'
  export_uri_base = 'local:///media/TideBackup/test/'
  
  object_dirs = GetImmediateSubdirectories(tide_images_root)
  
  for object_dir in object_dirs:
    object_name = os.path.basename(object_dir)
    output_pert_uri = '%s/%s.pert' % (export_uri_base,object_name)
    PackImagesDirectoryToPert(object_dir, output_pert_uri)
    
  
  return
  
  

if __name__ == "__main__":
   main()



