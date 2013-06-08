#!/usr/bin/env python
from snap import boto
from snap.boto.s3 import key
from snap.pyglog import *
import fnmatch

def main():
  s3 = boto.connect_s3()
  
  buckets = s3.get_all_buckets()
  
  bucket_names = []
  for bucket in buckets:
    print bucket.name
    bucket_names.append(bucket.name)
    
  print 'which bucket would you like to delete? (accepts glob syntax): '  
  bucket_pattern = raw_input()
  
  buckets_to_delete = fnmatch.filter(bucket_names, bucket_pattern)
  
  print 'buckets_to_delete: '
  print buckets_to_delete  
  print 'Are you sure you want to delete these buckets:?'
  
  confirm = raw_input()
  if confirm != 'y':
    return
  
  for bucket_name in buckets_to_delete:
    bucket = s3.get_bucket(bucket_name)
    CHECK(bucket)
    LOG(INFO, 'bucket %s' % (bucket_name))
    LOG(INFO, 'listing contents of bucket...')
    all_keys = [key.name for key in bucket.list()]
    LOG(INFO, 'deleting contents of bucket...')
    bucket.delete_keys(all_keys)      
    LOG(INFO, 'deleting bucket...')
    s3.delete_bucket(bucket_name)
    
    
  return
  

if __name__ == "__main__":
   main()



