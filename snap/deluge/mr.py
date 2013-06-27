#!/usr/bin/python
import shutil
import os
import subprocess
import sys
import time
import logging
from pert import py_pert
from pyglog import *
import ast
from iw import util as iwutil
from snap.deluge import deluge_pb2
import base64

# TODO(heathkh): remove the need for nfs_base
nfs_base = '/mapr/iwct/'


def Base64EncodeProto(msg):
  """ Encodes the proto as a base64 string which is safe to pass around on command line. """
  encoded_string = base64.b64encode(msg.SerializeToString())
  return encoded_string


def RunAndGetOutput(*popenargs, **kwargs):
  """
  Runs command, blocks until done, and returns the output.
  Raises exception if non-zero return value 
  """
  process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
  output, unused_err = process.communicate()
  retcode = process.poll()
  if retcode:
      cmd = kwargs.get("args")
      if cmd is None:
          cmd = popenargs[0]
      error = subprocess.CalledProcessError(retcode, cmd)
      error.output = output      
      raise error
  return output    


def GetClusterProperty(property):
  """ Queries the MAPR cluster for property value. """
  cmd = 'cirrus_cluster_cli get_property %s' % (property)
  output = RunAndGetOutput(cmd, shell=True)
  dict_result = ast.literal_eval(output)
  return dict_result


def GetNumActiveTaskTrackers():
  """ Queries the MAPR system for number of task trackers. """  
  print 'checking current number of active task trackers on cluster...'    
  result = GetClusterProperty('slot_summary')
  CHECK(result)  
  num_task_trackers = 0
  for key, value in result.iteritems():
    #print key, value    
    num_task_trackers += 1    
  return num_task_trackers


def GetNumCoresPerTaskTracker():
  num_cores_list = GetClusterProperty('cores_summary')
  CHECK(num_cores_list)  
  min_num_cores = 100000
  max_num_cores = 0
  for num_cores in num_cores_list:
    min_num_cores = min(min_num_cores, num_cores)  
    max_num_cores = max(max_num_cores, num_cores)
  CHECK_EQ(min_num_cores, max_num_cores)
  return max_num_cores


def GetGBRamPerTaskTracker():
  ram_list = GetClusterProperty('ram_summary')
  CHECK(ram_list)  
  min_ram = min(ram_list)
  max_ram = max(ram_list)  
  CHECK_EQ(min_ram, max_ram)
  return min_ram
  

def GetNumMapSlots():
  """ Queries the MAPR system for number of reduce slots. """  
  print 'checking current number of reduce slots on cluster...'
  result = GetClusterProperty('slot_summary')
  CHECK(result)  
  num = 0
  for host, host_data in result.iteritems():
    CHECK('map_slots' in host_data)    
    num += host_data['map_slots']
  return num  


def GetNumReduceSlots():
  """ Queries the MAPR system for number of reduce slots. """  
  print 'checking current number of reduce slots on cluster...'
  result = GetClusterProperty('slot_summary')
  CHECK(result)  
  num = 0
  for host, host_data in result.iteritems():
    #print key, value
    CHECK('reduce_slots' in host_data)    
    num += host_data['reduce_slots']
  return num


def GetNumMapReduceSlotsPerNode():
  """ Queries the MAPR system for number of map and reduce slots per mpde. """  
  result = GetClusterProperty('slot_summary')
  CHECK(result)  
  num_map_slots = 0
  num_reduce_slots = 0
  num_nodes = 0
  for host, host_data in result.iteritems():
    #print key, value
    CHECK('reduce_slots' in host_data)    
    num_nodes += 1
    num_reduce_slots += host_data['reduce_slots']
    num_map_slots += host_data['map_slots']
  CHECK_GE(num_nodes, 0)  
  num_map_slots_per_node = num_map_slots / num_nodes
  num_reduce_slots_per_node = num_reduce_slots / num_nodes  
  return num_map_slots_per_node, num_reduce_slots_per_node


def GetRackTopology():
  """ Queries the MAPR system for rack topology. """  
  result = GetClusterProperty('rack_topology')
  CHECK(result)      
  return result 


def SetNumSlotsPerNode(num_map_slots_per_node, num_reduce_slots_per_node):
  """ Force the MAPR system to change the number of map and reduce slots per node. 
  """
  # TODO(kheath): rewrite this to not depend on relative path to cirrus
  num_map_slots_str = str(int(num_map_slots_per_node))
  num_reduce_slots_str = str(int(num_reduce_slots_per_node))  
  cmd = 'cirrus_cluster_cli set_map_reduce_slots_per_node %s %s' % (num_map_slots_str, num_reduce_slots_str)
  RunAndGetOutput(cmd, shell=True)
  
  # wait for change to take affect
  while True:
    cur_num_map_per_node, cur_num_reduce_per_node = GetNumMapReduceSlotsPerNode()
    if cur_num_map_per_node == num_map_slots_per_node and cur_num_reduce_per_node == num_reduce_slots_per_node:
      break
    else:
      LOG(INFO, 'waiting for slot change... %d %d' % (cur_num_map_per_node, cur_num_reduce_per_node)) 
      time.sleep(5)        
  return 


def UriToNfsPath(uri):
  """
  Converts a uri to the equivalent local path using the NFS local mount point. 
  """
  ok, scheme, path, error = py_pert.ParseUri(uri)
  assert(ok)
  nfs_path = None
  if scheme == 'local':
    nfs_path = path
  elif scheme == 'maprfs':
    nfs_path = '%s/%s' % (nfs_base, path)  
  else:
    LOG(FATAL, 'unexpected scheme: %s' % scheme)
     
  return nfs_path


# def CopyUri(src_uri, dst_uri):
#   """ Makes a copy of src_uri available at the given dst_uri.
#   If src is a file, dst will be a file.
#   If src is a dir, dst will be a dir.
#   If dst already exists, it is overwritten.
#   """
#   src_path = UriToNfsPath(src_uri)
#   dst_path = UriToNfsPath(dst_uri)
#   if not os.path.exists(src_path):
#     LOG(FATAL, 'src uri does not exist: %s' % src_uri)
#   # To support overwrite, delete dst path recusively to be sure it doesn't exist
#   if os.path.exists(dst_path):
#     shutil.rmtree(dst_path, ignore_errors=True)
#   # create directory structure to parent of dst_uri if it doesn't exist  
#   try:
#     os.makedirs(os.path.dirname(dst_path))
#   except:
#     pass
#   # if src is a file
#   if os.path.isfile(src_path):
#     shutil.copyfile(src_path, dst_path)
#     shutil.copystat(src_path, dst_path) # transfer modified time
#   # if src is a directory
#   elif os.path.isdir(src_path):
#     # copy directory recusively 
#     shutil.copytree(src_path, dst_path)    
#   else:
#     LOG(FATAL, 'unexpected condition') 
#   return

def CopyUri(src_uri, dst_uri):
  """ Makes a copy of src_uri available at the given dst_uri.
  If src is a file, dst will be a file.
  If src is a dir, dst will be a dir.
  If dst already exists, it is overwritten.
  """  
  py_pert.CopyUri(src_uri, dst_uri)
  return   
 

# def CalculatePertFileSize(root):
#   names = sorted(os.listdir(root))
#   paths = [os.path.realpath(os.path.join(root, n)) for n in names]
#   # handles dangling symlinks
#   total = sum(os.stat(p).st_size for p in paths if os.path.exists(p))
#   return total

def CalculatePertFileSize(pert_uri):
  shard_uris = py_pert.GetShardUris(pert_uri)
  total = 0
  for shard_uri in shard_uris:
    print shard_uri
    ok, size = py_pert.FileSize(shard_uri)
    print size
    CHECK(ok)
    total += size   
  return total


class Timer(object):
  def __init__(self, verbose=False):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    self.msecs = self.secs * 1000  # millisecs
    if self.verbose:
      print str(self)
   
  def __str__(self):
    return 'elapsed time: %f ms' % self.msecs
      
def GetUriSplitInfo(uri):
  #TODO(heathkh): Can you make this faster by collecting max block size and entries without fully opening the reader?
  
  uris = py_pert.GetShardUris(uri)
  num_entries = 0
  max_block_size = 0
  progress = iwutil.MakeProgressBar(len(uris))
  for i, uri in enumerate(uris):
    reader = py_pert.StringTableShardReader()
    reader.Open(uri)
    max_block_size = max(max_block_size, reader.MaxBlockSize())
    num_entries += reader.Entries()
    progress.update(i)
  return num_entries, max_block_size
  
def ComputeMaxNumSplits(input_uri):
  """ Calculate the number of splits that are possible for a given pert file."""
  total_size_bytes = CalculatePertFileSize(input_uri)
  num_shards = py_pert.GetNumShards(input_uri)
  num_entries, max_block_size = GetUriSplitInfo(input_uri)
  min_split_size = max_block_size
  if min_split_size == 0:
    LOG(FATAL, 'The input is empty: %s' % input_uri)
  
  if not num_entries:
    LOG(FATAL, 'pert file has no entries: %s'  % input_uri )
    
  # if have more splits than this, then one of them will be too small
  max_num_splits = max(1, int(total_size_bytes / float(min_split_size) ))
  
  #print 'total_size_bytes: %d' % (total_size_bytes)
  #print 'min_split_size: %d' % (min_split_size)
  #print 'max_num_splits: %d' % (max_num_splits)
  
  # we can't have more splits than we have entries
  if num_entries < max_num_splits:
    max_num_splits = num_entries
    
  return max_num_splits


def ComputeNumSplits(input_uris_csv, desired_splits_per_map_slot):
  """ Calculate the number of splits that are possible for a given pert file.
  input_uri is a comma seperated list of uris
  """
  
  desired_splits_per_map_slot = max(1, desired_splits_per_map_slot)
  CHECK_GE(desired_splits_per_map_slot, 1)
  uris = [x.strip() for x in input_uris_csv.split(',')]
  LOG(INFO, 'computing max num splits for %s' % (uris))
  total_num_map_slots = GetNumMapSlots()
  CHECK_GE(total_num_map_slots, 1)
  desired_num_splits = desired_splits_per_map_slot * total_num_map_slots
  max_num_splits_pert = float('inf')
  for uri in uris:    
    max_num_splits_pert = min(max_num_splits_pert, ComputeMaxNumSplits(uri))
  num_splits = min(desired_num_splits, max_num_splits_pert)  
  print 'max_num_splits_pert: %d' % max_num_splits_pert
  print 'desired_num_splits: %d' % desired_num_splits      
  print 'num_splits: %d' % num_splits
  return num_splits

  
def EnsureList(param):
  if isinstance(param, (list, tuple)):
    return param
  else:
    return [param]
    

class MapReduceDriverHadoopCache(object):
  """ Runs a pipes MapReduce stage. """
  def __init__(self, pipes_binary, input_path, output_path, num_map_jobs, 
               num_reduce_jobs, output_sorted = True, parameters = dict(), 
               uris_to_cache=[], libjars=[]):    
    self.pipes_binary = pipes_binary    
    self.input_path = input_path
    self.output_path = output_path 
    self.num_map_jobs = int(num_map_jobs)
    assert(self.num_map_jobs)
    self.num_reduce_jobs = num_reduce_jobs    
    self.parameters = parameters
    self.libjars = EnsureList(libjars)
     
    # uris_to_cache can be either a single string or a list of strings
    # convert to list format here 
    self.uris_to_cache = EnsureList(uris_to_cache)
    
    for uri in self.uris_to_cache:
      ok, scheme, path, error = py_pert.ParseUri(uri)
      CHECK(ok)
      CHECK_EQ(scheme, 'maprfs')
      
    # set snappy compression for output if none selected
    if not self.parameters.has_key('pert.recordwriter.compression_codec_name'):
      self.parameters['pert.recordwriter.compression_codec_name'] = 'snappy'
    
    # set memcmp comparator if output should be sorted
    if output_sorted:
      if (self.parameters.has_key('pert.recordwriter.comparator_name')):
        logging.fatal('you requested sorted output but already set a comparator name: %s' , self.parameters['pert.recordwriter.comparator_name'])
      self.parameters['pert.recordwriter.comparator_name'] = 'memcmp'
    else:
      self.parameters['pert.recordwriter.comparator_name'] = 'none'
               
    return
 
  
  def __CreateValidUriCacheString(self, uris_to_cache):
    #TODO(kheath): if hadoop pipes -files flag is passed a uri with fewer than /// slashes after scheme: it fails... This rewrites uris to deal with this bug.
    # Remove this hack one this bug if fixed in hadoop.    
    clean_cache_uris = []
    for uri in uris_to_cache:
      ok, scheme, path, error = py_pert.ParseUri(uri)
      CHECK(ok)
      CHECK_EQ(scheme, 'maprfs')
      CHECK(py_pert.Exists(uri), 'uri not there: %s' % uri)
      clean_cache_uris.append( py_pert.CanonicalizeUri(uri))
      
    uri_cache_string = ','.join(clean_cache_uris)
    return uri_cache_string
  
  def MakeRunCommand(self):  
    # check preconditions
    assert(self.input_path)
    assert(self.output_path)               
    assert(self.pipes_binary)
    CHECK_NE(self.num_map_jobs, None)
    CHECK_NE(self.num_reduce_jobs, None)
    
    # TODO(kheath): pipes fails with no helpful error message when scheme prefix
    # is used.  Is this expected? 
    # Workaround for now is to strip off scheme prefix
    scheme_free_input_paths = []
    for orig_input_path in self.input_path.split(','):      
      ok, scheme, path, error = py_pert.ParseUri(orig_input_path)
      CHECK_EQ(scheme, "maprfs")
      CHECK(ok, error)
      scheme_free_input_paths.append(path)
    self.input_path = ','.join(scheme_free_input_paths)
    
    ok, scheme, self.output_path, error = py_pert.ParseUri(self.output_path)
    CHECK(ok, error)
    CHECK_EQ(scheme, "maprfs")
    
    remote_binary = '/data/deluge/bin/%s' % (self.pipes_binary)
    remote_binary_uri = 'maprfs://' + remote_binary
    CopyUri('local://' + self.pipes_binary, remote_binary_uri)
    # if profiler is turned on and a profiler_timeout is enabled, disable the failure redundancy so we get profile result faster 
    if 'profiler' in self.parameters and self.parameters['profiler'] == 'on' and 'profiler_timeout_sec' in self.parameters:
      self.parameters['mapred.map.max.attempts']='1'
      self.parameters['mapred.reduce.max.attempts']='1'
              
    if 'mapred.task.timeout' not in self.parameters:
      self.parameters['mapred.task.timeout']='1200000' #  = 20 min
      
    self.parameters['mapred.map.tasks.speculative.execution']='true'
    self.parameters['mapred.reduce.tasks.speculative.execution']='true'
    self.parameters['mapred.compress.map.output']='true'
    self.parameters['mapred.map.output.compression.codec']='org.apache.hadoop.io.compress.SnappyCodec'
    
    # These are required for c++ code that uses maprfs or hdfs
    self.parameters['mapred.map.child.env'] = 'CLASSPATH=$CLASSPATH:$(hadoop classpath)'
    self.parameters['mapred.reduce.child.env'] = 'CLASSPATH=$CLASSPATH:$(hadoop classpath)'
    
     
    binary_name = os.path.basename(self.pipes_binary)
    job_name = '%s_%d' % (binary_name, time.time())
    
    libjars_uris = []
    
    for local_libjar_path in self.libjars:
      remote_jar = '/data/deluge/jar/%s' % (local_libjar_path)
      remote_jar_uri = 'maprfs://' + remote_jar
      print 'uploading jar: %s' % remote_jar_uri
      CopyUri('local://' + local_libjar_path, remote_jar_uri)
      libjars_uris.append(remote_jar_uri)
    
    cmd = 'hadoop pipes '    
    
    if self.uris_to_cache:
      uri_cache_string  = self.__CreateValidUriCacheString(self.uris_to_cache)
      cmd += '-files %s ' % (uri_cache_string)  # these get added to the hadoop distributed cache
    
    if libjars_uris:  
      libjars_string = self.__CreateValidUriCacheString(libjars_uris)  
      cmd += '-libjars %s ' % (libjars_string)
      
    cmd += '-D mapred.job.name=%s ' % (job_name)
    cmd += '-D mapred.job.reuse.jvm.num.tasks=10 '  # reuse a JVM at most N times
    cmd += '-D mapred.map.tasks=%d ' % (self.num_map_jobs)
    for k, v in self.parameters.iteritems():
      if not isinstance(k, basestring):
        LOG(FATAL, 'expected a string but got: %s' % k)      
      cmd += '-D %s=%s ' % (k, str(v))      
    cmd += '-program %s ' % (remote_binary_uri) 
    cmd += '-input %s ' % (self.input_path) 
    cmd += '-output %s ' % (self.output_path) 
    cmd += '-reduces %d ' % (self.num_reduce_jobs)

    return cmd 
  
  def Run(self):  
    cmd = self.MakeRunCommand()      
    RunAndGetOutput(cmd, shell=True) # this throws an exception on non-zero return value        
    return True


class MapReduceDriverMaprCache(object):
  """ Runs a pipes MapReduce stage with mapr custom distributed cache. """
  def __init__(self, pipes_binary, input_path, output_path, num_map_jobs, 
               num_reduce_jobs, output_sorted = True, parameters = dict(), 
               uris_to_cache=[], libjars=[]):    
    self.pipes_binary = pipes_binary    
    self.input_path = input_path
    self.output_path = output_path 
    self.num_map_jobs = int(num_map_jobs)
    assert(self.num_map_jobs)
    self.num_reduce_jobs = num_reduce_jobs    
    self.parameters = parameters
    self.libjars = EnsureList(libjars)
     
    # uris_to_cache can be either a single string or a list of strings
    # convert to list format here 
    self.uris_to_cache = EnsureList(uris_to_cache)
    
    for uri in self.uris_to_cache:
      ok, scheme, path, error = py_pert.ParseUri(uri)
      CHECK(ok)
      CHECK_EQ(scheme, 'maprfs')
      
    # set snappy compression for output if none selected
    if not self.parameters.has_key('pert.recordwriter.compression_codec_name'):
      self.parameters['pert.recordwriter.compression_codec_name'] = 'snappy'
    
    # set memcmp comparator if output should be sorted
    if output_sorted:
      if (self.parameters.has_key('pert.recordwriter.comparator_name')):
        logging.fatal('you requested sorted output but already set a comparator name: %s' , self.parameters['pert.recordwriter.comparator_name'])
      self.parameters['pert.recordwriter.comparator_name'] = 'memcmp'
    else:
      self.parameters['pert.recordwriter.comparator_name'] = 'none'
               
    return
 
  
  def MakeRunCommand(self):  
    # check preconditions
    assert(self.input_path)
    assert(self.output_path)               
    assert(self.pipes_binary)
    CHECK_NE(self.num_map_jobs, None)
    CHECK_NE(self.num_reduce_jobs, None)
    
    # TODO(kheath): pipes fails with no helpful error message when scheme prefix
    # is used.  Is this expected? 
    # Workaround for now is to strip off scheme prefix
    scheme_free_input_paths = []
    for orig_input_path in self.input_path.split(','):      
      ok, scheme, path, error = py_pert.ParseUri(orig_input_path)
      CHECK_EQ(scheme, "maprfs")
      CHECK(ok, error)
      scheme_free_input_paths.append(path)
    self.input_path = ','.join(scheme_free_input_paths)
    
    ok, scheme, self.output_path, error = py_pert.ParseUri(self.output_path)
    CHECK(ok, error)
    CHECK_EQ(scheme, "maprfs")
    
    remote_binary = '/data/deluge/bin/%s' % (self.pipes_binary)
    remote_binary_uri = 'maprfs://' + remote_binary
    CopyUri('local://' + self.pipes_binary, remote_binary_uri)
    # if profiler is turned on and a profiler_timeout is enabled, disable the failure redundancy so we get profile result faster 
    if 'profiler' in self.parameters and self.parameters['profiler'] == 'on' and 'profiler_timeout_sec' in self.parameters:
      self.parameters['mapred.map.max.attempts']='1'
      self.parameters['mapred.reduce.max.attempts']='1'
              
    if 'mapred.task.timeout' not in self.parameters:
      self.parameters['mapred.task.timeout']='1200000' #  = 20 min
      
    self.parameters['mapred.map.tasks.speculative.execution']='true'
    self.parameters['mapred.reduce.tasks.speculative.execution']='true'
    self.parameters['mapred.compress.map.output']='true'
    self.parameters['mapred.map.output.compression.codec']='org.apache.hadoop.io.compress.SnappyCodec'
    
    # These are required for c++ code that uses maprfs or hdfs
    
    # TODO(kheath): using this used to work but is broken now... not clear if the call to 'hadoop classpath' happens remotely or locally... as a work around... I manually ran it remotely hard coded it and things work again.  for now... ;-)  
    #self.parameters['mapred.map.child.env'] = 'CLASSPATH=$CLASSPATH:$(hadoop classpath)'
    #self.parameters['mapred.reduce.child.env'] = 'CLASSPATH=$CLASSPATH:$(hadoop classpath)'
    
    classpath_stuff = '/opt/mapr/hadoop/hadoop-0.20.2/bin/../conf:/usr/lib/jvm/java-6-sun/lib/tools.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/..:/opt/mapr/hadoop/hadoop-0.20.2/bin/../hadoop*core*.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/amazon-s3.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/asm-3.2.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/aspectjrt-1.6.5.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/aspectjtools-1.6.5.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/aws-java-sdk-1.3.26.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/commons-cli-1.2.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/commons-codec-1.5.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/commons-configuration-1.8.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/commons-daemon-1.0.1.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/commons-el-1.0.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/commons-httpclient-3.0.1.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/commons-httpclient-3.1.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/commons-lang-2.6.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/commons-logging-1.0.4.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/commons-logging-1.1.1.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/commons-logging-api-1.0.4.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/commons-math-2.1.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/commons-net-1.4.1.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/commons-net-3.1.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/core-3.1.1.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/emr-metrics-1.0.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/eval-0.5.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/gson-1.4.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/guava-13.0.1.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/hadoop-0.20.2-dev-capacity-scheduler.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/hadoop-0.20.2-dev-core.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/hadoop-0.20.2-dev-fairscheduler.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/hsqldb-1.8.0.10.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/httpclient-4.1.1.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/httpcore-4.1.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/jackson-core-asl-1.5.2.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/jackson-mapper-asl-1.5.2.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/jasper-compiler-5.5.12.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/jasper-runtime-5.5.12.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/jersey-core-1.8.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/jersey-json-1.8.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/jersey-server-1.8.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/jets3t-0.6.1.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/jetty-6.1.14.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/jetty-servlet-tester-6.1.14.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/jetty-util-6.1.14.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/junit-4.5.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/kfs-0.2.2.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/log4j-1.2.15.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/logging-0.1.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/maprfs-0.20.2-2.1.2.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/maprfs-jni-0.20.2-2.1.2.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/mockito-all-1.8.2.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/mockito-all-1.8.5.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/mysql-connector-java-5.0.8-bin.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/oro-2.0.8.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/protobuf-java-2.4.1.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/servlet-api-2.5-6.1.14.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/slf4j-api-1.4.3.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/slf4j-log4j12-1.4.3.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/xmlenc-0.52.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/zookeeper-3.3.6.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/jsp-2.1/jsp-2.1.jar:/opt/mapr/hadoop/hadoop-0.20.2/bin/../lib/jsp-2.1/jsp-api-2.1.jar'
    self.parameters['mapred.map.child.env'] = 'CLASSPATH=$CLASSPATH:%s' % (classpath_stuff)
    self.parameters['mapred.reduce.child.env'] = 'CLASSPATH=$CLASSPATH:%s' % (classpath_stuff)
    
    
    binary_name = os.path.basename(self.pipes_binary)
    job_name = '%s_%d' % (binary_name, time.time())
    
    if self.uris_to_cache:
      cache_job = deluge_pb2.MaprDistributedCacheJob()
      cache_job.name = job_name
      cache_job.uris.extend(self.uris_to_cache)
      
      rack_topology = GetRackTopology()
      for rack_topology, ips in rack_topology.iteritems():
        new_rack = cache_job.racks.add() 
        new_rack.topology = rack_topology
        new_rack.member_ips.extend(ips)
    
      self.parameters['mapr_distributed_cache_job'] = Base64EncodeProto(cache_job)
    
    libjars_uris = []
    
    for local_libjar_path in self.libjars:
      remote_jar = '/data/deluge/jar/%s' % (local_libjar_path)
      remote_jar_uri = 'maprfs://' + remote_jar
      print 'uploading jar: %s' % remote_jar_uri
      CopyUri('local://' + local_libjar_path, remote_jar_uri)
      libjars_uris.append(remote_jar_uri)
    
    cmd = 'hadoop pipes '
        
    if libjars_uris:  
      libjars_string = self.__CreateValidUriCacheString(libjars_uris)  
      cmd += '-libjars %s ' % (libjars_string)
      
    cmd += '-D mapred.job.name=%s ' % (job_name)
    cmd += '-D mapred.job.reuse.jvm.num.tasks=10 '  # reuse a JVM at most N times
    cmd += '-D mapred.map.tasks=%d ' % (self.num_map_jobs)
    for k, v in self.parameters.iteritems():
      if not isinstance(k, basestring):
        LOG(FATAL, 'expected a string but got: %s' % k)      
      cmd += '-D %s=%s ' % (k, str(v))      
    cmd += '-program %s ' % (remote_binary_uri) 
    cmd += '-input %s ' % (self.input_path) 
    cmd += '-output %s ' % (self.output_path) 
    cmd += '-reduces %d ' % (self.num_reduce_jobs)

    return cmd 
  
  def Run(self):  
    cmd = self.MakeRunCommand()      
    RunAndGetOutput(cmd, shell=True) # this throws an exception on non-zero return value        
    return True      
