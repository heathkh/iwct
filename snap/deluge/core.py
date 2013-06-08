#!/bin/python

import os
from snap.pert import py_pert 
from snap.deluge import mr
from snap.deluge import deluge_pb2
from snap.pyglog import *
import time
import cPickle as pickle
from snap.deluge import provenance

def GetResourceFingerprintFromUri(uri):
  tmp_flow = Flow()  
  resource = PertResource(tmp_flow, uri)
  fingerprint = resource.GetFingerprint()
  return fingerprint

def CheckUriExistsOrDie(uri):
  CHECK(py_pert.Exists(uri), 'expected uri to exist: %s' % uri)
  return


# this only reports the chunk size for future files that will be create
def GetChunkSizeForUri(uri):
  CHECK(py_pert.Exists(uri), 'expected uri to exist: %s' % uri)
  CHECK(py_pert.IsDirectory(uri), 'Chunk size only defined for directories... See mapr docs for details')
  nfs_path = mr.UriToNfsPath(uri)
  dfs_attribute_path = '%s/.dfs_attributes' % (nfs_path)
  lines = open(dfs_attribute_path, 'r').readlines()
  #print lines
  tokens = lines[2].split('=')
  CHECK_EQ(tokens[0], 'ChunkSize')
  chunksize = long(tokens[1])  
  return chunksize


# this only changes the settings for future files created in a directory
def SetChunkSizeForUri(uri, block_size):
  CHECK_EQ(block_size % (2**16), 0) # must be a multiple of 2**16
  CHECK_LE(block_size, 1024*(2**20), 'Currently libmaprfs has a limitation that prevents chunk sizes greater than 1GB.')
  CHECK(py_pert.Exists(uri), 'expected uri to exist: %s' % uri)
  CHECK(py_pert.IsDirectory(uri), 'Chunk size only defined for directories... See mapr docs for details')
  nfs_path = mr.UriToNfsPath(uri)
  dfs_attribute_path = '%s/.dfs_attributes' % (nfs_path)
  control_file = open(dfs_attribute_path, 'w')
  control_file.write('# lines beginning with # are treated as comments\nCompression=true\nChunkSize=%d' % (block_size))
  control_file.close()
  new_block_size = GetChunkSizeForUri(uri)
  CHECK_EQ(new_block_size, block_size)
  return True 


def EnsureChunkSizeForUri(uri, desired_block_size):
  CHECK_EQ(desired_block_size % (2**16), 0) # must be a multiple of 2**16
  CHECK(py_pert.Exists(uri), 'expected uri to exist: %s' % uri)
  CHECK(py_pert.IsFile(uri), 'expected uri to be a file: %s' % uri)
  ok, actual_chunk_size = py_pert.ChunkSize(uri)
  CHECK(ok)
  CHECK_EQ(desired_block_size, actual_chunk_size, 'Expected chunk size of %d but actual chunk size is %d for uri: %s' % (desired_block_size, actual_chunk_size, uri))  
  return True
  

class Resource(object):
  """ Simple uri file resource. """
  def __init__(self, flow, uri, check_exists = False, is_generated = True):    
    self.flow = flow
    if (isinstance(uri, unicode)):
      uri = uri.encode('ascii','ignore')
    
    CHECK(isinstance(uri, str), "expected a string but got: %s" % (uri.__class__.__name__))
    self.uri = uri
    self.is_generated = is_generated
    if check_exists:
      self.CheckExists()    
    return
  
  def GetFlow(self):
    return self.flow
  
  def GetUri(self):    
    return self.uri
  
  def Exists(self):
    return py_pert.Exists(self.uri)
  
  def CheckExists(self):    
    CHECK(self.Exists(), 'expected resource to exist: %s' % self.uri)
    return
    
  def GetLastWriteTime(self):
    return self.LookupLastWriteTime()
    
  def LookupLastWriteTime(self):    
    CHECK(False) # this is an abstract class can't use it directly!
    
  def GetFingerprint(self):    
    CHECK(False) # this is an abstract class can't use it directly!  
  
  def __repr__(self):
    return '<Resource uri=%s>' % (self.uri)
  
  

class FileResource(Resource):
  """ Simple uri file resource. """
  def __init__(self, flow, uri, check_exists = False, is_generated = True):
    super(FileResource,self).__init__(flow, uri, check_exists, is_generated)    
    CHECK(isinstance(uri, str), 'expected str but got: %s' % type(uri))
    return
  
  def GetFlow(self):
    return self.flow
  
  def GetUri(self):    
    return self.uri
  
  def Exists(self):
    return py_pert.Exists(self.uri)
    
  def LookupLastWriteTime(self):
    CHECK(py_pert.IsFile(self.uri), 'uri: %s' % self.uri) # this notion of a resource write time only makes sense for files not dirs!
    ok, creation_time = py_pert.ModifiedTimestamp(self.uri)
    CHECK(ok)
    return creation_time
  
  def GetFingerprint(self):
    fingerprint = None        
    self.CheckExists()    
    ok, fingerprint = py_pert.ComputeMd5DigestForUri(self.uri)
    CHECK(ok)    
    CHECK(fingerprint)
    return fingerprint
  
  def __repr__(self):
    return '<Resource uri=%s>' % (self.uri)
  
    
class DirectoryResource(Resource):
  """ Simple uri dir resource. """
  def __init__(self, flow, uri, check_exists = False, is_generated = True):    
    super(DirectoryResource,self).__init__(flow, uri, check_exists, is_generated)
    return
    
  def LookupLastWriteTime(self):
    CHECK(py_pert.IsDirectory(self.uri))
    ok, mod_time = py_pert.ModifiedTimestamp(self.uri)
    CHECK(ok)
    return mod_time
    
  def GetFingerprint(self):
    self.CheckExists()
    fingerprint = None    
    # get list all the uris in the directory 
    ok, uris = py_pert.ListDirectory(self.uri)
    CHECK(ok)    
    CHECK(uris)
    ok, fingerprint = py_pert.ComputeMd5DigestForUriSet(uris)
    CHECK(ok)
    CHECK(fingerprint)
    return fingerprint
  
  def __repr__(self):
    return '<DirectoryResource uri=%s>' % (self.uri)


class PertResource(Resource):
  """ PERT resource (directory of part files). """  
  def __init__(self, flow, uri, check_exists = False, is_generated = True):
    super(PertResource,self).__init__(flow, uri, check_exists, is_generated)    
    return
    
  def GetNumShards(self):
    CHECK(self.Exists())
    return py_pert.GetNumShards(self.uri)
  
  def Exists(self):
    """ Returns true if the uri exists and contains some part files. """
    # To solve the problem of detecting the PERT as existing when some of the
    # shards are not yet there needs to be addressed.  A general solution would
    # indicate how many part files are expected either in the part filename or 
    # inside the file payload. 
    # A less general, but simpler change is to handle the case when the output
    # is being put in a directory by the MR framework which creates a "flag" 
    # file names _SUCCESS when all part files are deposited.     
    if not py_pert.Exists(self.uri):
      return False    
    # Check if we are in a directory being generated by mr and check for _SUCCESS file
    resource_generated_by_mr = py_pert.Exists(self.uri + '/_logs') 
    if resource_generated_by_mr:
      #LOG(INFO, 'pert uses mr type exist rules: %s' % self.uri)
      mr_resource_done = py_pert.Exists(self.uri + '/_SUCCESS')
      if not mr_resource_done:
        return False    
    # Otherwise, check that shards are there and there are none missing between 
    # largest and smallest part id
    # TODO(heathkh): We can still have an error where the part with largest ids 
    # are missing and we can't tell because the filenames don't indicate how
    # many are in the set in total      
    shards = py_pert.GetShardUris(self.uri)
    if not shards or not py_pert.ShardSetIsValid(shards):
      return False    
    return True
    
  def LookupLastWriteTime(self):
    CHECK(py_pert.IsDirectory(self.uri), 'expected dir: %s' % (self.uri)) # this notion of a resource write time only make sense for dirs
    ok, dir_mod_time = py_pert.ModifiedTimestamp(self.uri)
    CHECK(ok)    
    # This is a sanity check to verify the assumption that the dir mod time is at least as new as the most recent content mod time
    # Disable when you are confident this assumption is true
#    latest_shard_mod_time = 0
#    for shard_uri in py_pert.GetShardUris(self.uri):
#      CHECK(py_pert.IsValidUri(shard_uri), 'Invalid uri: %s' % (shard_uri))      
#      ok, shard_mod_time = py_pert.ModifiedTimestamp(shard_uri)
#      CHECK(ok)
#      latest_shard_mod_time = max(shard_mod_time, latest_shard_mod_time)    
#    CHECK_GE(dir_mod_time, latest_shard_mod_time)    
    return dir_mod_time
  
  def GetFingerprint(self):
    fingerprint = None
    self.CheckExists()    
    ok, fingerprint = py_pert.GetShardSetFingerprint(self.uri) # this is fast because it uses the precomputed hash of each shard
    CHECK(ok)        
    #CHECK_EQ(len(fingerprint),32, 'failed to get valid fingerprint for: %s' % (self.uri))
    
    # this is for backwards compatibility for reading pert files before fingerprints were embeded
    
    if len(fingerprint) != 32:
      uris = py_pert.GetShardUris(self.uri)
      ok, fingerprint = py_pert.ComputeMd5DigestForUriSet(uris)
      CHECK(ok)
    CHECK_EQ(len(fingerprint),32, 'failed to get valid fingerprint for: %s' % (self.uri))    
    return fingerprint
    
  def __repr__(self):
    return '<PertResource uri=%s>' % (self.uri)
   
    
class Flow(object):
  def __init__(self):
    print 'Initializing flow: %s' % self
    self.inputs = {}
    self.outputs = {}
    self.dependent_flows = set()     
    self.reserve_all_resources = False  # used by scheduler
    self.is_done = None # used to cache done state to prevent recompute
    return
  
  def AddDependentFlow(self, dep_flow):
    self.dependent_flows.add(dep_flow)
    return
  
  def AddInput(self, name, resource):
    resource.GetFlow().AddDependentFlow(self)
    CHECK(not self.inputs.has_key(name))
    self.inputs[name] = resource
    
    CHECK_NE(resource.flow, self, 'A flow can not create a resource that is also an input to the same flow.')
    return
  
  def AddOutput(self, name, resource):    
    CHECK(not self.outputs.has_key(name)) # the name of outputs must be unique (at least within scope of the flow)
    if resource.flow != self:
      LOG(FATAL, "This flow can not claim this resource as an output because it is created by a different flow: %s" % resource)  
    self.outputs[name] = resource
    return
  
  def GetOutput(self, name=None):
    out = None
    if name == None and len(self.outputs) == 1:
      out = self.outputs.values()[0]
    else:
      CHECK(self.outputs.has_key(name), 'This flow (%s) does not have output with requested name (%s). Did you mean one of these: %s'  % (self, name, str(self.outputs.keys())))
      out = self.outputs[name]      
    return out
  
  def GetInput(self, name=None):
    requested_input = None
    if name == None:
      CHECK(len(self.inputs) == 1, 'This flow (%s) has multiple inputs.  Please provide one of these as an argument: %s'  % (self,  str(self.inputs.keys())))
      requested_input = self.inputs.values()[0]
    else:
      CHECK(self.inputs.has_key(name), 'This flow (%s) does not have an input with requested name (%s). Did you mean one of these: %s'  % (self, name, str(self.inputs.keys())))
      requested_input = self.inputs[name]      
    return requested_input
  
  def IsDone(self):
    """ A Flow is done if all it's output resources exist are not older than any of it's input resources. """    
    if self.is_done:
      return True    
    self.is_done = self.ComputeIsDone()    
    return self.is_done 
  
  def ComputeIsDone(self):
    """ A Flow is done if all it's output resources exist and are not older than any of it's input resources. """
    #LOG(INFO, 'computing if flow is done: %s' % self)
    for output in self.outputs.itervalues():
      if not output.Exists():
        #print "flow %s is missing output %s" % (str(self), output) 
        return False  
    #return True    
    def GetSetLastWriteTime(items):    
      newest_time = 0
      for index, item in enumerate(items):
        if not item.Exists():
          continue
        item_time = item.GetLastWriteTime()
        if item_time > newest_time:
          newest_time = item_time
      return newest_time        
    newest_input_time = GetSetLastWriteTime(self.inputs.itervalues())    
    newest_output_time = GetSetLastWriteTime(self.outputs.itervalues())
    return  (newest_output_time >= newest_input_time)
  
  
  def Execute(self):
    """ Runs the flow and records provenance metadata for all outputs."""
    # to be called only by scheduler    
    # get fingerprints of all inputs
    input_fingerprints = []
    for input_name, input_resource in self.inputs.iteritems():
      fingerprint = input_resource.GetFingerprint()
      input_fingerprints.append(fingerprint)      
      if input_resource.is_generated:      
        record = provenance.GetResourceProvenanceRecord(fingerprint)
        if record == None:
          LOG(INFO, 'failed to find record for fingerprint: %s' % (fingerprint))
          LOG(INFO, 'uri: %s' % (input_resource.GetUri()))
          LOG(INFO, 'You are missing provenance record for a generated resource: %s.  You probably terminated a flow... but the MR was not stopped. The resource appears to be there, but has no "finish time" record.  You probably want to delete it so it gets recreated along with proper metadata.' % (input_resource))
          delete_file = raw_input()
          if delete_file == 'yes':
            py_pert.Remove(input_resource.GetUri())            
            LOG(FATAL, 'please restart the flow...')
            
    start_time_sec = time.time() 
    self.Run()
    end_time_sec = time.time()    
    for output_name, output_resource in self.outputs.iteritems():      
      if output_resource.flow != self:
        LOG(INFO, "I don't own this output... not creating provenance record for it: %s" % output_resource) # Only the flow that created the output should generate provenance record for it... otherwise later flows may overwrite the provenance info and cause a loss of info about the intermediate steps.
        continue
      
      output_fingerprint = output_resource.GetFingerprint()      
      record = deluge_pb2.ResourceProvenance()
      record.fingerprint = output_fingerprint
      record.flow = self.__class__.__name__
      record.name = output_name
      record.uri = output_resource.GetUri()
      record.start_time_sec = start_time_sec
      record.end_time_sec = end_time_sec
      record.input_fingerprints.extend(input_fingerprints)
      CHECK(record.IsInitialized())
      provenance.SetResourceProvenanceRecord(record)      
    return
  
  def PreRunConfig(self):
    """ Called after all inputs are fresh but before run.  Give a chance for run to compute configuration params based on input files. """
    return
    
  def Run(self):
    LOG(FATAL, 'Must define Run() method in dervied class: %s' % str(self))    
    return
  
  def __repr__(self):
    return '%s - %s' % (self.__class__.__name__, hash(self))

     
class PipesFlow(Flow):
  """ A flow for a Hadoop Pipes MapReduce job.""" 
  def __init__(self):
    super(PipesFlow, self).__init__()        
    # must set this in derived class
    self.pipes_binary = None              
    # optional to overide these values in derived class
    self.num_map_jobs = None
    self.num_reduce_jobs = None
    self.desired_splits_per_map_slot = 4 # set this low if overhead of starting new map job is high 
    self.output_is_sorted = True
    self.parameters = {}  
    self.input_path = None
    self.output_path = None
    self.output_chunk_size_bytes = None
    self.libjars = []
#    self.cpus_per_node = mr.GetNumCoresPerTaskTracker()
#    self.map_slots_per_node = max(1, int(self.cpus_per_node*0.95)) 
#    self.reduce_slots_per_node = max(1, int(self.cpus_per_node*0.90))
    self.cpus_per_node = None
    self.map_slots_per_node = None 
    self.reduce_slots_per_node = None

    self.cached_resources = {}
    return
    
  def AddInput(self, name, resource, is_primary = False, add_to_cache = False):
    super(PipesFlow, self).AddInput(name, resource)    
    if is_primary:
      CHECK_EQ(self.input_path, None, 'Another uri was already specified as the primary uri for the map reduce flow.')
      self.input_path = resource.GetUri()    
    if add_to_cache:
      self.__AddToCache(name, resource)    
    return
  
  def __AddToCache(self, name, resource):
    self.cached_resources[name] = resource
    return
  
  def AddParam(self, param_name, param_value):
    if param_name in self.parameters:
      LOG(FATAL, 'The parameter %s has already been set to: %s' % (param_name, self.parameters[param_name]))
    # if value is a protobuf, encode it peroperly using base64 encoding to be safe on hadoop command line    
    encoded_param_value = None
    if callable(getattr(param_value, "SerializeToString", None)):
      CHECK(param_value.IsInitialized())
      encoded_param_value = mr.Base64EncodeProto(param_value)
    elif isinstance(param_value, basestring):
      encoded_param_value = param_value      
    elif isinstance(param_value, bool): # right now we use Int to represent bools on c++ side...
      if param_value:
        encoded_param_value = '1'
      else:
        encoded_param_value = '0'
    elif isinstance(param_value, (int, long, float)):
      encoded_param_value = str(param_value)      
    CHECK(encoded_param_value != None)
    self.parameters[param_name] = encoded_param_value
    return
    
  def SetPipesBinary(self, my_file, name):        
    self.pipes_binary = '%s/%s' % (os.path.dirname(my_file), name)
    return 
  
  def SetReduceGroupKeyPrefixSize(self, prefix_len):
    # This is a hack that abuses the KeyFieldBasedPartitioner.java class to 
    # create a custom group function that looks at only a prefix of the full
    # key of a given length
    self.parameters['mapred.output.value.groupfn.class'] = 'MyReducerPrefixGroupingComparator'
    self.parameters['ReduceGroupKeyPrefixSize'] = prefix_len
    pertsupportjar_path = os.path.dirname(__file__) + '/java/pert.jar'
    self.libjars.append(pertsupportjar_path)
    return

  def MakeDriver(self):
    print 'making driver for pipes flow: %s' % (self.pipes_binary)        
    self.PreRunConfig()
    CHECK(self.pipes_binary, "Flow class must set self.pipes_binary")       
    # if input_path not set, try to set it automatically
    if not self.input_path:
      # can do this if there is only one input
      if len(self.inputs) == 1:
        self.input_path = self.GetInput().GetUri()
      # otherwise tell user how to fix the error
      else:
        if len(self.inputs) == 0:
          self.input_path = self.GetInput().GetUri()
          LOG(FATAL, 'This flow has no inputs... Please add one. : %s' % self)          
        elif len(self.inputs) > 1:
          LOG(FATAL, 'This flow has multiple inputs... please set self.input_path in __init__() : %s' % self)              
    if not self.output_path:
      self.output_path = self.GetOutput().GetUri()    
    CHECK(self.input_path)
    CHECK(self.output_path)    
    
    # reconfigure number of slots per node, if needed    
    if not self.map_slots_per_node: 
      self.cpus_per_node = mr.GetNumCoresPerTaskTracker()
      self.map_slots_per_node = max(1, int(self.cpus_per_node*0.95))
       
    if not self.reduce_slots_per_node: 
      self.cpus_per_node = mr.GetNumCoresPerTaskTracker()
      self.reduce_slots_per_node = max(1, int(self.cpus_per_node*0.90))
    
    LOG(INFO, 'map_slots_per_node: %s' % (self.map_slots_per_node))
    LOG(INFO, 'reduce_slots_per_node: %s' % (self.reduce_slots_per_node))    
    mr.SetNumSlotsPerNode(self.map_slots_per_node, self.reduce_slots_per_node)    
    if self.num_map_jobs == None:
      self.num_map_jobs = mr.ComputeNumSplits(self.input_path,
                                              self.desired_splits_per_map_slot)    
    if self.num_reduce_jobs == None:
      self.num_reduce_jobs = mr.GetNumReduceSlots()    
    uris_to_be_cached = [] 
    for name, resource in self.cached_resources.iteritems():
      uri = resource.GetUri()
      uris_to_be_cached.append(uri)      
      # add a parameter with special name so that c++ helper class can find cache resource via friendly name
      self.parameters['deluge_cache_%s' % (name)] = uri      
    mr_driver = mr.MapReduceDriverMaprCache(self.pipes_binary, 
                                   self.input_path,
                                   self.output_path,                                    
                                   self.num_map_jobs, 
                                   self.num_reduce_jobs,
                                   self.output_is_sorted,
                                   self.parameters, 
                                   uris_to_be_cached, 
                                   self.libjars)    
    return  mr_driver
  
  def Run(self):
    print 'about to run pipes flow: %s' % (self.pipes_binary)
    mr_driver = self.MakeDriver()    
    # set output directory property to create files with required chunk size
    if self.output_chunk_size_bytes != None:
      if not py_pert.Exists(self.output_path):
        nfs_path = mr.UriToNfsPath(self.output_path)
        os.makedirs(nfs_path)
      SetChunkSizeForUri(self.output_path, self.output_chunk_size_bytes)    
      CHECK_EQ(GetChunkSizeForUri(self.output_path), self.output_chunk_size_bytes ) # verify the features file will have a block size of 4 GB
    status = mr_driver.Run()
    # ensure output was created with the required chunk size
    if self.output_chunk_size_bytes != None:
      # ensure the created output has the requested chunk size
      for uri in py_pert.GetShardUris(self.output_path):
        EnsureChunkSizeForUri(uri, self.output_chunk_size_bytes)
    return status  
      
  def GetHadoopRunCommand(self):
    """ This is currently needed to support the parallel execution with 
    multiprocessing pool because a flow often contains a protobuf and protobufs 2.4.1
    can not be pickled."""
    mr_driver = self.MakeDriver()
    return mr_driver.MakeRunCommand()  


class MapJoinPipesFlow(PipesFlow):
  def __init__(self):
    super(MapJoinPipesFlow, self).__init__()            
    # must set this in derived class
    self.primary_input_uri = None
    self.secondary_input_uri = None                     
    return
  
  def Run(self):
    CHECK(self.primary_input_uri)
    CHECK(self.secondary_input_uri)    
    # check that primary and secondary inputs are joinable
    CheckUriExistsOrDie(self.primary_input_uri)
    CheckUriExistsOrDie(self.secondary_input_uri)    
    num_shards_primary = py_pert.GetNumShards(self.primary_input_uri)
    num_shards_secondary = py_pert.GetNumShards(self.secondary_input_uri)    
    if not num_shards_primary == num_shards_secondary:      
      LOG(INFO, 'primary input: %d (%s)' % (num_shards_primary, self.primary_input_uri) )
      LOG(INFO, 'secondary input: %d (%s)' % (num_shards_secondary, self.secondary_input_uri))
      LOG(FATAL, 'join inputs are not compatible')
      
    # Set properties required by map join
    self.parameters['join.primary.input.uri'] = self.primary_input_uri
    self.parameters['join.secondary.input.uri'] = self.secondary_input_uri
    self.parameters['join.type'] = 'restricted_inner_join'      
    self.input_path = self.primary_input_uri
    return super(MapJoinPipesFlow, self).Run()   
    
       