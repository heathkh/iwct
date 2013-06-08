#!/usr/bin/python

from deluge import core
from deluge import mr
from pyglog import *
from pert import py_pert
from snap.google.base import py_base
from progressbar import Bar, Percentage, ETA, ProgressBar
from iw.flow.util import util
from iw.flow.util import image_matcher_config
from iw import iw_pb2
from tide import tide_pb2

import cPickle as pickle

def ParseUint64Key(key):
  CHECK_EQ(len(key), 8)
  return py_base.KeyToUint64(key)
 

def ParseUint64KeyPair(key):
  CHECK_EQ(len(key), 16)
  id_a = py_base.KeyToUint64(key[0:8])
  id_b = py_base.KeyToUint64(key[8:16])
  return id_a, id_b


def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

class MainFlow(core.Flow):
  def __init__(self, root_uri):
    super(MainFlow,self).__init__()
    
    feature_type = 'usift'
    max_batch_size = 50
    
    matcher_config = image_matcher_config.GetImageMatcherConfig(feature_type)
    num_shards_features = 100
    
    images = core.PertResource(self, '%s/photoid_to_image.pert' % (root_uri))    
    CHECK(images.Exists(), 'flow expects input to exist: %s' % images)    
    #tide = core.PertResource(self, '%s/objectid_to_object.pert' % (root_uri))    
    #CHECK(tide.Exists(), 'flow expects input to exist: %s' % tide)
    
    # crop out borders
    crop_flow = util.CropAndScaleImagesFlow(root_uri, images, 1.0)
    
    # extract features
    feature_uri_base = "%s/%s/" % (root_uri, feature_type)
    feature_flow = util.ExtractFeaturesFlow(feature_uri_base, crop_flow.GetOutput(), feature_type, num_shards_features)
    create_allpairs_metadata_flow = CreateAllPairsMatchBatchesFlow(feature_uri_base, images, max_batch_size, num_shards_features)
    sorted_metadata_flow = util.SortFlow(create_allpairs_metadata_flow.GetOutput(), '%s/match_batches.pert' % feature_uri_base, num_shards_features)
    
    # build graph     
    matching_flow = util.MatchBatchesFlow( feature_uri_base, feature_flow.GetOutput(), sorted_metadata_flow.GetOutput(), matcher_config)
    
    return

  
class CreateAllPairsMatchBatchesFlow(core.Flow):
  def __init__(self, base_uri, images, max_batch_size, num_shards_features):
    super(CreateAllPairsMatchBatchesFlow,self).__init__()
    
    self.AddInput('images', images)
    self.AddOutput('unsorted_match_batches', core.PertResource(self, '%s/unsorted_match_batches.pert' % (base_uri)))
    self.max_batch_size = max_batch_size
    self.num_shards_features = num_shards_features
    return
  
  
  def Run(self):
    reader = py_pert.StringTableShardSetReader()
    reader.Open(self.GetInput('images').GetUri())
    image_ids = []
    for i, (k, v) in enumerate(reader):
      image_ids.append(ParseUint64Key(k))
    
    LOG(INFO, 'creating match groups')
    match_groups = []  # a list of tuples (primary_id, secondary id list)
    widgets = [Percentage(), ' ', Bar(), ' ', ETA()]
    pbar = ProgressBar(widgets=widgets, maxval=len(image_ids)).start()
    for i in range(len(image_ids)):
      primary_id = image_ids[i]
      secondary_ids = list(image_ids)
      secondary_ids.remove(primary_id)
      for secondary_id_chunk in chunks(secondary_ids, self.max_batch_size):
        match_groups.append((primary_id, secondary_id_chunk))
      pbar.update(i)
      
    # write out the match plan (must be later sorted by key for future join stage)    
    writer = py_pert.StringTableWriter()
    options = py_pert.WriterOptions()
    options.SetUnsorted()
    LOG(INFO, 'writing match groups')
    CHECK(writer.Open(self.GetOutput('unsorted_match_batches').GetUri(), 1, options))
    metadata = iw_pb2.MatchBatchMetadata()        
    pbar = ProgressBar(widgets=widgets, maxval=len(match_groups)).start()
    for batch_id, (batch_primary_image, batch_image_ids) in enumerate(match_groups):
      if len(batch_image_ids) == 0:
        continue
      
      batch_name = py_base.Uint64ToKey(batch_id)
      metadata = iw_pb2.MatchBatchMetadata()
      metadata.image_id = batch_primary_image
      metadata.batch_name = batch_name
      metadata.is_primary = True
      writer.Add(py_base.Uint64ToKey(metadata.image_id), metadata.SerializeToString())
          
      for image_id in batch_image_ids:
        metadata.image_id = image_id
        metadata.batch_name = batch_name
        metadata.is_primary = False
        writer.Add(py_base.Uint64ToKey(metadata.image_id), metadata.SerializeToString())
      
      pbar.update(batch_id)
    
    return


    