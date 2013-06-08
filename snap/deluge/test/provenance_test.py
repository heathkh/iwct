#!/usr/bin/python

from snap.pyglog import *
from snap.deluge import provenance
from snap.deluge import core


def test_PrintResourceProvenanceList():
  #uri = 'maprfs://data/itergraph/tide_v12//sift//cbir/3cf541188f15713d6ebb2b9ff6badb8e//itergraph/0142f574ff8a1d0f2deaabb1622e84f8//phase002//merged_matches.pert'
  uri = 'maprfs://data/itergraph/tide_v12/sift/cbir/3cf541188f15713d6ebb2b9ff6badb8e/query_results/shard_00000.pert'
  #uri = "maprfs://data/itergraph/tide_v12/sift/feature_counts.pert"
  #uri = "maprfs://data/itergraph/tide_v12/sift/features.pert"
  #uri = 'maprfs://data/itergraph/tide_v12/cropped_scaled_photoid_to_image.pert'
  
  fingerprint = core.GetResourceFingerprintFromUri(uri)
  provenance.GetResourceTiming(fingerprint)
      
  return
        
if __name__ == "__main__":  
  test_PrintResourceProvenanceList()
  

  
    
  
