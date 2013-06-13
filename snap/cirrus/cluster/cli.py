#!/usr/bin/python
import sys
from snap.cirrus.cluster import mapr
from snap.cirrus.cluster import config
from snap.pyglog import *

def main(argv):
  
  if (len(sys.argv) < 2):    
    print 'Usage:'
    print ' urls - Print the urls Web UI'
    print ' create <num_instances> - Launch a cluster with N nodes'
    print ' resize <num_instance> - Resize a cluster to have N nodes'
    print ' destroy <num_instance> - Shutdown all nodes (warning: all data on maprfs:// is destroyed)'
    print ' see source for additional experimental commands...'     
    return 1
  cmd = sys.argv[1]  
  cluster = mapr.MaprCluster(config.GetConfiguration())
  
  if cmd == 'urls':    
    cluster.ShowUiUrls()  
  elif cmd == 'create':
    assert(len(sys.argv) == 3)    
    num_instances = long(sys.argv[2])
    cluster.Create(num_instances)
    cluster.ShowUiUrls()
  elif cmd == 'resize':    
    assert(len(sys.argv) == 3)    
    num_instances = long(sys.argv[2])
    cluster.Resize(num_instances)
    cluster.ShowUiUrls()
  elif cmd == 'destroy':
    cluster.Destroy()
  # Experimental commands  
  elif cmd == 'push_config':
    cluster.PushConfig()      
  elif cmd == 'reset':
    cluster.Reset()  
  elif cmd == 'config_client':
    cluster.ConfigureClient()  
  elif cmd == 'config_lazy':
    cluster.ConfigureLazyWorkers()  
  elif cmd == 'debug':
    cluster.Debug()        
  elif cmd == 'get_property':
    assert(len(sys.argv) == 3)    
    property_name = sys.argv[2]
    print cluster.GetProperty(property_name)      
  elif cmd == 'set_map_reduce_slots_per_node':
    # since hadoop 20.2 has no working capacity scheduler, this hack allows manual reconfiguration of slots per node to indirectly enforce resource guarantees
    num_slots_map = long(sys.argv[2])
    num_slots_reduce = long(sys.argv[3])
    CHECK(cluster.SetNumMapReduceSlotsPerNode(num_slots_map, num_slots_reduce))    
  else:
    print 'unknown operation requested: ' + cmd    
    return 1
  return 0

if __name__ == "__main__":
  main(sys.argv)




