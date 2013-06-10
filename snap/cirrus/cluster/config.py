import os

class CirrusConfig(object):
  """
  Cirrus Configuration for a cluster and associated dev workstations.
  """  
  def __init__(self):
    super(CirrusConfig, self).__init__()
    self.region_name = 'us-east-1'
    self.prefered_availability_zone = self.region_name + 'b'
    self.ubuntu_release_name = 'precise'
    
    # cluster params
    self.cluster_instance_type = 'cc2.8xlarge'        
    self.mapr_version = 'v2.1.3'
    self.zones = ['b'] # list like this ['a','c','e']
    return
  
def GetConfiguration():
  conf = CirrusConfig()   
  return conf 

