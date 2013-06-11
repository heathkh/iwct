import os

class CirrusConfig(object):
  """
  Cirrus Configuration for a cluster and associated dev workstations.
  """  
  def __init__(self):
    super(CirrusConfig, self).__init__()
    self.cluster_name = 'cirrus_cluster_01'
    self.region_name = 'us-east-1'
    self.prefered_availability_zone = self.region_name + 'b'
    self.ubuntu_release_name = 'precise'
    self.mapr_ami_owner_id = None # Default is to use standard ami.  You can set this to your own AWS user account id (only available from AWS management console) to use custom AMI versions you created with ami_cli.py
    
    
    # cluster params
    self.cluster_instance_type = 'c1.xlarge'        
    #self.cluster_instance_type = 'cc2.8xlarge'        
    #self.cluster_instance_type = 'cc1.4xlarge'
    self.mapr_version = 'v2.1.3'
    self.zones = ['a'] # list like this ['a','c','e']
    return
  
def GetConfiguration():
  conf = CirrusConfig()   
  return conf 

