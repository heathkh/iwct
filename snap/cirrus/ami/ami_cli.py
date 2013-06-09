#!/usr/bin/python

from snap.cirrus.ami import manager
from snap.boto.ec2.connection import EC2Connection
from snap.cirrus import util
import os

def main():
  instance_type = 'c1.xlarge'
  role = 'workstation'
  region_name = 'us-east-1'
  ubuntu_release_name = 'precise'
  mapr_version = 'v2.1.3'
  ec2 = EC2Connection(region = util.GetRegion(region_name))
  ami_spec = manager.AmiSpecification(region_name, instance_type, ubuntu_release_name, mapr_version, role)
  
  keypair_name = 'cirrus_ami_maker'
  key_dir_path = os.path.expanduser('~/ec2/')
  if not os.path.exists(key_dir_path):
    os.mkdir(key_dir_path)
  
  private_key_filename = '%s/%s.pem' % (key_dir_path, keypair_name)
  try:
    keypair = ec2.create_key_pair(keypair_name)
    keypair.save(key_dir_path)
  except:
    pass
  
  ssh_key = open(private_key_filename, 'r').read()
  
  ami_maker = manager.AmiMaker(ec2, ami_spec, keypair_name, ssh_key)  
  ami_maker.Run()
    
    #TODO(heathkh): Copy the ami to all other regions and set the required permissions and tags as well
  return

if __name__ == "__main__":
  main()


