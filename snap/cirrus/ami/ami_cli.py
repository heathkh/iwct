#!/usr/bin/python

from snap.cirrus.ami import manager

def main():
  instance_type = 'c1.xlarge'
  role = 'workstation'
  region_name = 'us-east-1'
  ubuntu_release_name = 'precise'
  mapr_version = 'v2.1.3'
  ami_spec = manager.AmiSpecification(region_name, instance_type, ubuntu_release_name, mapr_version, role)
  with util.TemporaryKeyPair() as keypair:
    ami_maker = manager.AmiMaker(ami_spec, keypair)  
    ami_maker.Run()
  return

if __name__ == "__main__":
  main()


