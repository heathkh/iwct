#!/usr/bin/python
import sys
import os

# HACK: On bebo python env is messed up... this makes sure we find the modules
project_root = os.path.abspath(os.path.dirname(__file__) + '/../../../')
sys.path = [project_root, project_root + '/build', project_root + '/build/snap'] + sys.path
#print sys.path

from snap.cirrus import util
from snap.cirrus.workstation import workstation
from snap.pyglog import *
import ConfigParser
import os
import sys



class Cli(object):
  def __init__(self):
    
    self.region = None
    self.aws_id = None
    self.aws_secret = None
    self.instance_type = None
    self.ubuntu_release_name = None
    self.mapr_version = None
    
    self.config_filename = os.path.expanduser('~/.cirrus-workstation')
    self.__LoadConfigFile()
    
    if not workstation.IAMUserReady(self.aws_id, self.aws_secret):
      # try to get root AWS account from AWS default environment variables
      root_aws_id = os.environ.get('AWS_ACCESS_KEY_ID')
      root_aws_secret = os.environ.get('AWS_SECRET_ACCESS_KEY')
      
      # otherwise, ask the user directly
      if not root_aws_id or not root_aws_secret:
        print 'No IAM user is configured, please provide your ROOT aws credentials.'
        root_aws_id = raw_input('ROOT aws key id: ')
        root_aws_secret = raw_input('ROOT aws key secret: ')
        
      CHECK(root_aws_id)  
      CHECK(root_aws_secret)
      self.aws_id, self.aws_secret = workstation.InitCirrusIAMUser(root_aws_id, root_aws_secret)    
    
    valid_regions = ['us-east-1', 'us-west-1']   
    while not self.region:
      print 'No region has been configured, please provide one %s' % (valid_regions)
      region = raw_input('AWS Region: ')
      if region not in valid_regions:
        print 'invalid region: %s' % (region)
      else:
        self.region = region
        
    self.__SaveConfigFile()
    CHECK(workstation.IAMUserReady(self.aws_id, self.aws_secret), 'IAM user not ready... Please fix or delete configuration here: %s' % (self.config_filename))
    self.manager = workstation.Manager(self.region, self.aws_id, self.aws_secret)
    return 
  
  def Debug(self):
    res = self.manager.Debug()
    return
    
  def __LoadConfigFile(self):
    config = ConfigParser.RawConfigParser()
    config.read(self.config_filename)
    try:
      sec = 'credentials'      
      self.aws_id = config.get(sec, 'aws_id') 
      self.aws_secret = config.get(sec, 'aws_secret')
      sec = 'defaults'
      self.region = config.get(sec, 'region')
      self.instance_type = config.get(sec, 'instance_type') 
      self.ubuntu_release_name = config.get(sec, 'ubuntu_release_name')
      self.mapr_version = config.get(sec, 'mapr_version')
      
      # configure defaults
      self.instance_type = 'c1.xlarge'
      self.ubuntu_release_name = 'precise'
      self.mapr_version = 'v2.1.3'
    except:        
      pass
    return
    
  def __SaveConfigFile(self):
    config = ConfigParser.RawConfigParser()
    sec = 'credentials'
    config.add_section(sec)
    config.set(sec, 'aws_id', self.aws_id)
    config.set(sec, 'aws_secret', self.aws_secret)
    sec = 'defaults'
    config.add_section(sec)
    config.set(sec, 'region', self.region)
    config.set(sec, 'instance_type', self.instance_type)
    config.set(sec, 'ubuntu_release_name', self.ubuntu_release_name)
    config.set(sec, 'mapr_version', self.mapr_version)
    f = open(self.config_filename, 'wb')
    config.write(f)
    return
      
  
  def ListWorkstations(self):
    self.manager.ListInstances()    
    return
  
  def ConnectToWorkstation(self):
    self.manager.ListInstances()
    instance_id = raw_input('Which instance id would you like to connect to: ')    
    config_content = self.manager.CreateRemoteSessionConfig(instance_id)    
    config_filename = '/tmp/%s.nxs' % instance_id
    config_file = open(config_filename, 'w')
    config_file.write(config_content)
    config_file.close()
    cmd = 'nxclient --session %s' % config_filename
    util.ExecuteCmd(cmd)
    return

  def StopWorkstation(self):
    self.manager.ListInstances()
    instance_id = raw_input('Which instance id would you like to stop: ')
    self.manager.StopInstance(instance_id)
    return
  
  def DestroyWorkstation(self):
    self.manager.ListInstances()
    instance_id = raw_input('Which instance id would you like to destroy: ')
    confirm = raw_input('If you are sure, enter "yes": ')
    if confirm == 'yes':
      self.manager.TerminateInstance(instance_id)
    return
  
  def CreateWorkstation(self):
    workstation_name = raw_input('Unique name to give new workstation: ')
    self.manager.CreateInstance(workstation_name, self.instance_type, self.ubuntu_release_name, self.mapr_version)
    return
  
  def ResizeWorkstationRootVolume(self):
    self.manager.ListInstances()
    instance_id = raw_input('Which instance id would you like to modify: ')
    # TODO get the current size and show user
    vol_size_gb = long(raw_input('Desired size for resized volume (in GB): '))    
    self.manager.ResizeRootVolumeOfInstance(instance_id, vol_size_gb)    
    return
  
  def AddVolumeToWorkstation(self):
    self.manager.ListInstances()
    instance_id = raw_input('To which instance id would you like to add volume: ')
    vol_size_gb = long(raw_input('Desired size for new volume (in GB): '))    
    self.manager.AddNewVolumeToInstance(instance_id, vol_size_gb)    
    return
  
  def ConfigureWorkstation(self):
    self.manager.ListInstances()
    instance_id = raw_input('Which instance id would you like to configure: ')
    self.manager.ConfigureWorkstation(instance_id)        
    return

def main():
  cli = Cli()  
  
  if (len(sys.argv) < 2):    
    print 'Usage: commands are: list, connect, stop, create, configure, add_volume' 
    return 1
  cmd = sys.argv[1]  
  
  if cmd == 'debug':
    cli.Debug()
  elif cmd == 'list':
    cli.ListWorkstations()
  elif cmd == 'connect':
    cli.ConnectToWorkstation()
  elif cmd == 'stop':
    cli.StopWorkstation()
  elif cmd == 'create':
    cli.CreateWorkstation()
  elif cmd == 'configure':
    cli.ConfigureWorkstation()
  elif cmd == 'add_volume':
    cli.AddVolumeToWorkstation()
  elif cmd == 'resize':      
    cli.ResizeWorkstationRootVolume()  
  elif cmd == 'destroy':      
    cli.DestroyWorkstation()
  else:
    print 'unknown operation requested: ' + cmd    
  return
  

if __name__ == "__main__":
  main()




