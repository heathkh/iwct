from snap.pyglog import *
from snap.cirrus import util
from string import Template
from snap.cirrus.ami import manager
import time
import hashlib
from snap import boto
from snap.boto.iam.connection import IAMConnection
from snap.boto.ec2.connection import EC2Connection
from snap.boto.s3.connection import S3Connection
from snap.boto.exception import  BotoServerError
from snap.boto.s3.key import Key

workstation_instance_profile_name = 'cirrus_workstation_profile'

def IsHPCInstanceType(instance_type): 
  hpc_instance_types = ['cc1.4xlarge', 'cc2.8xlarge', 'cr1.8xlarge']
  return instance_type in hpc_instance_types

def IAMUserReady(iam_aws_id, iam_aws_secret):
  """ Returns true if IAM user can login. """
  ready = False
  if iam_aws_id and iam_aws_secret:
    try:
      s3 = S3Connection(aws_access_key_id=iam_aws_id, aws_secret_access_key=iam_aws_secret)
      ready = True
    except:
      LOG(INFO, 'failed to connect as user: %s' % (iam_aws_id))
      raise
  return ready  
    

def InitCirrusIAMUser(root_aws_id, root_aws_secret):
  """ Given a user's root aws credentials, setup the IAM user environment that
      can be used later for all future api actions. 
  """
  iam = IAMConnection(aws_access_key_id=root_aws_id,  aws_secret_access_key=root_aws_secret)  
  s3 = S3Connection(aws_access_key_id=root_aws_id,  aws_secret_access_key=root_aws_secret)
  cirrus_iam_username = 'cirrus'  
  cirrus_cred_bucketname = 'cirrus_iam_config_%s' % (hashlib.md5(root_aws_id).hexdigest())
  
  has_cirrus_user = False
  response = iam.get_all_users()  
  for user in response['list_users_response']['list_users_result']['users']:
    LOG(INFO, 'iam user exists')
    has_cirrus_user = True                
  if not has_cirrus_user:
    LOG(INFO, 'creating iam user')
    response = iam.create_user(cirrus_iam_username)    
    # setup role so workstation can assume IAM credentials without additional configuration
    # this may fail if role already exists
    
    LOG(INFO, 'created iam role and policy')
    role_name = 'cirrus_workstation'      
    power_user_policy_json = '{"Version": "2012-10-17", "Statement": [ {"Effect": "Allow", "NotAction": "iam:*", "Resource": "*"} ] }'
    
    # ensure no conflicting role exists with same name
    try:
      iam.remove_role_from_instance_profile(workstation_instance_profile_name, role_name)
    except:
      pass
    
    try:  
      iam.delete_instance_profile(workstation_instance_profile_name)
    except:
      pass
    
    response = iam.list_role_policies(role_name)
    role_policy_names = response['list_role_policies_response']['list_role_policies_result']['policy_names']
    for role_policy_name in role_policy_names:
      iam.delete_role_policy(role_name, role_policy_name)
    
    try:
      iam.delete_role(role_name)
    except:
      raise
    
    iam.create_instance_profile(workstation_instance_profile_name)
    
    workstation_role_arn = None
    try:    
      response = iam.create_role(role_name)
      workstation_role_arn = response['create_role_response']['create_role_result']['role']['arn']
    except:
      pass
    
    CHECK(workstation_role_arn)
    iam.add_role_to_instance_profile(workstation_instance_profile_name, role_name)
    iam.put_role_policy(role_name, 'power_user_policy', power_user_policy_json)
    
    # update iam user to have right to launch instances with the cirrus_workstatio_role    
    policy_json = '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action":"iam:PassRole", "Resource": "%s"}]}' % (workstation_role_arn)
    iam.put_user_policy(cirrus_iam_username, 'assume_cirrus_workstation_role', policy_json)
        
  
  policy_json = '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "NotAction": "iam:*", "Resource": "*"}]}'
  iam.put_user_policy(cirrus_iam_username, 'power_user_policy', policy_json)
  response = iam.get_all_access_keys(cirrus_iam_username)
  iam_id = None
  iam_secret = None
  for key in response['list_access_keys_response']['list_access_keys_result']['access_key_metadata']:
    if key['status'] == 'Active' and key['user_name'] == cirrus_iam_username:
      iam_id = key['access_key_id']
      break
  
  cred_bucket = s3.lookup(cirrus_cred_bucketname)
  if not cred_bucket:
    cred_bucket = s3.create_bucket(cirrus_cred_bucketname, policy='private')
    
  if iam_id:
    # fetch secret from s3
    cirrus_cred_bucket_key = 'cirrus_iam_sec_%s' % (hashlib.md5(iam_id).hexdigest())
    key = cred_bucket.lookup(cirrus_cred_bucket_key)
    #CHECK(key, 'secret for access key not stored... please delete all access keys and try again.')
    if key:
      iam_secret = key.get_contents_as_string()
      
  # part of the credentials is unknown, so make new ones and store those
  if not iam_id or not iam_secret:    
    LOG(INFO, 'creating new aws credentials for IAM user %s.' % cirrus_iam_username) 
    response = iam.create_access_key(cirrus_iam_username)
    new_key = response['create_access_key_response']['create_access_key_result']['access_key']
    iam_id = new_key['access_key_id']
    CHECK(iam_id)
    iam_secret = new_key['secret_access_key']
    cirrus_cred_bucket_key = 'cirrus_iam_sec_%s' % (hashlib.md5(iam_id).hexdigest())
    #store secret in s3 for future use 
    k = Key(cred_bucket)
    k.key = cirrus_cred_bucket_key
    k.set_contents_from_string(iam_secret)
    
  
  
  return iam_id, iam_secret
 


class Manager(object):
  def __init__(self, region_name, iam_aws_id, iam_aws_secret):
    CHECK(region_name)
    CHECK(iam_aws_id)
    CHECK(iam_aws_secret)
    self.region_name = region_name
    self.iam_aws_id = iam_aws_id
    self.iam_aws_secret = iam_aws_secret 
    #self.ec2 = boto.ec2.connect_to_region(region_name, aws_access_key_id=iam_aws_id, aws_secret_access_key=iam_aws_secret)
    self.ec2 = EC2Connection(region = util.GetRegion(region_name), aws_access_key_id=iam_aws_id, aws_secret_access_key=iam_aws_secret)
    self.s3 = S3Connection(aws_access_key_id=iam_aws_id, aws_secret_access_key=iam_aws_secret)
    self.scripts_dir = os.path.dirname(__file__) + '/scripts/'
    self.workstation_tag = 'cirrus_workstation'
    self.workstation_keypair_name = 'cirrus_workstation'
    self.ssh_key = None    
    config_bucketname = 'cirrus_workstation_config_%s' % (hashlib.md5(iam_aws_id).hexdigest())
    src_region = self.region_name
    dst_regions = util.tested_region_names
    self.ssh_key = util.InitKeypair(self.ec2, self.s3, config_bucketname, self.workstation_keypair_name, src_region, dst_regions, iam_aws_id, iam_aws_secret)    
    return
    
  def Debug(self):
    res = self.ec2.get_all_instances(instance_ids=['i-db6da8b4'])
    test_instance = res[0].instances[0]
    #print self.ssh_key
    print test_instance.public_dns_name 
    util.WaitForHostsReachable([test_instance.public_dns_name], self.ssh_key)
    return
    
  def ListInstances(self):
    instances = self.__GetInstances()
    for instance in instances:
      if instance.state == 'terminated':
        continue
              
      if self.workstation_tag in instance.tags:
        name = 'none'
        if 'Name' in instance.tags:
          name = instance.tags['Name']
          
        print 'id: %s   name: %s   state: %s' % (instance.id, name, instance.state)    
    return
      
  def TerminateInstance(self, instance_id):
    instance = self.__GetInstanceById(instance_id)
    CHECK(instance)
    
    instance.modify_attribute('disableApiTermination', False)
        
    self.ec2.terminate_instances([instance_id])
    return
  
  def StopInstance(self, instance_id):
    instance = self.__GetInstanceById(instance_id)
    CHECK(instance)
    self.ec2.stop_instances([instance_id])
    return
  
  
  
  def CreateInstance(self, workstation_name, instance_type, ubuntu_release_name, mapr_version, ami_owner_id = '925479793144'):
    role = 'workstation'
    ami = util.LookupCirrusAmi(self.ec2, instance_type, ubuntu_release_name, mapr_version, role, ami_owner_id)
    CHECK(ami, 'Failed to find a suitable ami')
    self.__CreateWorkstationSecurityGroup() # ensure the security group exists
    
    # find the IAM Policy Profile 
    LOG(INFO, 'Attempting to launch instance with ami: %s' % (ami.id))
    LOG(INFO, 'workstation_instance_profile_name: %s' % (workstation_instance_profile_name))
    reservation = self.ec2.run_instances(ami.id, 
                           key_name = self.workstation_keypair_name,
                           security_groups = [util.workstation_security_group],
                           instance_type = instance_type,
                           #placement = prefered_availability_zone,
                           disable_api_termination = True,
                           instance_initiated_shutdown_behavior = 'stop',   
                           instance_profile_name = workstation_instance_profile_name # IAM instance profile to associate with the instance                                            
                           )
    CHECK_EQ(len(reservation.instances), 1)
    instance = reservation.instances[0]
    instance.add_tag(self.workstation_tag, 'true')
    instance.add_tag('Name', workstation_name) # this shows up the AWS management console
    util.WaitForInstanceRunning(instance)
    util.WaitForInstanceReachable(instance, self.ssh_key)
    return
  
  def DeviceExists(self, device_name, instance):
    exists = util.FileExists(device_name, instance.dns_name, self.ssh_key)
    return exists  

  def ResizeRootVolumeOfInstance(self, instance_id, new_vol_size_gb):
    # check inputs are valid
    CHECK_GE(new_vol_size_gb, 1)
    CHECK_LE(new_vol_size_gb, 500) # probably spending too much if you go bigger than this!
    instance = self.__GetInstanceById(instance_id)
    CHECK(instance)
    
    # get info about current ebs root volume
    instance.update()
    root_device_name = instance.root_device_name
    CHECK(root_device_name, 'This instance has no root device.')
    
    
    LOG(INFO, 'root_device_name: %s' % (root_device_name))
    root_block_map = instance.block_device_mapping[root_device_name]
    CHECK(root_block_map.volume_id)
    orig_root_volume_id = str(root_block_map.volume_id)
    LOG(INFO, 'orig_root_volume_id: %s' % (orig_root_volume_id))
    orig_root_volume_termination_setting = root_block_map.delete_on_termination
    LOG(INFO, 'orig_root_volume_termination_setting: %s' % (orig_root_volume_termination_setting))
    
    vols = self.ec2.get_all_volumes([orig_root_volume_id])
    CHECK_EQ(len(vols), 1)
    orig_root_volume = vols[0]
    
    orig_root_volume_size = orig_root_volume.size
    LOG(INFO, 'orig_root_volume_size: %s' % (orig_root_volume_size))
    orig_root_volume_zone = str(orig_root_volume.zone)
    CHECK(orig_root_volume_zone)
    LOG(INFO, 'orig_root_volume_zone: %s' % (orig_root_volume_zone))
    
    CHECK_EQ(instance.root_device_type, 'ebs')
    
    CHECK_GE(new_vol_size_gb, orig_root_volume_size ,'only increasing size has been tested so far...')
    
    
    # stop the instance
    # if not stopped, stop the instance
    if util.GetInstanceState(instance) != 'stopped':
      self.ec2.stop_instances([instance_id])
      LOG(INFO, 'stopping instance')
      util.WaitForInstanceStopped(instance)
    LOG(INFO, 'Instance is stopped')
          
    # if volume not detached, detach it
    if root_block_map.status != 'detached':
      LOG(INFO, 'root_block_map.status: %s' % (root_block_map.status))
      LOG(INFO, 'detaching root volume')
      self.ec2.detach_volume(orig_root_volume_id, instance_id, root_device_name)
      util.WaitForVolumeAvailable(orig_root_volume)
    LOG(INFO, 'Root volume is detached')
    
    # Create a snapshot
    snapshot = self.ec2.create_snapshot(orig_root_volume_id, 'temporary snapshot of root vol for resize')
    util.WaitForSnapshotCompleted(snapshot)
    
    # Create a new larger volume from the snapshot                          
    #new_volume = snapshot.create_volume(orig_root_volume_zone, size=new_vol_size_gb)

    
    new_volume = self.ec2.create_volume(new_vol_size_gb, orig_root_volume_zone, snapshot = snapshot)    
    util.WaitForVolumeAvailable(new_volume)

    # Attach the new volume as the root device
    new_volume.attach(instance_id, '/dev/sda1')    
    util.WaitForVolumeAttached(new_volume)
    
    # TODO delete the old root volume and the temporary snapshot
    LOG(INFO, 'you should delete this snapshot: %s' % (snapshot))
    LOG(INFO, 'you should delete this volume: %s' % (orig_root_volume_id))    
    
    return
    
  def AddNewVolumeToInstance(self, instance_id, vol_size_gb):
    CHECK_GE(vol_size_gb, 1)
    CHECK_LE(vol_size_gb, 500) # probably spending too much if you go bigger than this!
    instance = self.__GetInstanceById(instance_id)
    CHECK(instance)
    
    # select an unused device
    # see http://askubuntu.com/questions/47617/how-to-attach-new-ebs-volume-to-ubuntu-machine-on-aws
    potential_device_names = ['/dev/xvdf', '/dev/xvdg', '/dev/xvdh', '/dev/xvdi'] 
    device_name = None
    
    for name in potential_device_names:
      if not self.DeviceExists(name, instance):
        device_name = name
        break
      
    CHECK_NE(device_name, None, 'No suitable device names available')    
        
    # Attach volume         
    volume = self.ec2.create_volume(vol_size_gb, instance.placement)
    volume.attach(instance.id, device_name)
    
    # wait for volume to attach
    util.WaitForVolumeAttached(volume)
    
    while not self.DeviceExists(device_name, instance): 
      LOG(INFO, 'waiting for device to be attached...')
      time.sleep(5)
            
    CHECK(volume.id)
    CHECK_EQ(volume.attach_data.device, device_name)
    
    # format file system, mount the file system, update fstab to automount
    hostname = instance.dns_name
    add_ebs_volume_playbook = os.path.dirname(__file__) + '/playbooks/add_ebs_volume.yml'
    
    extra_vars = {}
    extra_vars['volume_name'] = volume.id
    extra_vars['volume_device'] = device_name
    CHECK(util.RunPlaybookOnHost(add_ebs_volume_playbook, hostname, self.ssh_key, extra_vars))
    return  
    
  
  def CreateRemoteSessionConfig(self, instance_id):
    instance = self.__GetInstanceById(instance_id)
    if instance.state != 'running':
      instance.start()
      util.WaitForInstanceRunning(instance)
      util.WaitForHostsReachable([instance.public_dns_name], self.ssh_key)      
    nx_key = util.ReadRemoteFile('/usr/NX/share/keys/default.id_dsa.key', instance.public_dns_name, self.ssh_key)      
    CHECK(nx_key)
    CHECK_EQ(instance.state, 'running')
    config_content = Template(GetNxsTemplate()).substitute({'public_dns_name' : instance.public_dns_name, 'nx_key' : nx_key})
    
    
    # TODO(heathkh): Once this operation is performed at ami creatio ntime, this shouldn't be needed at login time
    rm_nx_known_hosts = 'sudo rm /usr/NX/home/nx/.ssh/known_hosts'
    util.RunCommandOnHost(rm_nx_known_hosts, instance.public_dns_name, self.ssh_key)
    return config_content
  
  
  def __GetInstanceById(self, id):
    instances = self.__GetInstances()
    desired_instance = None
    for instance in instances:      
      if instance.id == id:
        desired_instance = instance
        break
    return desired_instance
    
    
  def __GetInstances(self, group_name = None, state_filter=None):
    """
    Get all the instances in a group, filtered by state.

    @param group_name: the name of the group
    @param state_filter: the state that the instance should be in
      (e.g. "running"), or None for all states
    """
    all_instances = self.ec2.get_all_instances()
    instances = []
    for res in all_instances:
      for group in res.groups:
        if group_name and group.name != group_name:
          continue
        for instance in res.instances:            
          if state_filter == None or instance.state == state_filter:              
            instances.append(instance)
    return instances
    


  def __CreateWorkstationSecurityGroup(self):
    group_name = util.workstation_security_group
    group = None
    try:
      groups = self.ec2.get_all_security_groups([group_name])
      CHECK_EQ(len(groups), 1)
      group = groups[0]
    except:
      pass      
    if not group:          
      group = self.ec2.create_security_group(group_name, 'Group for development workstations')      
      # allow ssh connection to this group from anywhere
      group.authorize('tcp', 22, 22, '0.0.0.0/0')  
    return



      
#  def LaunchSpotFromSnapshot(self, snapshot_id):
#    
#    name = 'clone of %s' % (snapshot_id)
#    description = name
#    
#    root_device_name = '/dev/sda1'
#    block_device_map = BlockDeviceMapping()
#    block_device_map[root_device_name] = BlockDeviceType(snapshot_id=snapshot_id, delete_on_termination=False)
#    architecture = 'x86_64'
#    kernel_id = 'aki-9ba0f1de'
#    new_ami_id = self.ec2.register_image(name=name, description=description, architecture=architecture, kernel_id=kernel_id, block_device_map=block_device_map, root_device_name=root_device_name )
#    
#    price = 0.5
#    role = 'test-cluster'
#    instance_type = 'c1.xlarge'
#    availability_zone = self.ec2_region_name + 'a'
#    private_key_name = 'west_kp1'
#    security_groups = self.ec2.get_all_security_groups(groupnames=['test-cluster'])
#    
#    spot_instances = self._LaunchSpotInstances(price, security_groups, new_ami_id, kernel_id, instance_type, availability_zone, private_key_name)
#    
#    new_instance = spot_instances[0]
#    
#    self.LaunchRemoteSession(new_instance.id)
#    
#    return 
    
#  
#  
#  def _LaunchSpotInstances(self, price, security_groups, image_id, kernel_id, instance_type, availability_zone, private_key_name):    
#        
#    spot_instance_request_ids = []
#    
#    spot_request = self.ec2.request_spot_instances(price=price,
#      image_id=image_id,
#      count=1, 
#      type='one-time', 
#      valid_from=None, 
#      valid_until=None,
#      launch_group=None, #'maprcluster-spotstartgroup',
#      availability_zone_group=None, #availability_zone, 
#      key_name=private_key_name,
#      security_groups=security_groups, 
#      user_data=None,
#      instance_type=instance_type, 
#      placement=availability_zone)
#    spot_instance_request_ids.extend([request.id for request in spot_request])    
#    instances = self._WaitForSpotInstances(spot_instance_request_ids)
#    return instances
#    
#    
    
#  def _WaitForSpotInstances(self, request_ids, timeout=1200):
#    start_time = time.time()
#    instance_id_set = set()
#
#    print 'waiting for spot requests to be fulfilled'    
#    while True:
#      for request in self.ec2.get_all_spot_instance_requests(request_ids):
#        #print request        
#        #print dir(request)
#        if request.instance_id:
#          print 'request.instance_id %s' % request.instance_id
#          instance_id_set.add(request.instance_id)      
#      num_fulfilled = len(instance_id_set) 
#      num_requested = len(request_ids)
#       
#      if num_fulfilled == num_requested:
#        break
#      
#      print 'fulfilled %d of %d' % (num_fulfilled, num_requested)
#      time.sleep(15)    
#
#    instance_ids = [id for id in instance_id_set]
#    
#    time.sleep(5)
#    print 'waiting for spot instances to start'
#    while True:
#      
#      try:        
#        if NonePending(self.ec2.get_all_instances(instance_ids)):
#          break
#       # don't timeout for race condition where instance is not yet registered
#      except EC2ResponseError as e:
#        print e
#              
#      
#      instances = None
#      try:        
#        instances = self.ec2.get_all_instances(instance_ids)
#       # don't timeout for race condition where instance is not yet registered
#      except EC2ResponseError as e:
#        print e
#        continue
#            
#      num_started = NumberInstancesInState(instances, "running")
#      num_pending = NumberInstancesInState(instances, "pending")
#      print 'started: %d pending: %d' % (num_started, num_pending)
#    
#      time.sleep(15)      
#      
#    instances = []
#    for reservation in self.ec2.get_all_instances(instance_ids):
#      for instance in reservation.instances:      
#        instances.append(instance)
#    return instances
#      
    

def GetNxsTemplate():
  template = """
  <!DOCTYPE NXClientSettings>
<NXClientSettings application="nxclient" version="1.3" >
<group name="Advanced" >
<option key="Cache size" value="128" />
<option key="Cache size on disk" value="256" />
<option key="Current keyboard" value="true" />
<option key="Custom keyboard layout" value="" />
<option key="Disable DirectDraw" value="false" />
<option key="Disable ZLIB stream compression" value="false" />
<option key="Disable deferred updates" value="false" />
<option key="Enable HTTP proxy" value="false" />
<option key="Enable SSL encryption" value="true" />
<option key="Enable response time optimisations" value="false" />
<option key="Grab keyboard" value="false" />
<option key="HTTP proxy host" value="" />
<option key="HTTP proxy port" value="8080" />
<option key="HTTP proxy username" value="" />
<option key="Remember HTTP proxy password" value="false" />
<option key="Restore cache" value="true" />
<option key="StreamCompression" value="" />
</group>
<group name="Environment" >
<option key="CUPSD path" value="/usr/sbin/cupsd" />
</group>
<group name="General">
<option key="Automatic reconnect" value="true" />
<option key="Command line" value="" />
<option key="Custom Unix Desktop" value="application" />
<option key="Desktop" value="gnome" />
<option key="Disable SHM" value="false" />
<option key="Disable emulate shared pixmaps" value="false" />
<option key="Link speed" value="adsl" />
<option key="Remember password" value="false" />
<option key="Resolution" value="available" />
<option key="Resolution height" value="600" />
<option key="Resolution width" value="800" />
<option key="Server host" value="${public_dns_name}" />
<option key="Server port" value="22" />
<option key="Session" value="unix" />
<option key="Spread over monitors" value="false" />
<option key="Use default image encoding" value="0" />
<option key="Use render" value="true" />
<option key="Use taint" value="true" />
<option key="Virtual desktop" value="false" />
<option key="XAgent encoding" value="true" />
<option key="displaySaveOnExit" value="false" />
<option key="xdm broadcast port" value="177" />
<option key="xdm list host" value="localhost" />
<option key="xdm list port" value="177" />
<option key="xdm mode" value="server decide" />
<option key="xdm query host" value="localhost" />
<option key="xdm query port" value="177" />
</group>
<group name="Images" >
<option key="Disable JPEG Compression" value="0" />
<option key="Disable all image optimisations" value="false" />
<option key="Disable backingstore" value="false" />
<option key="Disable composite" value="false" />
<option key="Image Compression Type" value="3" />
<option key="Image Encoding Type" value="0" />
<option key="Image JPEG Encoding" value="false" />
<option key="JPEG Quality" value="6" />
<option key="RDP Image Encoding" value="3" />
<option key="RDP JPEG Quality" value="6" />
<option key="RDP optimization for low-bandwidth link" value="false" />
<option key="Reduce colors to" value="" />
<option key="Use PNG Compression" value="true" />
<option key="VNC JPEG Quality" value="6" />
<option key="VNC images compression" value="3" />
</group>
<group name="Login" >
<option key="Auth" value="EMPTY_PASSWORD" />
<option key="Guest Mode" value="false" />
<option key="Guest password" value="" />
<option key="Guest username" value="" />
<option key="Login Method" value="nx" />
<option key="Public Key" value="${nx_key}" />
<option key="User" value="ubuntu" />
</group>
<group name="Services" >
<option key="Audio" value="false" />
<option key="IPPPort" value="631" />
<option key="IPPPrinting" value="false" />
<option key="Shares" value="false" />
</group>
<group name="VNC Session" >
<option key="Display" value="0" />
<option key="Remember" value="false" />
<option key="Server" value="" />
</group>
<group name="Windows Session" >
<option key="Application" value="" />
<option key="Authentication" value="2" />
<option key="Color Depth" value="8" />
<option key="Domain" value="" />
<option key="Image Cache" value="true" />
<option key="Password" value="EMPTY_PASSWORD" />
<option key="Remember" value="true" />
<option key="Run application" value="false" />
<option key="Server" value="" />
<option key="User" value="" />
</group>
<group name="share chosen" >
<option key="Share number" value="0" />
</group>
</NXClientSettings>
"""
  return template       
    
