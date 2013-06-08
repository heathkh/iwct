# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import boto
from boto.ec2.connection import EC2Connection
from boto.exception import EC2ResponseError
import os
import re
import subprocess
import sys
import time
import datetime
import dateutil.parser
from snap.pyglog import *
from snap.cirrus import util

class Instance(object):
  """
  A server instance.
  """
  def __init__(self, id, public_hostname, private_hostname, private_ip):
    self.id = id
    self.public_hostname = public_hostname
    self.private_hostname = private_hostname
    self.private_ip = private_ip
    return

  
class RoleSyntaxException(Exception):
  """
  Raised when a role name is invalid. Role names may consist of a sequence
  of alphanumeric characters and underscores. Dashes are not permitted in role
  names.
  """
  def __init__(self, message):
    super(RoleSyntaxException, self).__init__()
    self.message = message
  def __str__(self):
    return repr(self.message)


class TimeoutException(Exception):
  """
  Raised when a timeout is exceeded.
  """
  pass


class Ec2Cluster():
  """
  A cluster of EC2 instances. A cluster has a unique name.

  Instances running in the cluster run in a security group with the cluster's
  name and the instance's role, e.g. <cluster-name>-<role>
  """

  def __init__(self, name, aws_access_key_id, aws_secret_access_key, region_name):    
    #super(Ec2Cluster, self).__init__()
    self._name = name
    
    regions = boto.ec2.regions()
    
    region = None
    for r in regions:
      if r.name == region_name:
        region = r 
        break
      
    if not region:
      LOG(FATAL, 'invalid region name: %s' % region_name)  
            
    self._ec2Connection = EC2Connection(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region=region)
    return

  def authorize_role(self, role, protocol, from_port, to_port, cidr_ip):
    """
    Authorize access to machines in a given role from a given network.
    """
    if (protocol != 'tcp' and protocol != 'udp'):
      raise Exception('error: expected protocol to be tcp or udp but got %s' % (protocol))
    
    self._check_role_name(role)
    role_group_name = self._group_name_for_role(role)
    # Revoke first to avoid InvalidPermission.Duplicate error
    self._ec2Connection.revoke_security_group(role_group_name,
                                             ip_protocol=protocol,
                                             from_port=from_port,
                                             to_port=to_port, cidr_ip=cidr_ip)
    self._ec2Connection.authorize_security_group(role_group_name,
                                                ip_protocol=protocol,
                                                from_port=from_port,
                                                to_port=to_port,
                                                cidr_ip=cidr_ip)

  def check_running(self, role, number):
    """
    Check that a certain number of instances in a role are running.
    """
    instances = self.get_instances_in_role(role, "running")
    if len(instances) != number:
      print "Expected %s instances in role %s, but was %s %s" % \
        (number, role, len(instances), instances)
      return False
    else:
      return instances

  def get_instances_in_role(self, role, state_filter=None):
    """
    Get all the instances in a role, filtered by state.

    @param role: the name of the role
    @param state_filter: the state that the instance should be in
      (e.g. "running"), or None for all states
    """
    self._check_role_name(role)
    instances = []
    
    for instance in self._get_instances(self._group_name_for_role(role), state_filter):            
      instances.append(Instance(instance.id, instance.dns_name, instance.private_dns_name, instance.private_ip_address))
      
    return instances
  
  def get_instances(self, state_filter=None):
    """
    Get all the instances filtered by state.
    
    @param state_filter: the state that the instance should be in
      (e.g. "running"), or None for all states
    """

    instances = []
    for instance in self._get_instances(self._get_cluster_group_name(), state_filter):
      instances.append(Instance(instance.id, instance.dns_name, instance.private_dns_name, instance.private_ip_address))
    return instances

  def print_status(self, roles=None, state_filter="running"):
    """
    Print the status of instances in the given roles, filtered by state.
    """
    if not roles:
      for instance in self._get_instances(self._get_cluster_group_name(),
                                          state_filter):
        self._print_instance("", instance)
    else:
      for role in roles:
        for instance in self._get_instances(self._group_name_for_role(role),
                                            state_filter):
          self._print_instance(role, instance)

  def launch_instances(self, role, number, image_id, instance_type, private_key_name, availability_zone):
    self._check_role_name(role)
    self._create_security_groups(role)
      
    user_data = None
    security_groups = self._get_group_names([role]) 

    reservation = self._ec2Connection.run_instances(image_id, min_count=number,
      max_count=number, key_name=private_key_name,
      security_groups=security_groups, user_data=user_data,
      instance_type=instance_type,
      placement=availability_zone)
    return [instance.id for instance in reservation.instances]
 
 
  def wait_for_instances(self, instance_ids, timeout=600):
    start_time = time.time()
    while True:
      instances = self._ec2Connection.get_all_instances(instance_ids)
      if (time.time() - start_time >= timeout):
        raise TimeoutException()
      try:
        if self._all_started(instances):
          break
      # don't timeout for race condition where instance is not yet registered
      except EC2ResponseError:
        pass
      
      num_started = self._number_instances_in_state(instances, "running")
      num_pending = self._number_instances_in_state(instances, "pending")
      print 'started: %d pending: %d' % (num_started, num_pending) 
      
      time.sleep(15)
    return
    
    
  def CreateBlockDeviceMapForInstanceType(self, image_id, instance_type):
      """
      If you launch without specifying a manual device block mapping, you may 
      not get all the ephemeral devices available to the given instance type.
      This will build one that ensures all available ephemeral devices are mapped.      
      """
      # get the block device mapping stored with the image
      image = self._ec2Connection.get_image(image_id)
      block_device_map = image.block_device_mapping
      CHECK(block_device_map)
      # update it to include the ephemeral devices
      
      num_ephemeral_drives = 4 # max is 4... is it an error for instances with fewer than 4 ?  see: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html#StorageOnInstanceTypes
      ephemeral_device_names = ['/dev/sdb', '/dev/sdc', '/dev/sdd', '/dev/sde' ]      
      for i, device_name in enumerate(ephemeral_device_names):
        block_device_map[device_name] = boto.ec2.blockdevicemapping.BlockDeviceType(ephemeral_name = 'ephemeral%d' % (i))
      return block_device_map  
    
    
  def launch_and_wait_for_spot_instances(self, price, role, image_id, instance_type, private_key_name, number_zone_list):    
    self._check_role_name(role)  
    self._create_security_groups(role)
    security_groups = self._get_group_names([role])    
    spot_instance_request_ids = []
    
    # placement_group compatible only for HPC class instances
    placement_group = None
    if util.IsHPCInstanceType(instance_type):    
      placement_group = self.get_cluster_placement_group() 
      
    for number, availability_zone in number_zone_list:
      print 'request: %d in %s' % (number, availability_zone)
      if number == 0:
        continue
      
      block_device_map = self.CreateBlockDeviceMapForInstanceType(image_id, instance_type)
      
      spot_request = self._ec2Connection.request_spot_instances(price=price,
        image_id=image_id,
        count=number, 
        type='one-time', 
        valid_from=None, 
        valid_until=None,
        launch_group=None, 
        availability_zone_group=None,  
        key_name=private_key_name,
        security_groups=security_groups, 
        user_data=None,
        instance_type=instance_type, 
        placement=availability_zone,
        placement_group = placement_group,
        block_device_map = block_device_map)
      spot_instance_request_ids.extend([request.id for request in spot_request])
      
    instances = self.wait_for_spot_instances(spot_instance_request_ids)
    return instances

#    
#  def launch_and_wait_for_spot_instances(self, price, role, number, image_id, instance_type, private_key_name, availability_zone):
#    
#    roles = [role]
#    
#    for role in roles:
#      self._check_role_name(role)  
#      self._create_security_groups(role)
#          
#    security_groups = self._get_group_names(roles)
#    
#    spot_request = self._ec2Connection.request_spot_instances(price=price, image_id=image_id,
#      count=number, type=None, valid_from=None, valid_until=None,
#      launch_group='maprcluster-spotstartgroup',
#      availability_zone_group=availability_zone, # want all in same zone to avoid data transfere cost between availability zones
#      key_name=private_key_name,
#      security_groups=security_groups, 
#      user_data=None,
#      instance_type=instance_type, 
#      placement=None)
#    spot_instance_request_ids = [request.id for request in spot_request]
#    instance_ids = self.wait_for_spot_instances(spot_instance_request_ids)
#    return instance_ids

 
 
  def wait_for_spot_instances(self, request_ids, timeout=1200):
    start_time = time.time()
    
    instance_id_set = set()

    print 'waiting for spot requests to be fulfilled'    
    while True:
      time.sleep(1) # wait for the request that was created to become live (reduces chance of race condition occuring)
      try:
        for request in self._ec2Connection.get_all_spot_instance_requests(request_ids): # this can raise an exception because of race condition mentioned above
          CHECK_NE(request.state, 'failed', ' %s' % request.fault)
          CHECK_NE(request.status, 'bad-parameters', '%s' % request.fault)          
          if request.instance_id:
            print 'request.instance_id %s' % request.instance_id
            instance_id_set.add(request.instance_id)
      except:
        LOG(INFO, '...')
        pass      
      num_fulfilled = len(instance_id_set) 
      num_requested = len(request_ids)
      if num_fulfilled == num_requested:
        break
      print 'fulfilled %d of %d' % (num_fulfilled, num_requested)
      time.sleep(15)    

    instance_ids = [mid for mid in instance_id_set]
    time.sleep(1)
    print 'waiting for spot instances to start'
    while True:
      try:        
        if self._none_pending(self._ec2Connection.get_all_instances(instance_ids)):
          break
       # don't timeout for race condition where instance is not yet registered
      except EC2ResponseError as e:
        print e
      
      instances = None
      try:        
        instances = self._ec2Connection.get_all_instances(instance_ids)
       # don't timeout for race condition where instance is not yet registered
      except EC2ResponseError as e:
        print e
        continue
            
      num_started = self._number_instances_in_state(instances, "running")
      num_pending = self._number_instances_in_state(instances, "pending")
      print 'started: %d pending: %d' % (num_started, num_pending)    
      time.sleep(15)      
      
    instances = []
    for reservation in self._ec2Connection.get_all_instances(instance_ids):
      for instance in reservation.instances:      
        CHECK(instance.id)
        CHECK(len(instance.dns_name) > 4)
        CHECK(len(instance.private_dns_name) > 4)
        CHECK(len(instance.private_ip_address) > 4)
        instances.append(Instance(instance.id, instance.dns_name, instance.private_dns_name, instance.private_ip_address))
    return instances  
      
 
 

  def get_current_spot_instance_price(self, instance_type, availability_zone):    
    hist = self._ec2Connection.get_spot_price_history(start_time=None, end_time=None, instance_type=instance_type, product_description='Linux/UNIX', availability_zone=availability_zone)    
    
    CHECK(hist, 'The requested instance type is not available in this zone. (instance type: %s, zone: %s) Use the AWS console to find the right zone/instance type combo.' % (instance_type, availability_zone))
    
    # The API doesn't always return history data in sorted order... so sort it increasing by time
    time_format = ''
    for h in hist:   
      #h.timestamp = time.strptime(time_format, h.timestamp)
      h.datetime = dateutil.parser.parse(h.timestamp)      
    hist.sort(key = lambda v : v.datetime)  
      
    #for h in hist:  
    #  print '%s %s' % (h.price, h.datetime) 
    
    # grab the most recent price
    cur_price = hist[-1].price
    return cur_price
  
  def get_recent_max_spot_instance_price(self, instance_type, availability_zone, num_days = 5):    
    end_time  = datetime.datetime.utcnow()
    start_time  = end_time - datetime.timedelta(num_days) # last num_days days
    
    #print start_time.isoformat()
    #print end_time.isoformat()
    
    hist = self._ec2Connection.get_spot_price_history(start_time=start_time.isoformat(), end_time=end_time.isoformat(), instance_type=instance_type, product_description='Linux/UNIX', availability_zone=availability_zone)    
    assert(hist)
    max_price = 0
    for h in hist:      
      max_price = max(max_price, h.price)
    
    return max_price

      
  def terminate_all_instances(self):
    instances = self._get_instances(self._get_cluster_group_name(), "running")
    if instances:
      self._ec2Connection.terminate_instances([i.id for i in instances])

    #cleanup security groups
    self._delete_security_groups()
    return

  def terminate_instances(self, instances):    
    if instances:
      self._ec2Connection.terminate_instances([i.id for i in instances])

    #cleanup security groups
    #self._delete_security_groups()
    return
  
  def get_cluster_placement_group(self):
    """
    Return the placement group, create it if it doesn't yet exist. (needed for cluster type instances).
    """
    placement_group_name = 'pg-%s' % (self._name)
    matching_placement_groups = self._ec2Connection.get_all_placement_groups(groupnames=[placement_group_name])
    placement_group = None
    if matching_placement_groups:
      CHECK_EQ(len(matching_placement_groups), 1)
      placement_group = matching_placement_groups[0] 
    else:
       CHECK(self._ec2Connection.create_placement_group(placement_group_name, strategy = 'cluster'), 'could not create placement group: %s' % (placement_group_name) )     
    return placement_group_name
    

# ************************************************************************
# PRIVATE  
# ************************************************************************


  def _get_cluster_group_name(self):
    return self._name

  def _check_role_name(self, role):
    if not re.match("^[a-zA-Z0-9_+]+$", role):
      raise RoleSyntaxException("Invalid role name '%s'" % role)

  def _group_name_for_role(self, role):
    """
    Return the security group name for an instance in a given role.
    """
    self._check_role_name(role)
    return "%s-%s" % (self._name, role)

  def _get_group_names(self, roles):
    group_names = [self._get_cluster_group_name()]
    for role in roles:
      group_names.append(self._group_name_for_role(role))
    return group_names

  def _get_all_group_names(self):
    security_groups = self._ec2Connection.get_all_security_groups()
    security_group_names = \
      [security_group.name for security_group in security_groups]
    return security_group_names

  def _get_all_group_names_for_cluster(self):
    all_group_names = self._get_all_group_names()
    r = []
    if self._name not in all_group_names:
      return r
    for group in all_group_names:
      if re.match("^%s(-[a-zA-Z0-9_+]+)?$" % self._name, group):
        r.append(group)
    return r

  def _create_security_groups(self, role):
    """
    Create the security groups for a given role, including a group for the
    cluster if it doesn't exist.
    """
    self._check_role_name(role)
    security_group_names = self._get_all_group_names()

    cluster_group_name = self._get_cluster_group_name()
    if not cluster_group_name in security_group_names:
      self._ec2Connection.create_security_group(cluster_group_name,
                                               "Cluster (%s)" % (self._name))
      self._ec2Connection.authorize_security_group(cluster_group_name,
                                                  cluster_group_name)
      # Allow SSH from anywhere
      self._ec2Connection.authorize_security_group(cluster_group_name,
                                                  ip_protocol="tcp",
                                                  from_port=22, to_port=22,
                                                  cidr_ip="0.0.0.0/0")

    role_group_name = self._group_name_for_role(role)
    if not role_group_name in security_group_names:
      self._ec2Connection.create_security_group(role_group_name,
        "Role %s (%s)" % (role, self._name))
    return

  

  
  def _delete_security_groups(self):
    """
    Delete the security groups for each role in the cluster, and the group for
    the cluster.
    """
    group_names = self._get_all_group_names_for_cluster()
    for group in group_names:
      self._ec2Connection.delete_security_group(group)
  
  def _get_instances(self, group_name, state_filter=None):
    """
    Get all the instances in a group, filtered by state.

    @param group_name: the name of the group
    @param state_filter: the state that the instance should be in
      (e.g. "running"), or None for all states
    """
    all_instances = self._ec2Connection.get_all_instances()
    instances = []
    for res in all_instances:
      for group in res.groups:
        if group.name == group_name:
          for instance in res.instances:            
            if state_filter == None or instance.state == state_filter:              
              instances.append(instance)    
    return instances


  def _print_instance(self, role, instance):
    print "\t".join((role, instance.id,
      instance.image_id,
      instance.dns_name, instance.private_dns_name,
      instance.state, str(instance.key_name), instance.instance_type,
      str(instance.launch_time), instance.placement))

  
  def _all_started(self, reservations):
    for res in reservations:
      for instance in res.instances:
        if instance.state != "running":
          return False
    return True

  def _none_pending(self, reservations):
    for res in reservations:
      for instance in res.instances:
        if instance.state == "pending":
          return False
    return True

  def _number_instances_in_state(self, reservations, state):
    number_instances = 0
    for res in reservations:
      for instance in res.instances:
        if instance.state == state:
          number_instances += 1
    return number_instances
  

