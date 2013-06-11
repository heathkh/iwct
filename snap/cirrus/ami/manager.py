#!/usr/bin/python
from snap.cirrus import util
from snap.pyglog import *
from snap.boto.ec2.connection import EC2Connection
import uuid
import shutil

class AmiSpecification(object):
  def __init__(self, region_name, instance_type, ubuntu_release_name, mapr_version, role, owner_id = 'self'):    
    CHECK(region_name in ['us-west-1', 'us-east-1'])
    CHECK(instance_type in ['c1.xlarge', 'cc2.8xlarge'])
    CHECK(ubuntu_release_name in ['precise'])
    CHECK(mapr_version in ['v2.1.3'])
    CHECK(role in ['workstation', 'master', 'worker'])
    self.owner_id = owner_id    
    self.region_name = region_name
    self.instance_type = instance_type
    self.ubuntu_release_name = ubuntu_release_name
    self.mapr_version = mapr_version
    self.role = role        
    self.root_store_type, self.virtualization_type = util.GetRootStoreAndVirtualizationType(self.instance_type)
    
    # currently only have tools to create ebs backed amis... 
    self.root_store_type = 'ebs'
  
    self.ami_name = 'cirrus-ubuntu-%s-%s-mapr%s-%s' % (self.ubuntu_release_name, self.virtualization_type, self.mapr_version, self.role)
    return

  
def GetAmi(ec2, ami_spec):
    """ Get the boto ami object given a AmiSpecification object. """ 
    images = ec2.get_all_images(owners=[ami_spec.owner_id] )
    requested_image = None
    for image in images:
      if image.name == ami_spec.ami_name:
        requested_image = image
        break
    return requested_image


class AmiMaker(object):  
  """ Creates an ubuntu ami pre-configured for different cirrus roles. """  
  def __init__(self, ec2, ami_spec, key_pair_name, ssh_key):
    self.ec2 = ec2
    self.ami_spec = ami_spec
    self.key_pair = ec2.get_key_pair(key_pair_name)
    self.ssh_key = ssh_key
    
    # Fail if an image matching this spec already exists
    if GetAmi(self.ec2, self.ami_spec):
      ami_webui_url = 'https://console.aws.amazon.com/ec2/home?region=%s#s=Images' % self.ami_spec.region_name     
      LOG(FATAL, 'An ami for this role already exists... please manually delete it here: %s' % ami_webui_url )      
    return
  
  def Run(self):
    template_instance = None
    res = self.ec2.get_all_instances(filters={'tag-key': 'spec', 'tag-value' : self.ami_spec.ami_name, 'instance-state-name' : 'running'})
    
    if res:
      running_template_instances = res[0].instances
      if running_template_instances:
        CHECK(len(running_template_instances), 1)
        template_instance = running_template_instances[0]
    
    # if there is not a currently running template instance being modified, start one
    if not template_instance:
      template_instance = self.__CreateTemplateInstance()
      template_instance.add_tag('spec', self.ami_spec.ami_name)
      
      
    CHECK(template_instance)  
    if self.ami_spec.role == 'workstation':
      self.__ConfigureAsWorkstation(template_instance)
    elif self.ami_spec.role == 'master':
      self.__ConfigureAsClusterMaster(template_instance)
    elif self.ami_spec.role == 'worker':
      self.__ConfigureAsClusterWorker(template_instance)
    else: 
      LOG(FATAL, 'unknown role: %s' % (self.ami_spec.role))        
    
    raw_input('Please login and perform any custom manipulations before snapshot is made!')
    
    self.__SecurityScrub(template_instance)
    
    ami_id = None
    if self.ami_spec.root_store_type == 'ebs':  
      ami_id = self.__CreateEbsAmi(template_instance)
    else:
      LOG(INFO, 'Support for creating instance-store backed images has been disabled in this version because it required much greater complexity.')
      ami_id = self.__CreateEbsAmi(template_instance)
      
    #self.__SetImagePermissions(ami_id)
    #self.__DistributeImageToAllRegions(ami_id)
      
    print 'terminating template instance'
    self.ec2.terminate_instances(instance_ids=[template_instance.id])
    util.WaitForInstanceTerminated(template_instance)    
    return
  
  def GetInstanceById(self, instance_id):
    reservations = self.ec2.get_all_instances([instance_id])  
    instance = None
    for r in reservations:
       for i in r.instances:         
         if i.id == instance_id:
           instance = i
           break
    return instance
  
  def __CreateTemplateInstance(self):     
    template_image = self.__GetTemplateImage()
    self.__CreateSshSecurityGroup()    
    reservation = template_image.run(key_name=self.key_pair.name, security_groups=['ssh'], instance_type=self.ami_spec.instance_type)  
    instance = reservation.instances[0]
    util.WaitForInstanceRunning(instance)    
    util.WaitForHostsReachable([instance.public_dns_name], self.ssh_key)    
    return instance
  
  def __ConfigureAsWorkstation(self, instance):
    LOG(INFO, 'Configuring a workstation...')
    playbook = os.path.dirname(__file__) + '/templates/workstation/workstation.yml'
    extra_vars = {'mapr_version' : self.ami_spec.mapr_version}
    CHECK(util.RunPlaybookOnHost(playbook, instance.dns_name, self.ssh_key, extra_vars = extra_vars))
    
    nx_client_ssh_key = util.ReadRemoteFile('/usr/NX/share/keys/default.id_dsa.key', instance.dns_name, self.ssh_key)    
    print 'To connect with NX Client, you must paste this key into the gui advanced settings config.'
    print nx_client_ssh_key
    return
  
  def __ConfigureAsClusterMaster(self, instance):
    LOG(INFO, 'Configuring a cluster master...')    
    playbook = os.path.dirname(__file__) + '/templates/cluster/master.yml'
    extra_vars = {'mapr_version' : self.ami_spec.mapr_version}
    CHECK(util.RunPlaybookOnHost(playbook, instance.dns_name, self.ssh_key, extra_vars = extra_vars))
    return
  
  def __ConfigureAsClusterWorker(self, instance):
    LOG(INFO, 'Configuring a cluster worker...')  
    playbook = os.path.dirname(__file__) + '/templates/cluster/worker.yml'
    extra_vars = {'mapr_version' : self.ami_spec.mapr_version}
    CHECK(util.RunPlaybookOnHost(playbook, instance.dns_name, self.ssh_key, extra_vars = extra_vars))
    return
   
  def __SecurityScrub(self, instance):
    # delete the shell history 
    CHECK(util.RunCommandOnHost('sudo find /root/.*history /home/*/.*history -exec rm -f {} \;', instance.dns_name, self.ssh_key))
    # Clear nx cache of known hosts, otherwise first login fails until this is cleared
    util.RunCommandOnHost('sudo rm /usr/NX/home/nx/.ssh/known_hosts', instance.dns_name, self.ssh_key)
    # Only run this right before you create the ami
    # After you do this, you can't make a new connection because key pair is gone!
    CHECK(util.RunCommandOnHost('sudo find / -name "authorized_keys" -exec rm -f {} \;', instance.dns_name, self.ssh_key))
    return
     
  def __CreateEbsAmi(self, instance):    
    # details here: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/creating-an-ami-ebs.html
    # step 1: stop the instance so it is in a consistent state
    self.ec2.stop_instances(instance_ids=[instance.id])
    # wait till stopped
    util.WaitForInstanceStopped(instance)
    LOG(INFO, 'instance stopped... ready to create image: %s' % (instance.id))
    ami_description = self.ami_spec.ami_name
    new_ami_id = self.ec2.create_image(instance.id, self.ami_spec.ami_name, ami_description)
    LOG(INFO, 'new ami: %s' % (new_ami_id))
    return new_ami_id
  
  def __SetImagePermissions(self, ami_id):
    new_images = self.ec2.get_all_images(image_ids=[ami_id])
    CHECK(len(new_images), 1)
    new_image = new_images[0]    
    new_image.set_launch_permissions(group_names=['all'])
    return    
      
  def __CreateSshSecurityGroup(self):
    ssh_group = None
    try:      
      groups = self.ec2.get_all_security_groups(['ssh'])
      CHECK_EQ(len(groups), 1)
      ssh_group = groups[0]
    except:
      pass
      
    if not ssh_group:          
      ssh_group = self.ec2.create_security_group('ssh', 'Our ssh group')
      ssh_group.authorize('tcp', 22, 22, '0.0.0.0/0')    
    return
     
  def __GetTemplateImage(self):
    # maps region name to the template ami used to create mapr images
    #template_ami_id = util.GetUbuntuAmiForInstanceType(self.ami_spec.ubuntu_release_name, self.ami_spec.region_name, self.ami_spec.instance_type)
    
    template_ami_id = util.SearchUbuntuAmiDatabase(self.ami_spec.ubuntu_release_name, self.ami_spec.region_name, self.ami_spec.root_store_type, self.ami_spec.virtualization_type)
    
    template_images = self.ec2.get_all_images([template_ami_id])
    if (len(template_images) != 1):
      LOG(FATAL, 'could not locate template image: %s' % (template_images))
    template_image = template_images[0]      
    return template_image
   
  
  

  

                         