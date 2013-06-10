"""
Classes for running sector sphere service on a cluster.
"""
#import re
from snap.cirrus import util
from snap.cirrus.cluster import mapr_image_manager
import urllib2
import ec2cluster
from snap.pyglog import *
import subprocess
import time
import os
from multiprocessing import Pool
from string import Template
import requests
from snap.pert import py_pert
from snap import ansible
from snap.ansible import callbacks
from snap.ansible import playbook
from snap.ansible import utils
from snap.ansible import inventory
from snap.ansible.utils import plugins
import re


class MaprClusterConfig(object):
  """
  MapR Service Configuration
  """
  def __init__(self):
    super(MaprClusterConfig, self).__init__()
    self.name = None
    self.mapr_ami_owner_id = None 
    self.instance_type = None    
    self.region = None
    self.authorized_client_cidrs = None
    self.mapr_version = None          
    self.zones = None # // # list of the placement zones within region that should be used to evenly distribute the nodes (ex ['a','b', 'e'])  
    return
  
class MaprCluster(object):
  """
  MapR Service
  """  
  def __init__(self, config):    
    self.cluster = ec2cluster.Ec2Cluster(config.configuration_name, config.region_name)    
    self.__CheckConfigOrDie(config)      
    self.config = config    
    self.scripts_dir = os.path.dirname(__file__) + '/scripts/'
    self.start_node_script = self.scripts_dir + 'start_node'
    self.default_volume_config_scirpt = self.scripts_dir + 'default_volume_config.sh'
    self.config_ganglia_master_script = self.scripts_dir + 'config_ganglia_master.sh'
    self.install_ganglia_master_script = self.scripts_dir + 'install_ganglia_master.sh'
    self.install_ganglia_worker_script = self.scripts_dir + 'install_ganglia_worker.sh'
    self.hadoop_conf_dir = '/opt/mapr/hadoop/hadoop-0.20.2/conf/'
    self.cluster_keypair_name = 'cirrus_cluster'
    config_bucketname = 'cirrus_cluster_config_%s' % (hashlib.md5(iam_aws_id).hexdigest())
    src_region = config.region_name
    dst_regions = util.tested_region_names
    self.ec2 = self.cluster._ec2connection
    self.s3 = S3Connection(aws_access_key_id=iam_aws_id, aws_secret_access_key=iam_aws_secret)
    self.ssh_key = util.InitKeypair(self.ec2, self.s3, config_bucketname, self.cluster_keypair_name, src_region, dst_regions)
    return
 
  def _GetAvailabilityZoneNameByIndex(self, index):
     CHECK_LT(index, len(self.config.zones))
     availability_zone_name = '%s%s' % (self.config.region_name , self.config.zones[index])
     return availability_zone_name
 
  def Create(self, num_instances):
    if self.__StartMaster():
      self.Resize(num_instances)
      self.PushConfig()
    return True
  
  def Resize(self, num_instances):
    num_spotworkers = len(self.__GetIpsFromCldb()) - 1    
    self.ConfigureClient() # must be done before add workers because it uses the nfs mount         
    if num_spotworkers < num_instances:
      num_to_add = num_instances - num_spotworkers
      LOG(INFO, 'num_to_add: %d' % (num_to_add)) 
      self.__AddWorkers(num_to_add)
    elif num_spotworkers > num_instances:  
      #num_to_remove = num_spotworkers - num_instances
      print num_spotworkers
      assert(False) # not yet implemented            
    self.__ConfigureGanglia()
    self.__FixCachePermissions()
    return
    
  def Destroy(self):
    instances = []
    instances.extend(self.cluster.get_instances_in_role("master", "running"))
    instances.extend(self.cluster.get_instances_in_role("spotworker", "running"))
    self.cluster.terminate_instances(instances)    
    return True
    
  def PushConfig(self):
    instances = self.__GetCldbRegisteredWorkerInstances()
    self.__ReconfigureTaskTrackers(instances)
    self.__ReconfigureJobTracker()    
    return
  
  def Reset(self):
    self.__CleanUpRootPartition()
    #master_instances = self.__GetMasterInstance()
    #self.__RunCommandOnInstances('sudo killall -9 maprcli', [master_instances])
    instances = self.__GetCldbRegisteredWorkerInstances()    
    #self.__RunCommandOnInstances('sudo service mapr-warden restart', instances)
    self.__RestartTaskTrackers(instances)
    self.__RestartJobTracker()
    return
  
  def ShowUiUrls(self):
    master_url = self.__GetMasterInstance().private_hostname
    mapr_ui_url = 'https://%s:8443' % (master_url)
    ganglia_ui_url = 'http://%s/ganglia' % (master_url)
    #cluster_cpu_url = 'http://%s/ganglia/graph.php?c=cirrus&h=%s&v=5&m=Cluster%%20Cpu%%20Busy%%20%%&r=hour&z=medium&jr=&js=&st=1362177694&ti=Cluster%%20Cpu%%20Busy%%20%%&z=large' % (master_url, master_url)
    cluster_cpu_url = 'http://%s/ganglia/graph.php?g=load_report&z=large&c=cirrus&m=Cluster%%20Memory%%20Used%%20MB&r=hour&s=descending&hc=4&mc=2&st=1362178130' % (master_url)
    cluster_ram_url = 'http://%s/ganglia/graph.php?g=mem_report&z=large&c=cirrus&m=Cluster%%20Memory%%20Used%%20MB&r=hour&s=descending&hc=4&mc=2&st=1362178130' % (master_url)
    cluster_net_url = 'http://%s/ganglia/graph.php?g=network_report&z=large&c=cirrus&m=network_report&r=hour&s=descending&hc=4&mc=2&st=1362180395' % (master_url) 
    print 'mapr_ui: %s' % (mapr_ui_url)
    print 'ganglia_ui_url: %s' % (ganglia_ui_url)
    print 'cluster_cpu_url: %s' % (cluster_cpu_url)
    print 'cluster_ram_url: %s' % (cluster_ram_url)
    print 'cluster_net_url: %s' % (cluster_net_url)
    return
  
  def __ReconfigureTaskTrackers(self, instances):
    #self.__CopyToInstancesRoot(self.scripts_dir+'mapred-site.xml', self.hadoop_conf_dir, instances)
    self.__ConfigureMapredSite(instances, self.GetNumCoresPerTaskTracker(), self.GetNumCoresPerTaskTracker()-1)
    time.sleep(1)
    self.__RestartTaskTrackers(instances)
    return

  def __ReconfigureJobTracker(self):
    master = self.__GetMasterInstance()
    self.__ConfigureMapredSite([master], self.GetNumCoresPerTaskTracker(), self.GetNumCoresPerTaskTracker()-1)
    time.sleep(1)
    self.__RestartJobTracker()
    return

  def GetNumCoresPerTaskTracker(self):
    #TODO(heathkh): simplify with max and min functions
    num_cores = None
    for _ in range(60):
      num_cores_list = self.GetProperty('cores_summary')
      CHECK(num_cores_list)  
      min_num_cores = min(num_cores_list)
      max_num_cores = max(num_cores_list)      
      if min_num_cores != max_num_cores:
        LOG(INFO, 'detected invalid number of cores... trying again: %s' % (num_cores_list))
        time.sleep(5)
      else:
        num_cores = max_num_cores
        break
    return num_cores
    
  def GetProperty(self, property_name):
    if property_name == 'slot_summary':    
      params = {}
      params['columns'] = 'service,ttmapSlots,ttReduceSlots'
      params['filter'] = '[service==tasktracker]'
      r = self.__MaprApi('node list', params)
      CHECK(r['status'], 'OK')
      slot_summary = {}   
      prefetch_maptasks = 1.0 #mapreduce.tasktracker.prefetch.maptasks
      for item in r['data']:
        host = item['ip']
        mapr_reported_map_slots = long(item['ttmapSlots']) # mapr doesn't report the number of map slots but: num_maps_slots + map num_maps_slots * mapreduce.tasktracker.prefetch.maptasks 
        map_slots = long(mapr_reported_map_slots / (1.0 + prefetch_maptasks))
        reudce_slots = long(item['ttReduceSlots'])
        slot_summary[host] = {}
        slot_summary[host]['map_slots'] = map_slots
        slot_summary[host]['reduce_slots'] = reudce_slots
      return slot_summary
    if property_name == 'cores_summary':    
      params = {}
      params['columns'] = 'cpus'
      params['filter'] = '[service==tasktracker]'
      r = self.__MaprApi('node list', params)
      CHECK(r['status'], 'OK')
      cores_summary = []   
      prefetch_maptasks = 1.0 #mapreduce.tasktracker.prefetch.maptasks
      CHECK('data' in r)
      for item in r['data']:
        host = item['ip']
        cpus = long(item['cpus']) # mapr doesn't report the number of map slots but: num_maps_slots + map num_maps_slots * mapreduce.tasktracker.prefetch.maptasks 
        cores_summary.append(cpus)
      return cores_summary
    if property_name == 'ram_summary':    
      params = {}
      params['columns'] = 'mtotal'
      params['filter'] = '[service==tasktracker]'
      r = self.__MaprApi('node list', params)
      CHECK(r['status'], 'OK')
      ram_summary = []   
      prefetch_maptasks = 1.0 #mapreduce.tasktracker.prefetch.maptasks
      for item in r['data']:
        host = item['ip']
        ram_megabytes = long(item['mtotal']) # mapr docs say ram is in GB but it seems to actually be in MB
        ram_gigabytes = int(ram_megabytes * 0.000976562)
        ram_summary.append(ram_gigabytes)
      return ram_summary    
    if property_name == 'rack_topology':    
      rack_topology, _ = self.__GetWorkNodeTopology()      
      return rack_topology
    else:
      LOG(FATAL, 'unknown property requested: %s' % (property_name)) 
    return None
  
  def SetNumMapReduceSlotsPerNode(self, num_map_slots, num_reduce_slots):
    # check if current configuration matches requested configuration
    slot_summary_data = self.GetProperty('slot_summary')
    current_settings_correct = True
    for host, host_data in slot_summary_data.iteritems():
      CHECK('map_slots' in host_data)    
      cur_map_slots_per_node = host_data['map_slots'] 
      if cur_map_slots_per_node != num_map_slots:
        print 'cur_map_slots_per_node: %d' %  cur_map_slots_per_node
        print 'num_map_slots: %d' %  num_map_slots        
        current_settings_correct = False
        break
    for host, host_data in slot_summary_data.iteritems():
      CHECK('reduce_slots' in host_data)    
      cur_reduce_slots_per_node = host_data['reduce_slots']
      if cur_reduce_slots_per_node != num_reduce_slots:
        print 'cur_reduce_slots_per_node: %d' %  cur_reduce_slots_per_node
        print 'num_reduce_slots: %d' %  num_reduce_slots        
        current_settings_correct = False
        break
    if current_settings_correct:      
      return True     
    else:
      print 'current slot config is not correct... need to reconfigure...'
    self.__ConfigureMapredSite(self.__GetAllInstances(), num_map_slots, num_reduce_slots )
    master = self.__GetMasterInstance()    
    self.__RestartTaskTrackers(self.__GetWorkerInstances())
    CHECK(self.__RestartJobTracker())
    # todo wait for job tracker
    master = self.__GetMasterInstance()
    job_tracker_url = 'http://%s:50030' % (master.private_ip)
    self.__WaitForUrlReady(job_tracker_url)
    return True

  def __ConfigureMapredSite(self, instances, num_map_slots, num_reduce_slots):
    if not instances:
      return
    map_slots_param = '%d' % (num_map_slots)
    reduce_slots_param = '%d' % (num_reduce_slots)
    # generate and distribute template
    filename = '/tmp/mapred-site.xml'
    conf_template = open(self.scripts_dir + 'mapred-site.xml.template').read()
    conf_file = open(filename, 'w')
    conf_file.write(Template(conf_template).substitute(map_slots_param=map_slots_param, reduce_slots_param=reduce_slots_param))
    conf_file.close()
    self.__CopyToInstancesRoot(filename, self.hadoop_conf_dir, instances)
    return
  
  def ConfigureLazyWorkers(self):
    """ Lazy workers are instances that are running and reachable but failed to 
    register with the cldb to join the mapr cluster. This trys to find these 
    missing workers and add them to the cluster. """
    lazy_worker_instances = self.__GetMissingWorkers()
    reachable_states = self.__AreInstancesReachable(lazy_worker_instances)
    reachable_instances = [t[0] for t in zip(lazy_worker_instances, reachable_states) if (t[1] == True)]
    print 'reachable_instances: %s' % reachable_instances 
    self.__ConfigureWorkers(reachable_instances)
    return

  def TerminateUnreachableInstances(self):
    unreachable_instances = util.GetUnreachableInstances(self.__GetWorkerInstances(), 
                                    self.config.private_key_file)    
    print 'unreachable_instances: '
    print unreachable_instances
    self.cluster.terminate_instances(unreachable_instances)
    return

  def Debug(self):
    #self.__CleanUpRootPartition()
    #self.__FixCachePermissions()
    #self.__ConfigureClient()
    #self.__ConfigureGanglia()
    self.__FixCachePermissions()
    #self.ConfigureLazyWorkers()
    #self.__CleanUpCoresAlarm()
    #self.__InstallTbb(self.__GetWorkerInstances())
    #self.TerminateUnreachableInstances()
    #self.__ConfigureMaster()
    #self.__ConfigureGanglia()
    #self.__ConfigureMaster()
    #self.__ConfigureWorkers(self.__GetWorkerInstances())
    
    #self.ConfigureClient()
    self.__SetWorkerTopology()
    self.__InstallSnappy(self.__GetWorkerInstances())
    
    #self.__EnableNfsServer()
    return
  
###############################################################################
## Private Methods
###############################################################################     
      
  def __CheckConfigOrDie(self, config):    
    CHECK(config.configuration_name)
    CHECK(config.mapr_ami_owner_id)
    CHECK(config.cluster_instance_type)
    CHECK(config.region_name)
    CHECK(config.mapr_version, 'config missing mapr_version: (e.g. v2.1.3) see http://package.mapr.com/releases/ ')
    CHECK_GE(len(config.zones), 1) # at least one zone must be specified
    tested_instance_types = ['cc2.8xlarge', 'c1.xlarge']
    CHECK(config.cluster_instance_type in tested_instance_types, 'this instance type has not been tested')
    CHECK_NE(config.cluster_instance_type, 'cr1.8xlarge', 'Currently not supported because mapr start_node perl script can not handle the fact that swap is bigger than ssd disk not to mention the required cache size.')
    valid_zones = ['a','b','c','d','e'] 
    for zone in config.zones:
      CHECK(zone in valid_zones) 
    return

  def __EnableNfsServer(self):    
    master = self.__GetMasterInstance()
    params = {}
    params['nfs'] = 'start'
    params['nodes'] = '%s' % (master.private_ip)     
    result = self.__MaprApi('node/services', params)    
    return result
  
  def __StartMaster(self):
    """ Starts a master node, waits for ssh, configures it, and starts services. """    
    num_masters = len(self.cluster.get_instances_in_role("master", "running"))
    CHECK_LT(num_masters, 1)
    LOG(INFO, "waiting for masters to start")          
    self.__LaunchSpotMasterInstances()
    time.sleep(1)
    self.__ConfigureMaster()
    return True
  
  def ConfigureClient(self):
    LOG(INFO, 'ConfigureClient')
    # try to start the nfs server and make sure it starts OK... 
    # if it doesn't ask the user to apply license and retry until success
    LOG(INFO, 'Enabling NFS server...')    
    while True: 
      result = self.__EnableNfsServer()
      if not result['status'] == 'OK':
        LOG(INFO, 'Please use web ui to apply M3 license in order to start nfs server.')
        sleep(5)
      else:
        break
    # tell local client to point at the master
    master_instance = self.__GetMasterInstance() 
    assert(master_instance)
    # TODO(kheath): allow the cluster name to be configure instead of hardwired MyCluster
    print 'configuring client: %s ' % master_instance.private_ip
    cmd = 'sudo rm -rf /opt/mapr/conf/mapr-clusters.conf; sudo /opt/mapr/server/configure.sh -N MyCluster -c -C %s:7222' % master_instance.private_ip
    print util.ExecuteCmd(cmd)
    unmount_nfs_cmd = 'sudo umount -l /mapr'
    mount_nfs_cmd = 'sudo mkdir /mapr; sudo chmod 777 /mapr; sudo mount %s:/mapr /mapr;' % (master_instance.public_hostname)
    perm_nfs_cmd = 'sudo chmod -R 777 /mapr/my.cluster.com/'
    # mount nfs locally
    LOG(INFO, 'unmounting nfs')
    util.ExecuteCmd(unmount_nfs_cmd)
    LOG(INFO, 'mounting nfs')
    util.ExecuteCmd(mount_nfs_cmd)
    LOG(INFO, 'setting nfs permissions')
    util.ExecuteCmd(perm_nfs_cmd)
    return

  def __SetupMasterTopology(self):
    master_instance = self.__GetMasterInstance()
    ip_to_id = self.__IpsToServerIds()
    CHECK(master_instance.private_ip in ip_to_id)
    master_id = ip_to_id[master_instance.private_ip]
    # move master node topology to /cldb 
    assert(master_instance)
    retval, response = self.__RunMaprCli('node move -serverids %s -topology /cldbonly' % master_id)
    # create data volume    
    retval, response = self.__RunMaprCli('volume create -name data -path /data -type 0 -mount 1')
    print retval, response
    return

  def __ConfigureMaster(self):
    master_instance = self.__GetMasterInstance() 
    assert(master_instance)
    self.__WaitForInstancesReachable([master_instance])
    self.__RunPostBootConfig([master_instance])
    self.__SetupAccessControl()
    # run configure on master
    # setup a user on master to allow login
    #retvals = self.__RunCommandOnInstances('sudo useradd -m -u 1024 %s; echo "%s:%s" | sudo chpasswd' % (self.default_user, self.default_user, self.default_user), [master_instance] )
    self.__EnableRootUser([master_instance]) 
    CHECK(self.__RunScriptOnInstance(self.start_node_script, '-n %s -z %s' % (master_instance.private_ip, master_instance.private_ip), master_instance))
    self.__WaitForMasterReady()    
    # Wait till setup of default user succeeds
    num_attempts = 0 
    while True:
      #retval, response = self.__RunMaprCli('acl edit -type cluster -user %s:fc' % self.default_user)
      retval, response = self.__RunMaprCli('acl edit -type cluster -user root:fc')
      print retval, response
      if retval == 0:
        print 'MaprCliReady!'
        break
      # this is a hack... sometimes the start node script fails to get the cldb service running... This pokes it if it is not responding..
      self.__RunCommandOnInstances('sudo service mapr-cldb restart', [master_instance])
      num_attempts += 1      
      time.sleep(5*num_attempts)
    
    # This is a hack because I can't figure out how to do the quotation escape to pass the jason bit...
    self.__RunScriptOnInstances(self.default_volume_config_scirpt, '' , [master_instance])  
    self.__SetupMasterTopology()
           
    # instruct the user to log in to web ui and install license and start nfs service
    web_ui_url = self.__GetWebUiUrl()
    print 'log in to web ui and install license and start nfs service'
    print 'url: %s' % (web_ui_url)
    raw_input() # wait for manual steps before continuing
    
    # TODO: for automating cluster reg, the web ui submits this POST command
    #http://www.mapr.com/index.php?option=com_comprofiler&task=savecluster&Itemid=0&clusterId=3858970976225177413&clusterName=my.cluster.com
    # next need to call REST method to auto-add licences from web
    self.__ConfigureGanglia()
    return
  
  def __CopyToInstancesRoot(self, local_path, remote_final_dir, instances):
    if not instances:
      return
    remote_tmp_dir = '/home/ubuntu/'
    self.__CopyToInstances(local_path, remote_tmp_dir, instances)
    time.sleep(1)
    leaf = os.path.basename(local_path)
    mv_cmd = 'sudo mv %s %s' % (remote_tmp_dir+leaf, remote_final_dir)
    self.__RunCommandOnInstances(mv_cmd, instances)
    return

  def __ConfigureGanglia(self):
    master_instance = self.__GetMasterInstance() 
    assert(master_instance)
    # This is a hack because I can't figure out how to do the quotation escape to pass the json bit...
    self.__RunScriptOnInstances(self.config_ganglia_master_script, '' , [master_instance])
    # render config templates 
    gmetad_conf_template = open(self.scripts_dir + 'gmetad.conf.template').read()
    conf_file = open('gmetad.conf', 'w')
    conf_file.write(Template(gmetad_conf_template).substitute(cluster_name='cirrus'))
    conf_file.close()
    worker_gmond_conf_template = open(self.scripts_dir + 'gmond.conf.template').read()
    conf_file = open('./gmond.conf', 'w')
    conf_file.write(Template(worker_gmond_conf_template).substitute(cluster_name='cirrus', master_ip=master_instance.private_ip))
    conf_file.close()
    # copy gmetad conf to master  
    self.__CopyToInstancesRoot('gmetad.conf', '/etc/ganglia/gmetad.conf', [master_instance])
    self.__RunCommandOnInstances('sudo service ganglia-monitor restart', [master_instance])
    self.__RunCommandOnInstances('sudo service gmetad restart', [master_instance])
    self.__RunCommandOnInstances('sudo service apache2 restart', [master_instance])
    # copy gmond.conf to all nodes
    instances = self.__GetAllInstances()
    if instances:
      self.__CopyToInstancesRoot('gmond.conf', '/etc/ganglia/gmond.conf', instances)
      self.__RunCommandOnInstances('sudo service ganglia-monitor restart', instances)
    return

  def __GetWebUiUrl(self):
    master_instance = self.__GetMasterInstance()
    web_ui_url = 'https://%s:8443' % (master_instance.public_hostname)
    return web_ui_url

  def __IsWebUiReady(self):      
    #TODO rewrite to use     __IsUrlLive   
    web_ui_ready = False                
    web_ui_url = self.__GetWebUiUrl()      
    try:
      print 'testing: %s' % (web_ui_url) 
      util.UrlGet(web_ui_url)        
      web_ui_ready = True
    except urllib2.URLError as e:
      print e      
    return web_ui_ready
  
  def __IsUrlLive(self, url):            
    ready = False                        
    try:
      print 'testing: %s' % (url) 
      util.UrlGet(url)        
      ready = True
    except:
      print '.'
      #LOG(INFO, 'error checking if url is live: %s' % (url))
      
    return ready 

  def __WaitForUrlReady(self, url):
    print 'waiting for url to be ready...'
    ready = self.__IsUrlLive(url)
    while not ready:
      time.sleep(5)
      ready = self.__IsUrlLive(url)           
    return

  def __WaitForMasterReady(self):
    print 'waiting for web ui to be ready...'
    master_instance = self.__GetMasterInstance()
    web_ui_ready = self.__IsWebUiReady()
    count = 0
    while True:
      time.sleep(10)
      if self.__IsWebUiReady():
        break
      
      count += 1
      if count > 12:
        count = 0
        # this is a hack... sometimes the start node script fails to get the cldb and the web service running... This pokes it if it is not responding..
        self.__RunCommandOnInstances('sudo service mapr-warden restart', [master_instance])
        time.sleep(30)        
        self.__RunCommandOnInstances('sudo /opt/mapr/adminuiapp/webserver start ', [master_instance])
        time.sleep(30)                              
    return


  def __SetupAccessControl(self):
    # modify security group to allow this machine (i.e. those in dev group) to connect to cluster    
    client_security_group = self.cluster._ec2Connection.get_all_security_groups(groupnames=[self.config.workstation_security_group])[0] 
    cluster_security_group = self.cluster._ec2Connection.get_all_security_groups(groupnames=[self.config.configuration_name])[0]                 
    cluster_security_group.revoke(src_group=client_security_group) # be sure this group not yet authorized, or next command fails
    cluster_security_group.authorize(src_group=client_security_group)
    return


  def __AddWorkers(self, num_to_add):
    """ Adds workers evenly across all enabled zones."""                
    # Check preconditions
    assert(self.__IsWebUiReady())
    zone_to_ips = self.__GetZoneToWorkerIpsTable()    
    zone_old_new = []
    for zone, ips in zone_to_ips.iteritems():
      num_nodes_in_zone = len(ips) 
      num_nodes_to_add = 0
      zone_old_new.append((zone, num_nodes_in_zone, num_nodes_to_add))    
    print 'num_to_add %s' % num_to_add
    for _ in range(num_to_add):
      zone_old_new.sort(key= lambda z : z[1]+z[2])
      zt = zone_old_new[0]
      zone_old_new[0] = (zt[0], zt[1], zt[2]+1)
      
    #print zone_old_new
    
    number_zone_list = [(zt[2], zt[0]) for zt in zone_old_new]
    
    print 'resize plan'
    print number_zone_list
    
    new_worker_instances = self.__LaunchSpotWorkerInstances(number_zone_list)    
    self.__WaitForInstancesReachable(new_worker_instances)
    
    self.__ConfigureWorkers(new_worker_instances)
            
    return
  
  def __EnableRootUser(self, instances):  
    # for ubuntu, need to enable root account by setting a password
    ubuntu_enable_root_cmd = 'echo "root:mapr" | sudo chpasswd'
    retvals = self.__RunCommandOnInstances(ubuntu_enable_root_cmd, instances)
    print retvals
    
    # generate ssh key for root user
    self.__RunCommandOnInstances("sudo rm -rf /root/.ssh/; sudo ssh-keygen -t rsa -N '' -f /root/.ssh/id_rsa; sudo touch /root/.ssh/authorized_keys", instances)    
    return
  
#  def __TerminateUnreachableInstances(self, instances):
#    unreachable_instances = util.GetUnreachableInstances(instances, self.config.private_key_file)
#    print 'unreachable_instances: %s' % (unreachable_instances)
#    self.cluster.ec2.terminate_instances(instances)
#    return
    
    
  
  def __ConfigurePasswordlessSSH(self, instances):
    # Setup passwordless SSH    
    self.__EnableRootUser(instances)
    
    # get copy of master root public key
    master_instance = self.__GetMasterInstance()
    cmd = util.MakeRemoteExecuteCmd('sudo cat /root/.ssh/id_rsa.pub' , master_instance.public_hostname, self.config.private_key_file)
    pub_key = None
    #retval = None
    
    while pub_key == None:
      try:
        _, pub_key = util.CheckOutput(cmd, shell=True) # work around because above        
      except subprocess.CalledProcessError as e:
        print 'Can not get public key from master instance... Will try again... %s' % (master_instance.private_ip)
        time.sleep(2)
    
    pub_key = pub_key.strip()
    
    print pub_key
    assert(pub_key)
    
    append_auth_cmd = """sudo sh -c 'echo %s >> /root/.ssh/authorized_keys'""" % (pub_key)
    self.__RunCommandOnInstances(append_auth_cmd, instances)    
    return
  
  def __GetIpsFromCldb(self):
    """ Gets ip of workers that are live in cldb """    
    ip_to_id = self.__IpsToServerIds()
    return ip_to_id.keys()
    
    
  def __GetMissingWorkers(self):
    cldb_ip_set = set(self.__GetIpsFromCldb())
    instances = self.__GetWorkerInstances()
    missing_workers = []
    for instance in instances:
      if instance.private_ip not in cldb_ip_set:
        LOG(INFO, 'we have a worker that is running but not in cldb: %s' % (instance))
        missing_workers.append(instance)
      
    return missing_workers  
  
  
  
  
  def __GetZoneForWorker(self, worker_ip):
    cldb_ip_set = set(self.__GetIpsFromCldb())
    raw_instances = self.cluster._get_instances(self.cluster._group_name_for_role('spotworker'), 'running')
    
    # group workers by zone
    workers_zone = None
    for raw_instance in raw_instances:
      zone_name = raw_instance.placement
      ip = raw_instance.private_ip_address
      if ip == worker_ip:
        workers_zone = zone_name
        break     
    return workers_zone
  
  
  def __GetZoneToWorkerIpsTable(self):
    
    cldb_ip_set = set(self.__GetIpsFromCldb())
    
    raw_instances = self.cluster._get_instances(self.cluster._group_name_for_role('spotworker'), 'running')
    zone_to_ip = {}
    
    for i, zone in enumerate(self.config.zones):
      zone_name = self._GetAvailabilityZoneNameByIndex(i)
      zone_to_ip[zone_name] = []
    
    # group workers by zone
    for raw_instance in raw_instances:
      zone_name = raw_instance.placement
      ip = raw_instance.private_ip_address
      
      if ip not in cldb_ip_set:
        LOG(INFO, 'we have a worker that is running but not in cldb: %s' % (ip))
        continue
      
      if zone_name not in zone_to_ip:
        LOG(FATAL, 'unexpected condition')
        #zone_to_ip[zone] = []
      zone_to_ip[zone_name].append(ip)
    return zone_to_ip  
  
  
  def __GetWorkNodeTopology(self):
    params = {'columns' : 'racktopo' }
    result = self.__MaprApi('node/list', params)
    valid_cldb_topology_re = re.compile(r"^/cldbonly/.+$")
    valid_rack_topology_re = re.compile(r"^(/data/us-.*/rack[0-9]{3})/.+$")
    rack_to_nodes = {}
    workers_outside_racks = []
    for d in result['data']:
      ip = d['ip']
      node_topo = d['racktopo']
      #print ip, node_topo
      match = valid_rack_topology_re.match(node_topo)
      if match:
        rack = match.groups()[0]
        if not rack in rack_to_nodes:
          rack_to_nodes[rack] = []
        rack_to_nodes[rack].append(ip)          
      else:
        if not valid_cldb_topology_re.match(node_topo):
          workers_outside_racks.append(ip)
    return rack_to_nodes, workers_outside_racks
  

  def __ParseRackTopology(self, input_topology):
    rack_topology_parse_re = re.compile(r"^/data/(us-.*)/rack([0-9]{3})$")
    match = rack_topology_parse_re.match(input_topology)
    rack_zone = None
    rack_id = None
    if match:
      rack_zone = match.groups()[0]
      rack_id = int(match.groups()[1])      
    return rack_zone, rack_id
  
  
  def __ChangeNodeToplogy(self, server_id, new_topology):
    topology = '/data/'
    params = {'serverids' : server_id, 
              'topology' : new_topology}
    result = self.__MaprApi('node/move', params)
    CHECK_EQ(result['status'], 'OK')
    return 
  
    
  @util.RetryUntilReturnsTrue(tries=10)
  def __CreateCacheVolume(self, new_rack_toplogy):
    LOG(INFO, '__CreateCacheVolume()')
    # set the desired topology for the new volume
    params = {'values' : '{"cldb.default.volume.topology":"%s"}' % new_rack_toplogy }
    result = self.__MaprApi('config/save', params)
    if result['status'] != 'OK':
      LOG(INFO, 'error')
      return False
    
    rack_zone, rack_id = self.__ParseRackTopology(new_rack_toplogy)
    CHECK(rack_zone != None)
    volume_name = 'data_deluge_cache_%s_rack%03d' % (rack_zone, rack_id)
    mount_path_parent = '/data/deluge/cache/%s' % (rack_zone)
    mount_path = '%s/rack%03d' % (mount_path_parent, rack_id)
    
    #print volume_name
    #print mount_path
    parent_uri = str('maprfs://' + mount_path_parent).encode('ascii','ignore')
    print parent_uri
    if not py_pert.Exists(parent_uri):
     LOG(INFO, 'creating deluge cache dir: %s' % (parent_uri)) 
     if not py_pert.MakeDirectory(parent_uri):
       LOG(INFO, 'failed to create dir: %s' % (parent_uri))
       return False
     
    CHECK(py_pert.Exists(parent_uri)) 
    LOG(INFO, 'Deluge cache dir OK: %s' % (parent_uri))
    
    # create the new cache volume
    params = {'name' : volume_name,              
              'type' : 0,
              'path' : mount_path,
              'mount' : 1,
              'replication' : 6,
              'replicationtype' : 'low_latency',              
              }
    LOG(INFO, 'about to create volume: %s' % (params))
    result = self.__MaprApi('volume/create', params)
    CHECK_EQ(result['status'], 'OK', result)
    
    LOG(INFO, 'about to configure client')    
    self.ConfigureClient() # nfs must be active before next commands can work 
    time.sleep(30)
    # set permisions
    LOG(INFO, 'about to fix cache permissions...')
    perm_nfs_cmd = 'sudo chmod -R 777 /mapr/my.cluster.com/%s' % (mount_path)
    util.ExecuteCmd(perm_nfs_cmd)  
      
    # reset to the default topology /inactive
    params = {'values' : '{"cldb.default.volume.topology":"/inactive"}' }
    result = self.__MaprApi('config/save', params)
    if result['status'] != 'OK':
      LOG(INFO, 'error')
      return False    
    return True
    
    
  def __SetWorkerTopology(self):
    ip_to_id = self.__IpsToServerIds()
    
    # get current state of the cluster topology
    rack_to_nodes, workers_outside_racks = self.__GetWorkNodeTopology()
    
    # verify all current racks have at most 6 nodes
    for rack, rack_nodes in rack_to_nodes.iteritems():
      CHECK_LE(len(rack_nodes), 6)
    
    # check if there are any workers that need to be placed into a rack
    new_racks = []
    for worker_ip in workers_outside_racks:    
      LOG(INFO, 'trying to place worker in a rack: %s' % worker_ip)  
      worker_zone = self.__GetZoneForWorker(worker_ip)
      
      # check if there are any racks in that zone that have fewer than 6 nodes
      available_rack = None
      for rack, rack_nodes in rack_to_nodes.iteritems():
        rack_zone, rack_id = self.__ParseRackTopology(rack)
        CHECK(rack_zone != None)
        CHECK(rack_id != None)
        CHECK_EQ(rack_zone, worker_zone)
        if rack_zone == worker_zone and len(rack_nodes) < 6:
          available_rack = rack
          break
      
      # if not, we need to create a new rack         
      if available_rack == None:        
        max_rack_id_in_workers_zone = 0                
        for rack, rack_nodes in rack_to_nodes.iteritems():
          rack_zone, rack_id = self.__ParseRackTopology(rack)        
          if rack_zone == worker_zone:
            max_rack_id_in_workers_zone = max(max_rack_id_in_workers_zone, rack_id)        
        new_rack_id = max_rack_id_in_workers_zone + 1
        new_rack_topology = '/data/%s/rack%03d' % (worker_zone, new_rack_id)
        new_racks.append(new_rack_topology)
        available_rack = new_rack_topology
        
      CHECK(available_rack) 
      
      server_id = ip_to_id[worker_ip]
      self.__ChangeNodeToplogy(server_id, available_rack)
      
      # update current state of the cluster topology
      rack_to_nodes, _ = self.__GetWorkNodeTopology()
    
    
    # get final state of the cluster topology
    rack_to_nodes, workers_outside_racks = self.__GetWorkNodeTopology()    
    #print rack_to_nodes
    #print workers_outside_racks
    
    # verify all workers have been placed in a rack
    CHECK_EQ(len(workers_outside_racks), 0)
    
    # create deluge cache volumes for any newly create racks
    time.sleep(60) # this seems to fail... try waiting to see if there may be a race condition causing segfault in maprfs    
    for new_rack in new_racks:
      self.__CreateCacheVolume(new_rack)
        
      
    return
  
  def __ConfigureWorkers(self, worker_instances):
    if not worker_instances:
      return
    master_instance = self.__GetMasterInstance()    
    self.__RunPostBootConfig(worker_instances)    
    self.__ConfigurePasswordlessSSH(worker_instances)    
    self.__RunScriptOnInstances(self.start_node_script, '-n %s -z %s' % (master_instance.private_ip, master_instance.private_ip), worker_instances)
    self.__ReconfigureTaskTrackers(worker_instances)
    self.__SetWorkerTopology()
    self.__InstallSnappy(worker_instances)
    return 

  def __MaprApi(self, command, params):
    #LOG(INFO, '__MaprApi')
    master_instance = self.__GetMasterInstance()    
    command_list = command.split(' ')
    command_path = '/'.join(command_list)          
    mapr_rest_api_url = 'https://%s:8443/rest/%s' % (master_instance.private_ip, command_path)
    
    r = None
    while r == None:
      try:     
        r = requests.post(mapr_rest_api_url, params=params, auth=('root', 'mapr'), verify=False)     # the verify requires requests versions >= 2.8
      except:
        LOG(INFO, 'error!!!')
        time.sleep(1)
    #print r.url  
    if r.status_code == 404:
      LOG(FATAL, '\ninvalid api call: %s.\nReview docs here for help: http://mapr.com/doc/display/MapR/API+Reference' % (r.url))      
    return r.json()
    
  
  def __RunMaprCli(self, cmd_str):
    master_instance = self.__GetMasterInstance()
    cmd = util.MakeRemoteExecuteCmd('sudo maprcli %s' % (cmd_str), master_instance.public_hostname, self.config.private_key_file)
    response = None
    retval = None
    try:
      retval, response = util.CheckOutput(cmd, shell=True) # work around because above
    except subprocess.CalledProcessError as e:
      print e
    
    return retval, response

  def __RestartTaskTrackers(self, instances):       
    node_list = ','.join([i.private_hostname for i in instances])  
    params = {'nodes': node_list, 'tasktracker' : 'restart'}
    r = self.__MaprApi('node/services', params)
    CHECK_EQ(r['status'], 'OK')
    return True


#  def __RestartTaskTrackers(self, instances):        
#    node_list = ''    
#    for instance in instances:        
#      node_list += ' %s' % (instance.private_hostname)
#    
#    cmd = 'node services -nodes %s -tasktracker restart' % (node_list)      
#    retval, response = self.__RunMaprCli(cmd)    
#    return (retval == 0)
  
  
  def __RestartJobTracker(self):      
    # restart job tracker
    master_instance = self.__GetMasterInstance()
    cmd = 'node services -nodes %s -jobtracker restart' % (master_instance.private_hostname)      
    retval, response = self.__RunMaprCli(cmd)
    return (retval == 0)
  
  
  def __InstancesToHostnames(self, instances):
    hostnames = [instance.public_hostname for instance in instances]
    return hostnames
  
  
  def __WaitForInstancesReachable(self, instances):        
    util.WaitForHostsReachable(self.__InstancesToHostnames(instances),
                                self.config.private_key_file)
    return
  
  def __AreInstancesReachable(self, instances):
    return util.AreHostsReachable(self.__InstancesToHostnames(instances),
                           self.config.private_key_file)
  
  def __RunCommandOnInstances(self, cmd, instances):    
    return util.RunCommandOnHosts(cmd, 
                          self.__InstancesToHostnames(instances), 
                          self.config.private_key_file)
    
  
  
  def __RunScriptOnInstances(self, script, script_args, instances):    
    return util.RunScriptOnHosts(script, script_args, 
                          self.__InstancesToHostnames(instances), 
                          self.config.private_key_file)
    
  
  def __RunScriptOnInstance(self, script, script_args, instance):   
    return util.RunScriptOnHost(script, script_args, 
                          instance.public_hostname, 
                          self.config.private_key_file)
    
  
  def __CopyToInstances(self, src_path, dst_path, instances):
    return util.CopyToHosts(src_path, dst_path, 
                            self.__InstancesToHostnames(instances), 
                            self.config.private_key_file)
  
 
      
  def __LaunchSpotMasterInstances(self):        
    # Compute a good bid price based on recent price history
    prefered_master_zone = self._GetAvailabilityZoneNameByIndex(0)
    num_days = 1
    cur_price = self.cluster.get_current_spot_instance_price(self.config.cluster_instance_type, prefered_master_zone)    
    recent_max_price = self.cluster.get_recent_max_spot_instance_price(self.config.cluster_instance_type, prefered_master_zone, num_days)    
    high_availability_bid_price = recent_max_price + 0.10     
    print 'current price: %f' % (cur_price)
    print 'high_availability_bid_price: %f' % (high_availability_bid_price)
    CHECK_LT(cur_price, 0.35) # sanity check so we don't do something stupid
    CHECK_LT(high_availability_bid_price, 1.25) # sanity check so we don't do something stupid

    # get the ami preconfigured as a master mapr node
    role = 'master'
    ami = LookupCirrusAmi(self.ec2, instance_type, ubuntu_release_name, mapr_version, role, ami_owner_id)
    CHECK(ami)    
    number_zone_list = [(1, prefered_master_zone)] 
    ids = self.cluster.launch_and_wait_for_spot_instances(price=high_availability_bid_price,
                                             role='master',                                             
                                             image_id=ami.id,
                                             instance_type=self.config.cluster_instance_type,
                                             private_key_name=self.cluster_keypair_name,
                                             number_zone_list=number_zone_list)
    return ids

  def __LaunchSpotWorkerInstances(self, number_zone_list): 
    max_zone_price = 0
    for i, zone in enumerate(self.config.zones):
      cur_zone_price = self.cluster.get_current_spot_instance_price(self.config.cluster_instance_type, self._GetAvailabilityZoneNameByIndex(i))
      LOG(INFO, '%s %f' % (zone, cur_zone_price))
      max_zone_price = max(max_zone_price, cur_zone_price)
    bid_price = max_zone_price + 0.10
    assert(bid_price < 0.5) # safety check!     
    print 'bid_price: %f' % (bid_price) 
    role = 'worker'
    ami = LookupCirrusAmi(self.ec2, instance_type, ubuntu_release_name, mapr_version, role, ami_owner_id)
    CHECK(ami)
    new_instances = self.cluster.launch_and_wait_for_spot_instances(price=bid_price,
                                             role='spotworker',
                                             image_id=ami.id,
                                             instance_type=self.config.cluster_instance_type,
                                             private_key_name=self.cluster_keypair_name,
                                             number_zone_list=number_zone_list)
    return new_instances

  def __GetMasterInstance(self):
    master_instance = None
    instances = self.cluster.get_instances_in_role("master", "running")
    if instances:
      assert(len(instances) == 1)
      master_instance = instances[0]     
    return master_instance
  
  def __GetAllInstances(self):    
    instances = self.cluster.get_instances()
    return instances
    
  def __GetWorkerInstances(self):
    instances = self.cluster.get_instances_in_role("spotworker", "running")
    return instances
  
  def __GetCldbRegisteredWorkerInstances(self):
    all_instances = self.__GetWorkerInstances()
    cldb_active_ips = set(self.__GetIpsFromCldb())
    instances = [i for i in all_instances if i.private_ip in cldb_active_ips]
    return instances
        
  def __IpsToServerIds(self):
    """ Get list of mapping of ip address into a server id"""
    master_instance = self.__GetMasterInstance()
    assert(master_instance)
    retval, response = self.__RunMaprCli('node list -columns id')
    ip_to_id = {}
    for line_num, line in enumerate(response.split('\n')):
      tokens = line.split()
      if len(tokens) == 3 and tokens[0] != 'id':        
        instance_id = tokens[0]
        ip = tokens[2]
        ip_to_id[ip] = instance_id
    return ip_to_id
    
  def __FixCachePermissions(self):    
    #TODO(kheath): tried to move this to the image generation stage, but these paths don't exist after mapr install... curious... just do it now... OK... these are on the ephemeral devices and create after instance boot... that's why!!!
    instances = self.__GetAllInstances()
    mapr_cache_path = '/opt/mapr/logs/cache'
    cmd = "sudo mkdir -p %s; sudo chmod -R 755 %s; sudo mkdir %s/hadoop/; sudo mkdir %s/tmp; sudo chmod -R 1777 %s/tmp" % (mapr_cache_path, mapr_cache_path, mapr_cache_path, mapr_cache_path, mapr_cache_path)
    self.__RunCommandOnInstances(cmd, instances)
    return  
    
  def __CleanUpCoresAlarm(self):
    """ Mapr signals an alarm in the web ui when there are files in a nodes /opt/cores dir.  This is anoying because we can't see if there is a real problem with the node because of this pointless error message.  This removes those files so the alarm goes away."""
    instances = self.__GetAllInstances()
    clean_cores_cmd = """sudo sh -c 'rm /opt/cores/*'"""
    self.__RunCommandOnInstances(clean_cores_cmd, instances)
    return 
  
  def __CleanUpRootPartition(self):
    """ Mapr signals an alarm in the web ui when there are files in a nodes /opt/cores dir.  This is anoying because we can't see if there is a real problem with the node because of this pointless error message.  This removes those files so the alarm goes away."""
    instances = self.__GetAllInstances()
    clean_cores_cmd = """sudo sh -c 'rm -rf /opt/mapr/cache/tmp/*'"""
    self.__RunCommandOnInstances(clean_cores_cmd, instances)
    return   
    
  
