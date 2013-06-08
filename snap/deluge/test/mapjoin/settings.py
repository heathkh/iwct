#!/usr/bin/python

import shutil
import os
import subprocess

num_mappers = 100
num_reducers = 100

app_name = 'test_mapjoin'

nfs_base='/mapr/my.cluster.com'
build_dir='/home/ubuntu/workspace/iwhadoop/build/pert4hadoop/pipes/test/mapjoin' 
  
def run_and_get_output(*popenargs, **kwargs):
  r"""Run command with arguments and return its output as a byte string.

  Backported from Python 2.7 as it's implemented as pure python on stdlib.

  >>> check_output(['/usr/bin/python', '--version'])
  Python 2.6.2
  """
  process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
  output, unused_err = process.communicate()
  retcode = process.poll()
  if retcode:
      cmd = kwargs.get("args")
      if cmd is None:
          cmd = popenargs[0]
      error = subprocess.CalledProcessError(retcode, cmd)
      error.output = output
      raise error
  return output    



def get_stage_path(stage_name):
    return '/%s/%s/' % (app_name, stage_name)


def get_nfs_path(path):
    return '%s/%s' % (nfs_base, path)

# path relative to default nfs root
def make_nfs_path(path):     
    try:
        os.makedirs(get_nfs_path(path))
    except:
      pass
    
    return True

def copy_local_to_nfs(local_path, path):
    dst = get_nfs_path(path)
    shutil.copy(local_path, dst)
    return True

def copy_to_stage_path(local_path, stage_name):
    return copy_local_to_nfs(local_path, get_stage_path(stage_name))

def stage_bin_path(stage_num):
    pipes_bin_path = '%s/app/%s/stage%02d/' % (build_dir, app_name, stage_num)        
    return pipes_bin_path 

def load_pipes_bin(bin_name, stage_name):
    pipes_bin_local_path = '%s/%s' % (build_dir, bin_name)    
    copy_local_to_nfs(pipes_bin_local_path, get_stage_path(stage_name))
    return True 
     

def init_stage_path(stage_name):
    stage_path = get_stage_path(stage_name)    
    shutil.rmtree(get_nfs_path(stage_path), ignore_errors=True)  
    make_nfs_path(stage_path)
    return stage_path

def run_pipes_job(bin_name, stage_name, input_path, output_path, num_map_jobs, num_reduce_jobs, extra_properties = dict()):
  
    stage_path = get_stage_path(stage_name)    
    pipes_bin = '%s/%s' % ( stage_path, bin_name)
    cmd = 'hadoop pipes '
    cmd += '-D mapred.job.name=%s_%s ' % (app_name, stage_name)
    cmd += '-D mapred.job.reuse.jvm.num.tasks=-1 '
    cmd += '-D mapred.map.tasks=%d ' % (num_map_jobs)
    for k,v in extra_properties.iteritems():
      cmd += '-D %s=%s ' % (k,v)      
    cmd += '-program maprfs://%s ' % (pipes_bin) 
    cmd += '-input %s ' % (input_path) 
    cmd += '-output %s ' % (output_path) 
    cmd += '-reduces %d ' % (num_reduce_jobs)
              
    output = run_and_get_output(cmd, shell=True)
    print output
    return
    


