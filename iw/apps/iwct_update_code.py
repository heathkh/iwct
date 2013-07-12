#!/usr/bin/python

import os
import subprocess

def ExecuteCmd(cmd, quiet=False):
  """ Run a command in a shell. """
  result = None
  if quiet:
    with open(os.devnull, "w") as fnull:
      result = subprocess.call(cmd, shell=True, stdout=fnull, stderr=fnull)
  else:
    result = subprocess.call(cmd, shell=True)
  return result

def main():
  git_base_path = '/home/ubuntu/git/'
  repos = ['cirruscluster', 'iwct' , 'snap']
  
  for repo in repos:
    cmd = 'cd %s/%s && git pull origin' % (git_base_path, repo)
    ExecuteCmd(cmd)
  
  cmd = 'cd %s/iwct/build && make -j10' % (git_base_path)
  ExecuteCmd(cmd)
    
  return

if __name__ == "__main__":      
  main()
 
