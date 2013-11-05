#!/usr/bin/python

""" This script installs gperf.  This is needed because there is no deb package available."""

import os
import urllib
import urllib2
from pyglog import *
import tarfile
import subprocess
#import sys
#import time



def ExecuteCmd(cmd, quiet=False):
  result = None
  if quiet:    
    with open(os.devnull, "w") as fnull:    
      result = subprocess.call(cmd, shell=True, stdout = fnull, stderr = fnull)
  else:
    result = subprocess.call(cmd, shell=True)
  return result

def EnsurePath(path):
  try:
    os.makedirs(path)
  except:
    pass
  return

def InstallLibunwind():
  url = 'http://download.savannah.gnu.org/releases/libunwind/libunwind-1.0.1.tar.gz'
  split = urllib2.urlparse.urlsplit(url)
  dest_filename = "/tmp/" + split.path.split("/")[-1]
  urllib.urlretrieve(url, dest_filename)
  CHECK(os.path.exists(dest_filename))
  tar = tarfile.open(dest_filename)
  tar.extractall('/tmp/')
  tar.close()
  src_path = '/tmp/libunwind-1.0.1/'
  CHECK(os.path.exists(src_path))
  cmd = 'cd %s && export CFLAGS="-U_FORTIFY_SOURCE -fPIC" && ./configure && make && sudo make install && sudo ldconfig' % (src_path)
  ExecuteCmd(cmd)
  return 0

def InstallPerformanceTools():
  url = 'http://gperftools.googlecode.com/files/gperftools-2.1.tar.gz'
  split = urllib2.urlparse.urlsplit(url)
  dest_filename = "/tmp/" + split.path.split("/")[-1]
  urllib.urlretrieve(url, dest_filename)
  CHECK(os.path.exists(dest_filename))
  tar = tarfile.open(dest_filename)
  tar.extractall('/tmp/')
  tar.close()
  src_path = '/tmp/gperftools-2.1/'
  CHECK(os.path.exists(src_path))
  #cmd = 'cd %s && export CCFLAGS=-fPIC && export CXXFLAGS=-fPIC && ./configure --enable-frame-pointers && make && sudo make install' % (src_path)
  cmd = 'cd %s && export CCFLAGS=-fPIC && export CXXFLAGS=-fPIC && ./configure && make && sudo make install && sudo ldconfig' % (src_path)
  ExecuteCmd(cmd)
  return 0


if __name__ == "__main__":
  InstallLibunwind()
  InstallPerformanceTools()  
