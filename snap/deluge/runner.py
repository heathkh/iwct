#!/usr/bin/python
 
import sys
import pickle
import os
import traceback
import subprocess
from pyglog import *

def RunAndGetOutput(*popenargs, **kwargs):
  """
  Runs command, blocks until done, and returns the output.
  Raises exception if non-zero return value 
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


def main(argv):
  """ Runs a hadoop command line. """
  if len(argv) != 2:
    return 1
  cmd = argv[1]
  
  print 'pid: %s' % os.getpid()
  retval = 0 # success
  try:
    RunAndGetOutput(cmd)
  except Exception, e:
    print e
    traceback.print_exc(file=sys.stdout)
    retval = 1
  return retval

if __name__ == "__main__":
  main(sys.argv)


