#!/usr/bin/python

import settings
import logging

if __name__ == "__main__":   
  cmd = '%s/init' % (settings.build_dir)
  output = settings.run_and_get_output(cmd, shell=True)
  print output









