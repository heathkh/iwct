#!/usr/bin/env python
# pyglog v1.2 - A logging facility for Python.
# riesa@isi.edu (Jason Riesa)
import datetime
import os
import sys
import tempfile
import time
import traceback

# Set global logging constants
# INFO: Just print the log to stderr.
# FATAL: Print log and stack trace to stderr and abort.

INFO = 1
FATAL = -1
VLOG_LEVEL = 1

class InfoException(Exception):
  def __init__(self, msg):
    stack = traceback.extract_stack()
    self.module = stack[-3][0]
    self.line_number = stack[-3][1]
    self.msg = msg
    self.time = datetime.datetime.now()
  def __str__(self):
    return "%s%s %d  %s:%d] %s" %("I",self.time.strftime("%m%d%y %H:%M:%S.%f"), os.getpid(),
                                  os.path.basename(self.module), self.line_number, self.msg)

class FatalException(Exception):
  def __init__(self, msg, isCheck):
    stack = traceback.extract_stack()
    stack_depth = -3
    if isCheck:
      stack_depth = -4
    self.module = stack[stack_depth][0]
    self.line_number = stack[stack_depth][1]

    self.msg = msg
    self.time = datetime.datetime.now()
  def __str__(self):
    return "%s%s %d  %s:%d] %s" %("F",self.time.strftime("%m%d%y %H:%M:%S.%f"), os.getpid(),
                                  os.path.basename(self.module), self.line_number, self.msg)

def PrintStackTrace(stack):
  """ Print a stack trace in a compact, readable format
      similar to glog format.
  """
  for frame in stack:
    sys.stderr.write("\t@\t%s:%d\t%s\n" % (os.path.basename(frame[0])+"::"+frame[2], frame[1], frame[3]))

def SetLogLevel(level):
  global VLOG_LEVEL
  VLOG_LEVEL = level

def GetLogLevel():
  return VLOG_LEVEL

def LOG(code, msg, isCheck=False):
  """ Send msg to stderr. Abort if using code FATAL. """
  if VLOG_LEVEL == 0 or (VLOG_LEVEL > code and code != FATAL):
    return

  if code == FATAL:
    try:
      raise FatalException(msg, isCheck)
    except FatalException as e:
      sys.stderr.write("%s\n" %(e))
      sys.stderr.write("Stack trace:\n")
      PrintStackTrace(traceback.extract_stack())
      sys.stderr.write("Aborted\n")
      sys.exit(6)
  elif VLOG_LEVEL >= code:
    try:
      raise InfoException(msg)
    except InfoException as e:
      sys.stderr.write("%s\n" %(e))

def CHECK_EQ(obj1, obj2, msg=None):
  """ Abort with logging info if obj1 != obj2
  """
  if obj1 != obj2:
    if msg is None:
      msg = "%s != %s" % (str(obj1), str(obj2))
    LOG(FATAL, msg, isCheck=True)

def CHECK_NE(obj1, obj2, msg=None):
  """ Abort with logging info if obj1 == obj2
  """
  if obj1 == obj2:
    if msg is None:
      msg = "%s == %s" % (str(obj1), str(obj2))
    LOG(FATAL, msg, isCheck=True)

def CHECK_LT(obj1, obj2, msg=None):
  """ Abort with logging info if not (obj1 < obj2)
  """
  if obj1 >= obj2:
    if msg is None:
      msg = "%s >= %s" % (str(obj1), str(obj2))
    LOG(FATAL, msg, isCheck=True)

def CHECK_GT(obj1, obj2, msg=None):
  """ Abort with logging info if not (obj1 > obj2)
  """
  if obj1 <= obj2:
    if msg is None:
      msg = "%s <= %s" % (str(obj1), str(obj2))
    LOG(FATAL, msg, isCheck=True)

def CHECK_LE(obj1, obj2, msg=None):
  """ Abort with logging info if not (obj1 <= obj2)
  """
  if obj1 > obj2:
    if msg is None:
      msg = "%s > %s" % (str(obj1), str(obj2))
    LOG(FATAL, msg, isCheck=True)

def CHECK_GE(obj1, obj2, msg=None):
  """ Abort with logging info if not (obj1 >= obj2)
  """
  if obj1 < obj2:
    if msg is None:
      msg = "%s < %s" % (str(obj1), str(obj2))
    LOG(FATAL, msg, isCheck=True)

def CHECK_NOTNONE(obj, msg=None):
  """ Abort with logging info is obj is None """
  if obj is None:
    if msg is None:
      msg = "Check failed. Object is None."
    LOG(FATAL, msg, isCheck=True)

def CHECK(obj, msg=None):
  """ Abort with logging info is obj is False """
  if not obj:
    if msg is None:
      msg = "Check failed."
    LOG(FATAL, msg, isCheck=True)

def SetLogsOff():
  """ Turn all logging off."""
  SetLogLevel(0)

def SetLogsOn(level=1):
  """ Turn logging on with loglevel level."""
  SetLogLevel(level)

def InitLogging(level=1):
  """ Turn logging on with loglevel level. """
  SetLogLevel(level)
