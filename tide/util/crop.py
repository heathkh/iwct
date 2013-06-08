#!/usr/bin/python

import os
import sys
import logging
from snap.google.base import py_base
from pert import py_pert
from tide import tide_pb2
from iw import iw_pb2
from progressbar import Bar, Percentage, ETA, ProgressBar #, Counter, , Percentage, ProgressBar, SimpleProgress, Timer
import Image
import ImageChops
import glob
import numpy
import StringIO
import math
from snap.pyglog import *


def IntersectRects(a_x, a_y, a_w, a_h, b_x, b_y, b_w, b_h):  
  intersection = None
  
  a_left = a_x
  a_right = a_x + a_w
  a_top = a_y
  a_bottom = a_y + a_h
  
  b_left = b_x
  b_right = b_x + b_w
  b_top = b_y
  b_bottom = b_y + b_h
  
  i_left = max(a_left, b_left)
  i_top = max(a_top, b_top)
  i_right = min(a_right, b_right)
  i_bottom = min(a_bottom, b_bottom)
  
  if i_right > i_left and i_bottom > i_top:
    i_x = i_left
    i_y = i_top
    i_w = i_right - i_left
    i_h = i_bottom - i_top
    intersection = i_x, i_y, i_w, i_h
  return intersection



class CropRect(object):
  def __init__(self, image_width, image_height, crop_fraction):
    crop_pixels = int(math.floor(crop_fraction * max(image_width, image_height)))
    CHECK_GE(crop_pixels, 0);
    # Setup a rectangle to define your region of interest
    new_width = image_width-2*crop_pixels
    new_height = image_height-2*crop_pixels;
    
    min_dim = 50
    if new_width < min_dim or new_height < min_dim:
      raise ValueError('Image dimensions do not allow requried crop operation: %d %d' % (image_width, image_height))
    
    self.x = crop_pixels
    self.y = crop_pixels
    self.w = new_width
    self.h = new_height
    return
 
  def ApplyCropToRect(self, in_x, in_y, in_w, in_h):
    # Check that input rect and crop rect have non-zero intersection, otherwise result is undefined
    intersection = IntersectRects(in_x, in_y, in_w, in_h, self.x, self.y, self.w, self.h)
    if not intersection:
      raise ValueError('rects do not intersect')
    
    i_x, i_y, i_w, i_h = intersection
    new_x1 = max(in_x - self.x, 0)
    new_y1 = max(in_y - self.y, 0)
    new_w = i_w
    new_h = i_h
    return  new_x1, new_y1, new_w, new_h     

