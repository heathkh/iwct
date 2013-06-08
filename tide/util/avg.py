#!/usr/bin/env python
import Image
import ImageChops
import glob
import numpy


def main():
  
  input_dir = '/home/heathkh/Desktop/tide_export/pos_jpeg/tide_v00/starbucks_logo/*'
  filenames = glob.glob(input_dir)
  
  width = 200
  height = 200
  avg_image = None
  for index, file in enumerate(filenames):    
    input_image = Image.open(file)  
    input_image = input_image.convert("RGB")
    
    # accumulate average
    resized_image = input_image.resize((width, height), Image.NEAREST)      # use nearest neighbour
    
    if not avg_image:
      avg_image = resized_image
    
    print avg_image.size
    print resized_image.size
    #print resized_image
    
    alpha = 1.0/(index+1.0)
    
    avg_image = ImageChops.blend(avg_image, resized_image, alpha)
    
    print alpha
    
    if index == 2:
      break
    
  pix = numpy.array(avg_image)
  print pix
    
  avg_image.show()
    
  
    

if __name__ == "__main__":
   main()



