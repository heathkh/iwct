#!/usr/bin/python
import tornado.ioloop
import tornado.web
from iw.eval.labelprop.eval1 import eval1_pb2
from iw.eval.labelprop.eval1 import py_eval1
from pyglog import *
from iw import visutil
import json
import numpy as np
from tide import tide

class MainHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app      
      return            
    
    def get(self):
      params = {'app' : self.app }
      self.render('main.html', **params)
      return


class ConfusionMatrixHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app      
      return    
    
    def get(self):            
      pretty_label_names = [ l.replace('_', ' ') for l in self.app.result.label_names]            
      # generate json that maps the node index to the node name (here the image id) and node group (here the tide object id)
      result = self.app.result
      num_labels = self.app.num_labels
      matrix_headers = []
      for i in range(num_labels):        
        matrix_headers.append({'name': pretty_label_names[i], 'precision' : result.phases[-1].object_precision.mean[i], 'recall' : result.phases[-1].object_recall.mean[i] })
  
      # compute row and col sums
      cm = np.mat(result.phases[-1].confusion_matrix.mean).reshape(num_labels,num_labels)
      max_val = np.max(cm)      
      row_sums = np.squeeze(np.array(np.sum(cm, 1)))
      col_sums = np.squeeze(np.array(np.sum(cm, 0)))
      
      # generate confusion matrix (with different normalizations)
      matrix_unnorm = []
      matrix_precision = []
      matrix_recall = []      
      for row_id in range(num_labels):
        row_unnorm = []
        row_precision = []
        row_recall = []
        for col_id in range(num_labels):
          mean_image_count = result.phases[-1].confusion_matrix.mean[num_labels*row_id + col_id]
          value_unnormalized = mean_image_count/max_val
          value_precision = 0
          value_recall = 0
          if row_id < num_labels-1:
            value_precision = mean_image_count / row_sums[row_id]          
          if col_id < num_labels-1:
            value_recall = mean_image_count / col_sums[col_id]        
          row_unnorm.append(value_unnormalized)
          row_precision.append(value_precision)
          row_recall.append(value_recall)          
        matrix_unnorm.append(row_unnorm)
        matrix_precision.append(row_precision)
        matrix_recall.append(row_recall)
      
      size = 800
      params = { 'size' : size,
                 'matrix_headers' : json.dumps(matrix_headers), 
                 'matrix_unnorm' : json.dumps(matrix_unnorm),
                 'matrix_precision' : json.dumps(matrix_precision),
                 'matrix_recall' : json.dumps(matrix_recall),
                }
      self.render('confusion_matrix.html', **params)
      return
    

class ConfusionMatrixCellHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app      
      return    
    
    def get(self):      
      # parse arguments
      row = int(self.get_argument('row'))
      col = int(self.get_argument('col'))
      result = self.app.result
      predicted_label = result.label_names[row]
      actual_label = result.label_names[col]
            
      # get list of vertices in that cell
      images = result.phases[-1].confusion_matrix_item_freq.rows[row].cells[col].items
      images = images[0:50] # limit to top 50
      image_ids = [image.item_id for image in images]
      imageid_to_datauri = self.app.GetImageLoader().BatchGetImagesAsDataUri(image_ids)
      params = { 'actual_label' : actual_label,
                 'predicted_label' : predicted_label,
                 'images' : images,
                 'imageid_to_datauri' : imageid_to_datauri,
                 'labelid_to_labelnames' : result.label_names
                } 
      self.render('confusion_matrix_cell.html', **params)      
      return



class ImageHandler(tornado.web.RequestHandler):    
    def initialize(self, app):
      self.app = app      
      return    
    
    def get(self):      
      # parse arguments
      image_id = self.get_argument('id', default=None)
      
      if image_id:
        jpeg = self.app.GetImageLoader().GetImage(int(image_id))
        self.set_header("Content-Type", "image/jpeg")
        self.write(jpeg.data)
      else:
        params = { 'tide_dataset' : self.app.tide_dataset} 
        self.render('tide_index.html', **params)      
      return


class ViewerApp:
  def __init__(self):
    #self.images_uri = 'local://media/vol-0449ca74/itergraph/tide_v08/cropped_scaled_photoid_to_image.pert'
    #self.tide_uri = 'local://media/vol-0449ca74/itergraph/tide_v08/objectid_to_object.pert'
    #self.result_uri = 'local://media/vol-0449ca74/itergraph/tide_v08/usift/cbir/d27df409ad95e12823feed0c658eabeb/itergraph/1b8ba7a00a9d1cd558716ce882e8408f/labelprop_eval1.pert'
    #self.result_uri = 'local://media/vol-0449ca74/itergraph/tide_v08/sift/cbir/2c775823bc8963253c020bae0d89f41f/itergraph/6b762f93548ef0dc9f18a1ddc2924c70/labelprop_eval1.pert'
    
    #self.images_uri = 'local://media/vol-0449ca74/itergraph/tide_v14/cropped_scaled_photoid_to_image_randomaccess.pert'
    #self.result_uri = 'local://media/vol-0449ca74/itergraph/tide_v14/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/6b54a6007b3260b250a2671023dea0f0/labelprop_eval1.pert'
    
#    self.images_uri = 'local://media/vol-0449ca74/itergraph/tide_v14_mixed/photoid_to_image.pert'
#    self.tide_uri = 'local://media/vol-0449ca74/itergraph/tide_v14_mixed/objectid_to_object.pert'
#    self.result_uri = 'local://media/vol-0449ca74/itergraph/tide_v14_mixed/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/e4baab98c10a434d90d092c71ecb566c/labelprop_eval1.pert'
    
#    self.images_uri = 'local://media/vol-0449ca74/itergraph/tide_v14_mixed_v2/photoid_to_image.pert'
#    self.tide_uri = 'local://media/vol-0449ca74/itergraph/tide_v14_mixed_v2/objectid_to_object.pert'
#    self.result_uri = 'local://media/vol-0449ca74/itergraph/tide_v14_mixed_v2/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/e4baab98c10a434d90d092c71ecb566c/labelprop_eval1.pert'


    self.images_uri = 'local://media/vol-0449ca74/itergraph/tide_v16_mixed/photoid_to_image.pert'
    self.tide_uri = 'local://media/vol-0449ca74/itergraph/tide_v16_mixed/objectid_to_object.pert'
    self.result_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v16_mixed/8183e99d6c9424eb5bbe029e16c05453/cbir/494617b7c40f9dce9faa2f15e3de0041/itergraph/e4a1dd52c08ec99117aa7813ab66e38b/labelprop_eval1.pert'

    #self.images_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/tide_v08_distractors/cropped_scaled_photoid_to_image.pert'
    #self.tide_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/tide_v08_distractors/objectid_to_object.pert'
    #self.result_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/tide_v08_distractors/usift/cbir/2c775823bc8963253c020bae0d89f41f/itergraph/81201d63e4a9a1917c8b6e3301474c31/labelprop_eval1.pert'
    
    #self.images_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/tide_v08_distractors/cropped_scaled_photoid_to_image.pert'
    #self.tide_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/tide_v08_distractors/objectid_to_object.pert'
    ##self.result_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/tide_v08_distractors/usift/cbir/2c775823bc8963253c020bae0d89f41f/itergraph/5cefde6b608127b221fa1541c6b009db/labelprop_eval1.pert'
    #self.result_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/tide_v08_distractors/usift/cbir/9e6c60d825c5a9814e19c0a735747011/itergraph/0ffd08c74629993310fd73f6461673af/labelprop_eval1.pert'

#    self.images_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v12/cropped_scaled_photoid_to_image.pert'
#    self.tide_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v12/objectid_to_object.pert'
#    self.result_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v12/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/67b9c7a8221bb1bc7e5f67eb60805af5/labelprop_eval1.pert'
    #self.result_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v12/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/d0210bda2decf5fdb1f6e3d65eff878f/labelprop_eval1.pert'
    #self.result_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v12/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/96d6cee74588e249bcbe2a7c88ca18d5/labelprop_eval1.pert'
    #self.result_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v12/sift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/3fc8eb5fe25f00377d92f6e04146741f/labelprop_eval1.pert'


#    self.images_uri = 'local://home/ubuntu/Desktop/my.cluster.com/data/itergraph/tide_v12/photoid_to_image.pert'
#    self.tide_uri = 'local://home/ubuntu/Desktop/my.cluster.com/data/itergraph/tide_v12/objectid_to_object.pert'
#    self.result_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v12/c76fcf997f9a6083b766e7f30f97f604/cbir/e65781a3f9cdfcd58aadf92f99a2a838/itergraph/830f8900789f6e3e9029c5c7509cd2a0/labelprop_eval1.pert'
    
    
    self.tide_dataset = tide.TideDataset(self.tide_uri)

    #load label prop results file
    self.result = py_eval1.LoadResultOrDie(self.result_uri)
    
    
    # Validate format is correct
    cm = self.result.phases[-1].confusion_matrix
    CHECK_EQ(len(cm.mean), cm.rows*cm.cols)
    
    self.num_labels = len(self.result.label_names)
    self.image_loader = None
    self.application = tornado.web.Application([
      (r"/", MainHandler, dict(app=self)),
      (r"/confusionmatrix/", ConfusionMatrixHandler, dict(app=self)),
      (r"/confusionmatrixcell", ConfusionMatrixCellHandler, dict(app=self)),
      (r"/image", ImageHandler, dict(app=self)),              
      ], debug=True)    
    return
  
  def GetImageLoader(self):
    if self.image_loader == None:
      LOG(INFO, 'loading images...')
      self.image_loader = visutil.BatchImageLoader(self.images_uri)
    return self.image_loader 
  
  def Run(self):
    port = 8888
    while True:
      try:
        self.application.listen(port)
        break
      except:
        print 'trying port: %d' % (port)
        time.sleep(1)
        port +=1

    print 'server running at: http://localhost:%d' % (port)      
    tornado.ioloop.IOLoop.instance().start()
    return

  def RowSum(self, row_index):
    mean = self.result.phases[-1].confusion_matrix.mean    
    sum = 0
    for col_index in range(self.num_labels):
      sum += mean[row_index*self.num_labels + col_index]      
    return sum
  
  def ColSum(self, col_index):
    mean = self.result.phases[-1].confusion_matrix.mean    
    sum = 0
    for row_index in range(self.num_labels):
      sum += mean[row_index*self.num_labels + col_index]      
    return sum
    

if __name__ == "__main__":      
  app = ViewerApp()
  app.Run()
 