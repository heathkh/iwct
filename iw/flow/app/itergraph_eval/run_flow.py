#!/usr/bin/python
from deluge import scheduler 
from iw.flow.app.itergraph_eval import itergraph_eval
from snap.pyglog import *

def main():
  #tide 12      
  #root_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v12/' 
  #matches_root_uri = '%s/c76fcf997f9a6083b766e7f30f97f604/cbir/e65781a3f9cdfcd58aadf92f99a2a838/itergraph/830f8900789f6e3e9029c5c7509cd2a0' % (root_uri)
  #matches_root_uri = '%s/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/96d6cee74588e249bcbe2a7c88ca18d5/' % (root_uri)
  #matches_root_uri = '%s/sift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/3fc8eb5fe25f00377d92f6e04146741f/' % (root_uri)
  
  
  
  
  #tide 14
  #root_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v14/' 
  #matches_root_uri = '%s/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/6b54a6007b3260b250a2671023dea0f0/' % (root_uri)  
  
  #tide 14 mixed
  #root_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v14_mixed/' 
  #matches_root_uri = '%s/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/e4baab98c10a434d90d092c71ecb566c/' % (root_uri)  
  
  #tide 14 mixed_v2
  #root_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v14_mixed_v2/' 
  #matches_root_uri = '%s/usift/cbir/654c8f59fd938958c1c739fd65949dad/itergraph/e4baab98c10a434d90d092c71ecb566c/' % (root_uri)
  
  #tide 16 mixed
  #root_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v16_mixed/' 
  #matches_root_uri = '%s/8183e99d6c9424eb5bbe029e16c05453/cbir/494617b7c40f9dce9faa2f15e3de0041/itergraph/e4a1dd52c08ec99117aa7813ab66e38b' % (root_uri)
  
  # tide 08 distractors
  #root_uri = 'local://home/ubuntu/Desktop/vol-7f209e0c/itergraph/tide_v08_distractors/'   
  #matches_root_uri = '%s/usift/cbir/9e6c60d825c5a9814e19c0a735747011/itergraph/0ffd08c74629993310fd73f6461673af/' % (root_uri)
  
  #tide 8
  #root_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v08/' 
  #matches_root_uri = '%s/usift/cbir/d27df409ad95e12823feed0c658eabeb/itergraph/1b8ba7a00a9d1cd558716ce882e8408f/' % (root_uri)
  ##matches_root_uri = '%s/sift/cbir/2c775823bc8963253c020bae0d89f41f/itergraph/6b762f93548ef0dc9f18a1ddc2924c70/' % (root_uri)
  
  #root_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v17/' 
  #matches_root_uri = '%s/7192be9ebd89da866039ed7cbb3d6011/cbir/494617b7c40f9dce9faa2f15e3de0041/itergraph/a6a2155b8d4f91f6114cebb36efd1d5a' % (root_uri)
  
  #tide_18_mixed
  root_uri = 'local://home/ubuntu/Desktop/vol-0449ca74/itergraph/tide_v18_mixed/' 
  matches_root_uri = '%s/e1059d38bd73546eea4e2f6ca9ec0966/cbir/494617b7c40f9dce9faa2f15e3de0041/itergraph/097fed13ba6240199b9e7e1862da58f1' % (root_uri)
  
  
  fs = scheduler.FlowScheduler(itergraph_eval.MainFlow(root_uri, matches_root_uri))
  fs.RunSequential()
  return 0

if __name__ == "__main__":        
  main()
    
    
    
    