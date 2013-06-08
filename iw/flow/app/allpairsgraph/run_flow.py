#!/usr/bin/python
from deluge import scheduler 
from iw.flow.app.allpairsgraph import allpairsgraph 

def main():    
  root_uri = 'maprfs://data/allpairsgraph/pos_tide_v13/'
  fs = scheduler.FlowScheduler(allpairsgraph.MainFlow(root_uri))
  fs.RunSequential()
  return 0

if __name__ == "__main__":        
  main()
    
    
    
    
    
  
