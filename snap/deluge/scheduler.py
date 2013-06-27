#!/bin/python

import glob
import os
import networkx as nx
import matplotlib.pyplot as plt
from deluge import notification
from multiprocessing import Pool
from pyglog import *
import pickle
#import inspect
import subprocess

try:
    from networkx import graphviz_layout
except ImportError:
    raise ImportError("This example needs Graphviz and either PyGraphviz or Pydot")


def ExecuteCmd(cmd, quiet=False):
  result = None
  if quiet:    
    with open(os.devnull, "w") as fnull:    
      result =  subprocess.call(cmd, shell=True, stdout = fnull, stderr = fnull)
  else:
    result = subprocess.call(cmd, shell=True)
  return result


#def RunFlowExternal(flow):
#  pid = os.getpid()
#  
#  config_filename = '%s.conf' % (pid)
#  config_file = open(config_filename, 'w')
#  try:
#    pickle.dump(flow, config_file)
#  except:
#    LOG(FATAL, 'error with pickle')
#    
#  config_file.close()
#  
#  title = 'FLOW STARTED - %s' % flow
#  notification.SendNotification(title, 'flow started')
#  print title
#
#  extrun_path = os.path.dirname(__file__) + '/runner.py'
#  cmd = 'python %s %s' % (extrun_path, config_filename)
#  retval = ExecuteCmd(cmd)
#  success = (retval == 0)
#  
#  if success:
#    title = 'FLOW DONE - %s' % flow
#    notification.SendNotification(title, 'flow done')
#  else:
#    title = 'FAIL - %s' % flow
#    notification.SendNotification(title, 'flow failed')
#  
#  os.remove(config_filename)
#  return success

#def RunFlowExternal(cmd):
#  #pid = os.getpid()  
#  title = 'FLOW STARTED - %s' % cmd 
#  notification.SendNotification(title, 'flow started')
#  print title
#  
#  retval = ExecuteCmd(cmd)
#  success = (retval == 0)
#  
#  if success:
#    title = 'FLOW DONE - %s' % cmd
#    notification.SendNotification(title, 'flow done')
#  else:
#    title = 'FAIL - %s' % cmd
#    notification.SendNotification(title, 'flow failed')
#    
#  return success
#  
  
class FlowScheduler(object):
  def __init__(self, root_flow):
    self.root_flow = root_flow
    self.graph = nx.DiGraph() 
    
    self.cur_step = 0    
    self.node_pos = None
    self.visited_nodes = set()
    
    self.__BuildDependencyGraph(root_flow)
    return
    
  def __BuildDependencyGraph(self, parent_flow, level=0):
    self.graph.add_node(parent_flow, depth = level)
    self.visited_nodes.add(parent_flow)
    for child_flow in parent_flow.dependent_flows:
      self.graph.add_edge(parent_flow, child_flow, weight=1)
      CHECK_NE(child_flow, parent_flow, 'Circular dependency found: %s' % (parent_flow))
      if child_flow in self.visited_nodes:
        continue        
      self.__BuildDependencyGraph(child_flow, level+1)       
      
    return
    
  def __UpdateStatus(self):
    #LOG(INFO, '__UpdateStatus')
    self.graph.node_done = {}
    self.graph.node_status = {}
    self.graph.blocked_node_missing_input = {}
    for flow in self.graph.nodes():
      if flow.IsDone():
        self.graph.node_done[flow] = True
      else:
        self.graph.node_done[flow] = False
        
    for flow in self.graph.nodes():
      if self.graph.node_done[flow]:
        self.graph.node_status[flow] = 'done'       
      else:
        input_nodes = self.graph.predecessors(flow)
        all_inputs_done = True
        missing_input = None
        for input_node in input_nodes:
          if not self.graph.node_done[input_node]:
            all_inputs_done = False
            missing_input = input_node
            break        
        if all_inputs_done:          
          self.graph.node_status[flow] = 'runnable'
        else:
          self.graph.node_status[flow] = 'blocked'
          self.graph.blocked_node_missing_input[flow] = missing_input
          
    return
         
  def RemoveOldFlowDiagrams(self):
    old_files = glob.glob('FlowDiagram*.png')    
    for filename in old_files:      
      os.remove(filename)    
    return
      
    
  def SaveFlowDiagram(self):
    #LOG(INFO, 'SaveFlowDiagram')
    self.__UpdateStatus()
    color_map = {'done': 'gray', 'runnable': 'green', 'blocked' : 'red'}
    node_color = [color_map[self.graph.node_status[v]] for v in self.graph]
        
    if not self.node_pos:
      # spring layout
      #self.node_pos = nx.spring_layout(self.graph, scale=1000.0, iterations=1000)

      # shell layout
#      max_depth = 0 
#      for (node, node_data) in self.graph.nodes(data=True):
#        depth = node_data['depth']
#        max_depth = max(depth, max_depth)
#        
#      shells = [[] for i in range(max_depth+1)]
#      for (node, node_data) in self.graph.nodes(data=True):
#        depth = node_data['depth']  
#        shells[depth].append(node)   
#      
#      self.node_pos = nx.shell_layout(self.graph, shells)

      # graphviz circular tree layout      
      #self.node_pos=nx.graphviz_layout(self.graph,prog='twopi',args='')
      
      # graphviz force layout
      self.node_pos=nx.graphviz_layout(self.graph,prog='neato',args='')
      
    nx.draw(self.graph, pos=self.node_pos, node_color=node_color, node_size=50, font_size = 10, font_color = 'black', edge_color = 'gray')
    filename = "FlowDiagram%05d.png" % (self.cur_step)    
    plt.savefig(filename) # save as png
    plt.clf()
    return filename
    
    
  def GetRunnableNodes(self):
    #LOG(INFO, 'GetRunnableNodes')
    self.__UpdateStatus()
    
    runnable_nodes = []
    for node in self.graph.nodes():
      if self.graph.node_status[node] == 'runnable':
        runnable_nodes.append(node)

    # the key is the depth and then the flow python object id... this makes the sort deterministic for nodes at the same depth
    runnable_nodes.sort(key=lambda n: '%05d-%064d' % (self.graph.node[n]['depth'], id(n)))
    return runnable_nodes
  
  
  def GetBlockedNodes(self):
    self.__UpdateStatus()
    nodes = []
    for node in self.graph.nodes():
      if self.graph.node_status[node] == 'blocked':
        nodes.append(node)

    # the key is the depth and then the flow python object id... this makes the sort deterministic for nodes at the same depth
    nodes.sort(key=lambda n: '%05d-%064d' % (self.graph.node[n]['depth'], id(n)))
    return nodes
    
    
  def GetDeepestRunnable(self):
    runnable_nodes = self.GetRunnableNodes()
    selected_node = None
    if runnable_nodes:
      selected_node = runnable_nodes[0]
    return selected_node
  
      
  def __GetFringeNodes(self):
    #LOG(INFO, '__GetFringeNodes')
    fringe = []
    for node in self.graph.nodes():      
      if self.graph.out_degree(node) == 0:
        fringe.append(node)
    return fringe
    
    
  def NodesAreDone(self, node_set):
    self.__UpdateStatus()
    target_set_done = True
    for node in node_set:
      if not self.graph.node_done[node]:
        target_set_done = False
        break      
    return target_set_done
        
    
  def RunSequential(self):
    print 'RunSequential'
    self.cur_step = 0
    fringe_nodes = self.__GetFringeNodes()
    self.RemoveOldFlowDiagrams()
    while True:
      flow_diagram_filename = self.SaveFlowDiagram()
      if self.NodesAreDone(fringe_nodes):
        print 'all nodes are done!'        
        break
      try:
        runable = self.GetDeepestRunnable()        
        if not runable:
          blocked_nodes = self.GetBlockedNodes()
          for node in blocked_nodes:
            print 'node: %s' % (node)
            print 'missing input: %s ' % (self.graph.blocked_node_missing_input[node])          
          LOG(INFO, 'Not done, but no runnable nodes are available...')
          return False
        title = 'FLOW STARTED - %s' % runable
        #notification.SendNotification(title, 'flow started')
        print title
        runable.Execute()
      except Exception, e:
        error_msg = str(e)
        trace = traceback.format_exc()
        error_msg += 'Trace:\n'
        error_msg += trace
        #notification.SendNotification('FAIL - %s' % runable , error_msg)
        print ''
        print error_msg
        return False      
      title = 'FLOW DONE - %s' % runable
      #notification.SendNotification(title, 'flow done')
      self.cur_step += 1
    #notification.SendNotification('ALL DONE' , 'all flows completed')    
    return True
  
  
  def RunParallel(self, num_processes = 4):
    self.RemoveOldFlowDiagrams()
    fringe_nodes = self.__GetFringeNodes()
    pool = Pool(num_processes)  # start worker processes    
    
    prev_started_flows = set()
    cur_running_flows = []
    pending_results = set()
    result_to_flow = {}
    while True:
      flow_diagram_filename = self.SaveFlowDiagram()
      
      # check for termination condition
      if self.NodesAreDone(fringe_nodes):
        print 'all fringe nodes are done!'        
        break

      runable_flows = self.GetRunnableNodes()      
      if not runable_flows:
        LOG(FATAL, 'Not done, but no runnable nodes are available...')
        break
      
      # see if we can start any new jobs
      resources_available = True
      for flow in cur_running_flows:
        if flow.reserve_all_resources:
          resources_available = False
          CHECK(False)
          break
      
      if resources_available:
        for flow in runable_flows:        
          if flow in prev_started_flows:
            continue
          
          hadoop_cmd = flow.GetHadoopRunCommand()
          
          async_result = pool.apply_async(RunFlowExternal, [hadoop_cmd])
          pending_results.add(async_result)
          prev_started_flows.add(flow)          
          result_to_flow[async_result] = flow 
        
      # update status of running jobs
      succesful_results = set()
      failed_results = set()
      for async_result in pending_results:
        if async_result.ready():
          ok = False
          if async_result.successful():
            if async_result.get():
              ok = True
          else:
            async_result.get()
            
          if ok:
            succesful_results.add(async_result)
            print 'flow finished: %s' % async_result
            
          else:
            failed_results.add(async_result)
            print 'flow failed: ' % async_result
        
      pending_results -=  succesful_results
      pending_results -=  failed_results
      
      cur_running_flows = [result_to_flow[result] for result in pending_results]
      
      print 'pending: %d' % (len(pending_results))
      print cur_running_flows
      time.sleep(5)

    #notification.SendNotification('ALL DONE' , 'all flows completed')
    
    return
    
       