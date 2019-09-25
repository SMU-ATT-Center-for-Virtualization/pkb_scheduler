class Node:
  """[summary]
  
  [description]
  """

  def __init__(self, node_id):
    self.node_id = node_id
    self.adjacent_nodes = []
    
  def add_adjacent_node(self, adjacent_node):
    self.adjacent_nodes.append(adjacent_node)


class NodeGroup:
  """[summary]
  
  [description]
  """

  def __init__(self, node_list=[]):
    self.node_list = node_list
    
  def add_node(self, node):
    self.node_list.append(node)
    
  def remove_node(self, node):
    self.node_list.remove(node)
    
  def get_node(self, node):
    self.node_list[node]


class edge:
  """[summary]
  
  [description]
  """

  def __init__(self, node1, node2, metadata=None):
    self.node1 = node1
    self.node2 = node2
    self.metadata = metadata
    
   
class Graph:
  """[summary]
  
  [description]
  """
  
  def __init__(self):
    self.nodes = []
    self.node_groups = []
    self.edges = []
    
    
    