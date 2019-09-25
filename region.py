import graph

class Region(graph.NodeGroup):
  """[summary]
  
  [description]
  """

  def __init__(self, cpu_quota=None):
    self.cpu_quota = cpu_quota
    self.virtual_machines = []
    
    graph.NodeGroup.__init__(self)
    
  


