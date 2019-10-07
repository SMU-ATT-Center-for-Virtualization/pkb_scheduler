import graph

class VirtualMachine(graph.Node):
  """[summary]
  
  [description]
  """

  def __init__(self, node_id, cpu_count, zone, os_type=None, machine_type=None, cloud=None, 
               network_tier=None, vpn=False, vpn_gateway_count=0, vpn_tunnel_count=0):        
    self.cpu_count = cpu_count
    self.os_type = os_type
    self.zone = zone
    self.machine_type = machine_type
    self.cloud = cloud
    self.network_tier = network_tier
    self.vpn = vpn
    self.ssh_key = None
    self.status = 'Not Created'
    # self.ssh_key = None
    # self.ip = None
    
    graph.Node.__init__(self, node_id)
    
  def add_adjacent_node(self, adjacent_node):
    self.adjacent_node_list.append(adjacent_node)
    
  def add_edge(self, edge):
    self.edge_list.append(edge)
  
  def create_edge(self, adjacent_node, benchmark):
    pass
  
  def add_benchmark(self, benchmark):
    pass
