class Benchmark():
  """[summary]
  
  [description]
  """

  def __init__(self, benchmark_id, benchmark_type, zone1, zone2, more_zones=[],
               os_type='ubuntu1804', machine_type=None, cloud='GCP',
               network_tier='premium', vpn=False, vpn_gateway_count=0,
               vpn_tunnel_count=0, flags=None):        

    self.zone1 = zone1
    self.zone2 = zone2
    self.machine_type = machine_type
    self.cloud = cloud
    self.os_type = os_type
    self.network_tier = network_tier
    self.vpn = vpn
    self.vms = []
    # self.ssh_key = None
    # self.ip = None
    
  def add_adjacent_node(self, adjacent_node):
    self.adjacent_node_list.append(adjacent_node)
    
  def add_edge(self, edge):
    self.edge_list.append(edge)
  
  def create_edge(self, adjacent_node, benchmark):
    pass
  
  def add_benchmark(self, benchmark):
    pass
  


