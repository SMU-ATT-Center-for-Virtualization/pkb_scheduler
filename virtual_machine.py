import graph

class VirtualMachine(graph.Node):
  """[summary]
  
  [description]
  """

  def __init__(self, node_id, cpu_count, os_type=None, machine_type=None, cloud=None, 
               network_tier=None, vpn=False, vpn_gateway_count=0, vpn_tunnel_count=0):
    self.cpu_count = cpu_count
    self.os_type = os_type
    self.machine_type = machine_type
    self.cloud = cloud
    self.network_tier = network_tier
    self.vpn = vpn
    
    graph.Node.__init__(self, node_id)
    
  


