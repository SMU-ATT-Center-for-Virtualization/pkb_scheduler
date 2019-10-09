class VirtualMachine():
  """[summary]
  
  [description]
  """

  def __init__(self, node_id, cpu_count, zone, os_type=None, machine_type=None, cloud=None, 
               network_tier=None, vpn=False, vpn_gateway_count=0, vpn_tunnel_count=0):        
    self.node_id = node_id
    self.cpu_count = cpu_count
    self.os_type = os_type
    self.zone = zone
    self.machine_type = machine_type
    self.cloud = cloud
    self.network_tier = network_tier
    self.vpn = vpn
    self.ssh_key = None
    self.status = 'Not Created'
    # self.ip_address = None