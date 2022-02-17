import copy

class Benchmark():
  """[summary]

  [description]
  """

  def __init__(self, benchmark_id, benchmark_type, vm_specs=[], 
               zone1=None, zone2=None, more_zones=[],
               os_type='ubuntu1804', machine_type=None, cloud='GCP',
               network_tier='premium', vpn=False, vpn_gateway_count=0,
               vpn_tunnel_count=0, bigquery_table=None, bq_project=None,
               estimated_bandwidth=-1,
               flags={}):

    self.benchmark_id = benchmark_id
    self.benchmark_type = benchmark_type
    self.status = "Not Executed"
    self.vms = []
    self.config_file = None
    self.flags = flags
    self.bigquery_table=bigquery_table
    self.bq_project=bq_project
    self.estimated_bandwidth=estimated_bandwidth

    if len(vm_specs) <= 0:  
      self.zone1 = zone1
      self.zone2 = zone2
      self.machine_type = machine_type
      self.cloud = cloud
      self.os_type = os_type
      self.network_tier = network_tier
      self.vpn = vpn
      self.vm_specs = []
    else:
      self.vm_specs = vm_specs
      largest_cpu_count = 0
      for vm_spec in self.vm_specs:
        if vm_spec.cpu_count > largest_cpu_count:
          largest_cpu_count = vm_spec.cpu_count
      self.largest_vm = largest_cpu_count
      

    # self.ssh_key = None
    # self.ip = None

  def copy_contents(self, bm):
    self.benchmark_id = bm.benchmark_id
    self.benchmark_type = bm.benchmark_type
    self.zone1 = bm.zone1
    self.zone2 = bm.zone2
    self.machine_type = bm.machine_type
    self.cloud = bm.cloud
    self.os_type = bm.os_type
    self.network_tier = bm.network_tier
    self.vpn = bm.vpn
    # self.vms = []
    self.status = bm.status
    self.config_file = bm.config_file
    self.flags = bm.flags
    self.flags = copy.deepcopy(bm.flags)
    self.vm_specs = []
    for vm_spec in bm.vm_specs:
      tmp_vm_spec = vm_spec.copy_contents()
      self.vm_specs.append(tmp_vm_spec)
    # self.ssh_key = None
    # self.ip = None
    self.largest_vm = bm.largest_vm

  def run(self):
    pass

  def __repr__(self):
    # zones = []
    # machine_sizes = []
    # for vm_spec in self.vm_specs:
    #   zones.append(vm_spec.zone)
    #   machine_sizes.append(vm_spec.machine_type)
    # return f'BM {{id: {self.benchmark_id}, zones: {zones}, machine_sizes: {machine_sizes}}}'
    return f'BM {id}'

  def __str__(self):
    # zones = []
    # machine_sizes = []
    # for vm_spec in self.vm_specs:
    #   zones.append(vm_spec.zone)
    #   machine_sizes.append(vm_spec.machine_type)
    # return f'BM {{id: {self.benchmark_id}, zones: {zones}, machine_sizes: {machine_sizes}}}'
    return f'BM {id}'
