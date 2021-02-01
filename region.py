import cloud_util
from cloud import Cloud

class Region():
  """[summary]

  [description]
  """

  def __init__(self, region_name, cloud, cpu_quota=0, cpu_usage=0, bandwidth_limit=None):
    self.cpu_quota = cpu_quota
    self.address_quota = None
    self.address_usage = 0
    self.cpu_usage = cpu_usage
    self.reserved_usage = 0
    self.virtual_machines = []
    self.name = region_name
    self.cloud = cloud
    self.bandwidth_limit = bandwidth_limit
    self.bandwidth_usage = 0

  def get_available_cpus(self):
    return self.cpu_quota - self.cpu_usage

  def has_enough_cpus(self, cpu_count):
    return self.get_available_cpus() >= cpu_count 

  def has_enough_resources(self, cpu_count, machine_type):
    if (self.get_available_cpus() >= cpu_count 
        and self.address_quota > self.address_usage):
      return True
    else:
      return False

  def add_virtual_machine_if_possible(self, vm):
    if (self.get_available_cpus() >= vm.cpu_count 
        and self.address_quota > self.address_usage):
      self.virtual_machines.append(vm)
      self.cpu_usage += vm.cpu_count
      self.address_usage += 1
      print("CPU USAGE: " + str(self.cpu_usage) + " QUOTA: " + str(self.cpu_quota))
      print("ADDR USAGE: " + str(self.address_usage) + " QUOTA: " + str(self.address_quota))
      return True
    else:
      print("Quota reached for region: " + self.name)
      return False

  def remove_virtual_machine(self, vm):
    # TODO add safety checks here
    self.virtual_machines.remove(vm)
    self.cpu_usage -= vm.cpu_count
    self.address_usage -= 1

    if self.cpu_usage < 0:
      self.cpu_usage = 0
    if self.address_usage < 0:
      self.address_usage = 0

  def update_cpu_quota(self, quota):
    self.cpu_quota = quota

  def update_cpu_usage(self, usage):
    if usage <= self.cpu_quota:
      self.cpu_usage = usage
      return True
    else:
      return False

  def update_address_quota(self, quota):
    self.address_quota = quota

  def update_address_usage(self, usage):
    if usage <= self.address_quota:
      self.address_usage = usage
      return True
    else:
      return False

  def update_quota_usage(self, usages):
    if usage <= self.cpu_quota:
      self.cpu_usage = usage
      return True
    else:
      return False

  def update_quotas(self, quotas):
    pass


class GcpRegion(Region):

  machine_type_bandwidth_dict = {}

  def __init__(self, region_name, cloud, quotas, bandwidth_limit=None):
    # self.my_var = 123
    self.quotas = quotas # this is a dictionary
    Region.__init__(self, region_name, cloud, bandwidth_limit=bandwidth_limit)

  def _get_cpu_type(self, machine_type):
    cpu_type = cloud_util.cpu_type_from_machine_type('GCP', machine_type)
    if cpu_type.upper() == 'N1':
      cpu_type = 'CPUS'
    else:
      cpu_type = cpu_type.upper() + '_CPUS'
    return cpu_type

  def get_available_cpus(self, machine_type):
    cpu_type = self._get_cpu_type(machine_type)
    return self.quotas[cpu_type]['limit'] - self.quotas[cpu_type]['usage']

  def has_enough_cpus(self, cpu_count, machine_type):    
    return self.get_available_cpus(machine_type) >= cpu_count

  def has_enough_resources(self, cpu_count, machine_type):
    estimated_bandwidth = cloud_util.get_max_bandwidth_from_machine_type('GCP', machine_type)
    if (self.get_available_cpus(machine_type) >= cpu_count and self.quotas['IN_USE_ADDRESSES']['limit'] > self.quotas['IN_USE_ADDRESSES']['usage']):
      if not self.bandwidth_limit or estimated_bandwidth + self.bandwidth_usage <= self.bandwidth_limit:
        if not self.cloud.bandwidth_limit or estimated_bandwidth + self.cloud.bandwidth_usage <= self.cloud.bandwidth_limit:
          return True
  
    return False

  def add_virtual_machine_if_possible(self, vm):
    if self.has_enough_resources(vm.cpu_count, vm.machine_type):
      cpu_type = self._get_cpu_type(vm.machine_type)
      estimated_bandwidth = cloud_util.get_max_bandwidth_from_machine_type('GCP', vm.machine_type)
      self.virtual_machines.append(vm)
      self.quotas[cpu_type]['usage'] += vm.cpu_count
      self.quotas['IN_USE_ADDRESSES']['usage'] += 1
      self.bandwidth_usage += estimated_bandwidth
      self.cloud.bandwidth_usage += estimated_bandwidth
      print(f"{cpu_type} CPU USAGE: {self.quotas[cpu_type]['usage']}, QUOTA: {self.quotas[cpu_type]['limit']}")
      print(f"ADDR USAGE: {self.quotas['IN_USE_ADDRESSES']['usage']}, QUOTA: {self.quotas['IN_USE_ADDRESSES']['limit']}")
      return True
    else:
      print("Quota reached for region: " + self.name)
      return False

  def remove_virtual_machine(self, vm):
    # TODO add safety checks here
    cpu_type = self._get_cpu_type(vm.machine_type)
    estimated_bandwidth = cloud_util.get_max_bandwidth_from_machine_type('GCP', vm.machine_type)
    self.virtual_machines.remove(vm)
    self.quotas[cpu_type]['usage'] -= vm.cpu_count
    self.quotas['IN_USE_ADDRESSES']['usage'] -= 1
    self.bandwidth_usage -= estimated_bandwidth
    self.cloud.bandwidth_usage -= estimated_bandwidth

    if self.quotas[cpu_type]['usage'] < 0:
      self.quotas[cpu_type]['usage'] = 0
    if self.quotas['IN_USE_ADDRESSES']['usage'] < 0:
      self.quotas['IN_USE_ADDRESSES']['usage'] = 0

  def update_cpu_quota(self, quota, machine_type):
    cpu_type = self._get_cpu_type(machine_type)
    self.quotas[cpu_type]['limit'] = quota

  def update_cpu_usage(self, usage, machine_type):
    cpu_type = self._get_cpu_type(machine_type)
    if usage <= self.quotas[cpu_type]['limit']:
      self.quotas[cpu_type]['limit'] = quota
      return True
    else:
      return False

  def update_address_quota(self, quota):
    self.quotas['IN_USE_ADDRESSES']['limit'] = quota

  def update_address_usage(self, usage):
    if usage <= self.quotas['IN_USE_ADDRESSES']['limit']:
      self.quotas['IN_USE_ADDRESSES']['usage'] = usage
      return True
    else:
      return False

  # def update_quota_usage(self, usages):
  #   if usage <= self.cpu_quota:
  #     self.cpu_usage = usage
  #     return True
  #   else:
  #     return False

  def update_quotas(self, quotas):
    self.quotas = quotas
