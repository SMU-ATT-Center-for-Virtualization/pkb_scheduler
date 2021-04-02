
class Cloud():
  """[summary]

  [description]
  """

  def __init__(self, cloud_name, instance_quota=None, cpu_quota=None, cpu_usage=None, address_quota=None, bandwidth_limit=None):
    self.instance_quota = instance_quota
    self.instance_usage = 0
    self.cpu_quota = cpu_quota
    self.address_quota = address_quota
    self.address_usage = 0
    self.cpu_usage = cpu_usage
    self.reserved_usage = 0.0
    self.virtual_machines = []
    self.name = cloud_name
    self.regions = {}
    self.bandwidth_limit = bandwidth_limit
    self.bandwidth_usage = 0

    # project_quotas['cpu'] = {'quota':15, 'usage':0} 
    self.project_quotas = {}


  def get_available_cpus(self, region=None):
    if self.cpu_quota:
      return self.cpu_quota - self.cpu_usage
    return None

  def get_available_instances(self, region=None):
    if self.instance_quota:
      return self.instance_quota - self.instance_usage
    return None

  def update_instance_quota(self, quota, region=None):
    self.instance_quota = quota

  def update_instance_usage(self, usage, region=None):
    self.instance_usage = usage

  def has_enough_cpus(self, cpu_count, region=None):
    return self.get_available_cpus() >= cpu_count 

  def cloud_has_enough_resources(self, cpu_count):
    has_enough = True
    if self.cpu_quota:
      has_enough = self.has_enough_cpus(cpu_count)
    for quota_type in self.project_quotas:
      quota = self.project_quotas[quota_type]
      has_enough = (quota['usage'] < quota['quota'])
      if not has_enough:
        return False

    return has_enough

  def has_enough_resources(self, cpu_count, region=None):
    
    has_enough = False
    if region:
      has_enough = self.regions['region'].has_enough_resources(cpu_count)
      return has_enough


    elif (self.get_available_cpus() >= cpu_count 
        and self.address_quota > self.address_usage):
      return True
    else:
      # has_enough =
      print(f"CLOUD.PY LINE 62: THIS LINE WAS NOT COMPLETE SO I HAVE NO IDEA WHAT HAPPENS IF YOU HIT THIS LINE.") 
    
    

  def add_virtual_machine_if_possible(self, vm, region=None):
    
    if (self.get_available_cpus() >= vm.cpu_count 
        and self.address_quota > self.address_usage):
      self.virtual_machines.append(vm)
      self.cpu_usage += vm.cpu_count
      self.address_usage += 1
      return True
    else:
      print("Quota reached for region: " + self.name)
      return False

  def remove_virtual_machine(self, vm, region=None):
    # TODO add safety checks here
    self.virtual_machines.remove(vm)
    self.cpu_usage -= vm.cpu_count
    self.address_usage -= 1

    if self.cpu_usage < 0:
      self.cpu_usage = 0
    if self.address_usage < 0:
      self.address_usage = 0

  def update_cpu_quota(self, quota, region=None):
    self.cpu_quota = quota

  def update_cpu_usage(self, usage, region=None):
    if usage <= self.cpu_quota:
      self.cpu_usage = usage
      return True
    else:
      return False

  def update_address_quota(self, quota, region=None):
    self.address_quota = quota

  def update_address_usage(self, usage, region=None):
    if usage <= self.address_quota:
      self.address_usage = usage
      return True
    else:
      return False

  def add_region_if_not_exists(self, new_region):
    if new_region.name not in self.regions:
      self.regions[new_region.name] = new_region

  def region_exists(self, region_name):
    return region_name in self.regions
