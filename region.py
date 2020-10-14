
class Region():
  """[summary]

  [description]
  """

  def __init__(self, region_name, cloud, cpu_quota=0.0, cpu_usage=0.0):
    self.cpu_quota = cpu_quota
    self.address_quota = None
    self.address_usage = 0
    self.cpu_usage = cpu_usage
    self.reserved_usage = 0.0
    self.virtual_machines = []
    self.name = region_name
    self.cloud = cloud

  def get_available_cpus(self):
    return self.cpu_quota - self.cpu_usage

  def has_enough_cpus(self, cpu_count):
    return self.get_available_cpus() >= cpu_count 

  def has_enough_resources(self, cpu_count):
    if (self.get_available_cpus() >= cpu_count 
        and self.address_quota > self.address_usage):
      return True
    else:
      return False

  def add_virtual_machine_if_possible(self, vm):
    print(f"in add_virtual_machine_if_possible: \n\n {vm.__dict__}")
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
