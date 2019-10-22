import graph


class Region():
  """[summary]

  [description]
  """

  def __init__(self, region_name, cpu_quota=0.0, usage=0.0):
    self.cpu_quota = cpu_quota
    self.usage = usage
    self.reserved_usage = 0.0
    self.virtual_machines = []
    self.name = region_name

  def get_available_cpus(self):
    return self.cpu_quota - self.usage

  def has_enough_cpus(self, cpu_count):
    return self.get_available_cpus() >= cpu_count 

  def add_virtual_machine_if_possible(self, vm):
    if self.get_available_cpus() >= vm.cpu_count:
      self.virtual_machines.append(vm)
      self.usage += vm.cpu_count
      print("USAGE: " + str(self.usage) + " QUOTA: " + str(self.cpu_quota))
      return True
    else:
      print("Quota reached for region: " + self.name)
      return False

  def remove_virtual_machine(self, vm):
    # TODO add safety checks here
    self.virtual_machines.remove(vm)
    self.usage -= vm.cpu_count
