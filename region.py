import cloud_util
from cloud import Cloud
import re
import copy

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

  def vm_has_enough_resources(self, vm):
    return self.has_enough_resources(vm.cpu_count, vm.machine_type)

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

  def get_all_quotas(self):
    quotas = {}
    quotas[self.name] = {}
    quotas[self.name]['address_usage'] = self.address_usage
    quotas[self.name]['address_quota'] = self.address_quota
    return quotas


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

  def has_enough_resources(self, cpu_count, machine_type, estimated_bandwidth):
    if estimated_bandwidth < 0:
      estimated_bandwidth = 0
    # estimated_bandwidth = cloud_util.get_max_bandwidth_from_machine_type('GCP', machine_type)
    if (self.get_available_cpus(machine_type) >= cpu_count and self.quotas['IN_USE_ADDRESSES']['limit'] > self.quotas['IN_USE_ADDRESSES']['usage']):
      if not self.bandwidth_limit or estimated_bandwidth + self.bandwidth_usage <= self.bandwidth_limit:
        if not self.cloud.bandwidth_limit or estimated_bandwidth + self.cloud.bandwidth_usage <= self.cloud.bandwidth_limit:
          return True
  
    return False

  def vm_has_enough_resources(self, vm):
    return self.has_enough_resources(vm.cpu_count, vm.machine_type, vm.estimated_bandwidth)

  def add_virtual_machine_if_possible(self, vm):
    if self.has_enough_resources(vm.cpu_count, vm.machine_type, vm.estimated_bandwidth):
      cpu_type = self._get_cpu_type(vm.machine_type)
      estimated_bandwidth = vm.estimated_bandwidth
      if estimated_bandwidth < 0:
        estimated_bandwidth = 0
      self.virtual_machines.append(vm)
      self.quotas[cpu_type]['usage'] += vm.cpu_count
      self.quotas['IN_USE_ADDRESSES']['usage'] += 1
      self.bandwidth_usage += estimated_bandwidth
      self.cloud.bandwidth_usage += estimated_bandwidth
      # print(f"ADD {self.name} {cpu_type} CPU USAGE: {self.quotas[cpu_type]['usage']}, QUOTA: {self.quotas[cpu_type]['limit']}")
      # print(f"ADD {self.name} ADDR USAGE: {self.quotas['IN_USE_ADDRESSES']['usage']}, QUOTA: {self.quotas['IN_USE_ADDRESSES']['limit']}")
      return True
    else:
      #print("Quota reached for region: " + self.name)
      return False

  def remove_virtual_machine(self, vm):
    # TODO add safety checks here
    cpu_type = self._get_cpu_type(vm.machine_type)
    # estimated_bandwidth = cloud_util.get_max_bandwidth_from_machine_type('GCP', vm.machine_type)
    estimated_bandwidth = vm.estimated_bandwidth
    if estimated_bandwidth < 0:
      estimated_bandwidth = 0
    self.virtual_machines.remove(vm)
    self.quotas[cpu_type]['usage'] -= vm.cpu_count
    self.quotas['IN_USE_ADDRESSES']['usage'] -= 1
    self.bandwidth_usage -= estimated_bandwidth
    self.cloud.bandwidth_usage -= estimated_bandwidth

    if self.quotas[cpu_type]['usage'] < 0:
      self.quotas[cpu_type]['usage'] = 0
    if self.quotas['IN_USE_ADDRESSES']['usage'] < 0:
      self.quotas['IN_USE_ADDRESSES']['usage'] = 0

    # print(f"REMOVE {self.name} {cpu_type} CPU USAGE: {self.quotas[cpu_type]['usage']}, QUOTA: {self.quotas[cpu_type]['limit']}")
    # print(f"REMOVE {self.name} ADDR USAGE: {self.quotas['IN_USE_ADDRESSES']['usage']}, QUOTA: {self.quotas['IN_USE_ADDRESSES']['limit']}")

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
    print("UPDATE ALL QUOTAS")
    self.quotas = quotas

  def get_all_quotas(self):
    quotas = {}
    quotas[self.name] = copy.deepcopy(self.quotas)
    return quotas


class AwsRegion(Region):

  machine_type_bandwidth_dict = {}

  def __init__(self, region_name, cloud, quotas, bandwidth_limit=None):
    # self.my_var = 123
    self.quotas = quotas # this is a dictionary
    Region.__init__(self, region_name, cloud, bandwidth_limit=bandwidth_limit)

  # def _get_cpu_type(self, machine_type):
  #   cpu_type = cloud_util.cpu_type_from_machine_type('AWS', machine_type)
  #   if cpu_type.upper() == 'N1':
  #     cpu_type = 'CPUS'
  #   else:
  #     cpu_type = cpu_type.upper() + '_CPUS'
  #   return cpu_type

  # def get_available_cpus(self, machine_type):
  #   cpu_type = self._get_cpu_type(machine_type)
  #   return self.quotas[cpu]['limit'] - self.quotas[cpu_type]['usage']

  # def has_enough_cpus(self, cpu_count, machine_type):    
  #   return self.get_available_cpus(machine_type) >= cpu_count

  def has_enough_resources(self, cpu_count, machine_type):
    estimated_bandwidth = cloud_util.get_max_bandwidth_from_machine_type('AWS', machine_type)
    if (self.quotas['vm']['usage'] < self.quotas['vm']['limit']
      and self.quotas['elastic_ip']['usage'] < self.quotas['elastic_ip']['limit']
      and self.quotas['vpc']['usage'] < self.quotas['vpc']['limit']):
      # If we are checking bandwidth limits, and if this exceeds limit
      if not self.bandwidth_limit or estimated_bandwidth + self.bandwidth_usage <= self.bandwidth_limit:
        if not self.cloud.bandwidth_limit or estimated_bandwidth + self.cloud.bandwidth_usage <= self.cloud.bandwidth_limit:
          return True
  
    return False

  def vm_has_enough_resources(self, vm):
    return self.has_enough_resources(vm.cpu_count, vm.machine_type)

  def add_virtual_machine_if_possible(self, vm):
    if self.has_enough_resources(vm.cpu_count, vm.machine_type):
      # cpu_type = self._get_cpu_type(vm.machine_type)
      estimated_bandwidth = cloud_util.get_max_bandwidth_from_machine_type('AWS', vm.machine_type)
      self.virtual_machines.append(vm)
      self.quotas['vm']['usage'] += 1
      self.quotas['vpc']['usage'] += 1
      self.quotas['elastic_ip']['usage'] += 1
      self.bandwidth_usage += estimated_bandwidth
      self.cloud.bandwidth_usage += estimated_bandwidth
      print(f"AWS VM USAGE: {self.quotas['vm']['usage']}, QUOTA: {self.quotas['vm']['limit']}")
      print(f"AWS ELASTIC IP USAGE: {self.quotas['elastic_ip']['usage']}, QUOTA: {self.quotas['elastic_ip']['limit']}")
      return True
    else:
      print("Quota reached for region: " + self.name)
      return False

  def remove_virtual_machine(self, vm):
    # TODO add safety checks here
    # cpu_type = self._get_cpu_type(vm.machine_type)
    estimated_bandwidth = cloud_util.get_max_bandwidth_from_machine_type('AWS', vm.machine_type)
    self.virtual_machines.remove(vm)
    self.quotas['vm']['usage'] -= 1
    self.quotas['vpc']['usage'] -= 1
    self.quotas['elastic_ip']['usage'] -= 1
    self.bandwidth_usage -= estimated_bandwidth
    self.cloud.bandwidth_usage -= estimated_bandwidth

    if self.quotas['vm']['usage'] < 0:
      self.quotas['vm']['usage'] = 0 
    if self.quotas['vpc']['usage'] < 0:
      self.quotas['vm']['usage'] = 0
    if self.quotas['elastic_ip']['usage'] < 0:
      self.quotas['vm']['usage'] = 0

  # def update_cpu_quota(self, quota, machine_type):
  #   cpu_type = self._get_cpu_type(machine_type)
  #   self.quotas[cpu_type]['limit'] = quota

  # def update_cpu_usage(self, usage, machine_type):
  #   cpu_type = self._get_cpu_type(machine_type)
  #   if usage <= self.quotas[cpu_type]['limit']:
  #     self.quotas[cpu_type]['limit'] = quota
  #     return True
  #   else:
  #     return False

  def update_address_quota(self, quota):
    self.quotas['elastic_ip']['limit'] = quota

  def update_address_usage(self, usage):
    if usage <= self.quotas['elastic_ip']['limit']:
      self.quotas['elastic_ip']['usage'] = usage
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


# Troy, I've split region into subclasses for each cloud
# so put all the azure stuff in this class
# the only absolutely necessary functions are has_enough_resources, add_virtual_machine_if_possible and remove_virtual_machine
# What is currently in this class is just a duplicate of AwsRegion.
class AzureRegion(Region):

  machine_type_bandwidth_dict = {}

  def __init__(self, region_name, cloud, quotas, bandwidth_limit=None):
    # self.my_var = 123
    self.quotas = quotas # this is a dictionary
    Region.__init__(self, region_name, cloud, bandwidth_limit=bandwidth_limit)

  def has_enough_resources(self, cpu_count, machine_type):
    estimated_bandwidth = cloud_util.get_max_bandwidth_from_machine_type('AZURE', machine_type)
    # Troy, change this depending on the relevant quotas. Leave the bandwidth stuff alone
    print(f"self.quotas: {self.quotas}")
    passingQuotas = 1
    # for quotaCheck in quotas:
    #   print(f"quotaCheck: {quotaCheck}")
    if (self.quotas['TOTAL REGIONAL VCPUS'][0] < self.quotas['TOTAL REGIONAL VCPUS'][1]
      and self.quotas['VIRTUAL MACHINES'][0] < self.quotas['VIRTUAL MACHINES'][1]
      and self.quotas['PUBLIC IP ADDRESSES - BASIC'][0] < self.quotas['PUBLIC IP ADDRESSES - BASIC'][1]):
      # If we are checking bandwidth limits, and if this exceeds limit
      if not self.bandwidth_limit or estimated_bandwidth + self.bandwidth_usage <= self.bandwidth_limit:
        if not self.cloud.bandwidth_limit or estimated_bandwidth + self.cloud.bandwidth_usage <= self.cloud.bandwidth_limit:
          return True
  
    return False

  def vm_has_enough_resources(self, vm):
    return self.has_enough_resources(vm.cpu_count, vm.machine_type)

  def add_virtual_machine_if_possible(self, vm):
    if self.has_enough_resources(vm.cpu_count, vm.machine_type):
      estimated_bandwidth = cloud_util.get_max_bandwidth_from_machine_type('AWS', vm.machine_type)
      self.virtual_machines.append(vm)  
      print(f"\n\nvm is: {vm.__dict__}")
      if vm.cloud.upper() == "AZURE":
        # verified_machine_type = re.findall("[123456789-]+", vm.machine_type)
        # verified_machine_type = vm.machine_type.replace(verified_machine_type[0], "")
        verified_machine_type = vm.machine_type
        previous = ""
        counter = 0 
        for x in vm.machine_type:
          if x.isdigit() and previous != 'v':
            print(f"Before type is: {verified_machine_type}")
            verified_machine_type = verified_machine_type[0:counter] + verified_machine_type[counter+1:]
            print(f"After type is: {verified_machine_type}")

            print(f"In if!")
          counter += 1
          previous = x
        verified_machine_type = verified_machine_type.replace("Standard_", "")
        verified_machine_type = verified_machine_type.replace("_", "")
        verified_machine_type = verified_machine_type.upper()
        full_machine_string = "STANDARD " + verified_machine_type + " FAMILY VCPUS"
        print(f"\n\nThe full machine string is: {full_machine_string}\n\n")
        print(f"self.quotas[full_machine_string][0] is : {self.quotas[full_machine_string][0]}")
        if self.quotas[full_machine_string][0] == self.quotas[full_machine_string][1]:
          return False
        self.quotas[full_machine_string][0] += 1
        self.quotas['TOTAL REGIONAL VCPUS'][0] += 1
        self.quotas['VIRTUAL MACHINES'][0] += 1
        self.quotas['PUBLIC IP ADDRESSES - BASIC'][0] += 1
      else:
        self.quotas['vm']['usage'] += 1
        self.quotas['vpc']['usage'] += 1
        self.quotas['elastic_ip']['usage'] += 1
      
      self.bandwidth_usage += estimated_bandwidth
      self.cloud.bandwidth_usage += estimated_bandwidth
      #print(f"AWS VM USAGE: {self.quotas['vm']['usage']}, QUOTA: {self.quotas['vm']['limit']}")
      #print(f"AWS ELASTIC IP USAGE: {self.quotas['elastic_ip']['usage']}, QUOTA: {self.quotas['elastic_ip']['limit']}")
      return True
    else:
      print("Quota reached for region: " + self.name)
      return False

  def remove_virtual_machine(self, vm):
    # TODO add safety checks here
    print(f"\n\nSelf in remove v machine is: {self.virtual_machines[0].__dict__} \n\n")
    print(f"vm is: {vm.__dict__}")
    if self.cloud.name.upper() == "AZURE":
      verified_machine_type = vm.machine_type

      previous = ""
      counter = 0 
      for x in vm.machine_type:
        if x.isdigit() and previous != 'v':
          #print(f"Before type is: {verified_machine_type}")
          verified_machine_type = verified_machine_type[0:counter] + verified_machine_type[counter+1:]
          #print(f"After type is: {verified_machine_type}")

          print(f"In if!")
        counter += 1
        previous = x
      verified_machine_type = verified_machine_type.replace("Standard_", "")
      verified_machine_type = verified_machine_type.replace("_", "")
      verified_machine_type = verified_machine_type.upper()
      full_machine_string = "STANDARD " + verified_machine_type + " FAMILY VCPUS"
      self.quotas[full_machine_string][0] -= 1
      self.quotas['TOTAL REGIONAL VCPUS'][0] -= 1
      self.quotas['VIRTUAL MACHINES'][0] -= 1
      self.quotas['PUBLIC IP ADDRESSES - BASIC'][0] -= 1
      if self.quotas[full_machine_string][0] < 0:
        self.quotas[full_machine_string][0] = 0
      if self.quotas['TOTAL REGIONAL VCPUS'][0] < 0:
        self.quotas['TOTAL REGIONAL VCPUS'][0] = 0
      if self.quotas['PUBLIC IP ADDRESSES - BASIC'][0] < 0:
        self.quotas['PUBLIC IP ADDRESSES - BASIC'][0] = 0
    else:
      cpu_type = self._get_cpu_type(vm.machine_type)
      self.quotas['vm']['usage'] -= 1
      self.quotas['vpc']['usage'] -= 1
      self.quotas['elastic_ip']['usage'] -= 1
      if self.quotas['vm']['usage'] < 0:
        self.quotas['vm']['usage'] = 0 
      if self.quotas['vpc']['usage'] < 0:
        self.quotas['vm']['usage'] = 0
      if self.quotas['elastic_ip']['usage'] < 0:
        self.quotas['vm']['usage'] = 0
      
    estimated_bandwidth = cloud_util.get_max_bandwidth_from_machine_type(self.cloud.name.upper(), vm.machine_type)
    self.virtual_machines.remove(vm)
    self.bandwidth_usage -= estimated_bandwidth
    self.cloud.bandwidth_usage -= estimated_bandwidth
  def update_address_quota(self, quota):
    self.quotas['elastic_ip']['limit'] = quota

  def update_address_usage(self, usage):
    if usage <= self.quotas['elastic_ip']['limit']:
      self.quotas['elastic_ip']['usage'] = usage
      return True
    else:
      return False

  def update_quotas(self, quotas):
    self.quotas = quotas
