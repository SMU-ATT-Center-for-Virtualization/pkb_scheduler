import cloud_util
import re
import copy
from typing import List, Dict, Tuple, Set, Any, Sequence, Optional
from cloud import Cloud
from virtual_machine import VirtualMachine
# from meta_region import MetaRegion

class Region():
  """Class that represents a region for a cloud provider
  
  Attributes:
      address_quota (int): total number of addresses we can create 
      address_usage (int): number of addresses we have
      bandwidth_limit (int): total bandwidth we can use
      bandwidth_usage (int): bandwidth we are currently using
      cloud (str): Name of cloud
      cpu_quota (int): total number of cpus we can allocate
      cpu_usage (int): number of cpus already allocated
      name (str): region name
      reserved_usage (int): amount of reserved cpus
      virtual_machines (list): list of VirtualMachines in this region
  """

  def __init__(self, 
               region_name: str, 
               cloud: str,
               cpu_quota: int = 0,
               cpu_usage: int = 0,
               bandwidth_limit: Optional[int] = None):
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
    self.meta_region = None


  def get_available_cpus(self) -> int:
    """Get the number of available vCPUs in this cloud region
    
    Returns:
        int: number of vCPUs available
    """
    return self.cpu_quota - self.cpu_usage

  def has_enough_cpus(self, cpu_count: int) -> bool:
    """Checks whether or not we can allocate a machine with <cpu_count> additional cpus
    
    Args:
        cpu_count (int): number of CPUs we are checking if there is room for
    
    Returns:
        bool: True if we can fit cpu_count additional CPUs, false otherwise
    """
    return self.get_available_cpus() >= cpu_count 

  def has_enough_resources(self, cpu_count: int, machine_type: str) -> bool:
    """Checks all the resource quotas for this cloud region.
    Returns whether or not we can add another machine
    
    Args:
        cpu_count (int): Number of cpus on machine we want to add
        machine_type (str): Machine type of instance we want to add
    
    Returns:
        bool: whether or not we can add another machine
    """
    if (self.get_available_cpus() >= cpu_count 
        and self.address_quota > self.address_usage):
      return True
    else:
      return False

  def vm_has_enough_resources(self, vm: VirtualMachine) -> bool:
    """Abstraction of has_enough_resources that takes VirtualMachine as an argument
    
    Args:
        vm (VirtualMachine): VirtualMachine we want to add to region
    
    Returns:
        bool: whether or not we can add another machine
    """
    return self.has_enough_resources(vm.cpu_count, vm.machine_type)

  def add_virtual_machine_if_possible(self, vm: VirtualMachine) -> bool:
    """Adds virtual machine to cloud region if there is enough resources
    
    Args:
        vm (VirtualMachine): VirtualMachine we want to add to region
    
    Returns:
        bool: True if successful, False if not enough space
    """
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

  def remove_virtual_machine(self, vm: VirtualMachine):
    """Remove virtual machine from cloud region
    
    Args:
        vm (VirtualMachine): VirtualMachine to remove
    """

    # TODO add additional safety checks here
    self.virtual_machines.remove(vm)
    self.cpu_usage -= vm.cpu_count
    self.address_usage -= 1

    if self.cpu_usage < 0:
      self.cpu_usage = 0
    if self.address_usage < 0:
      self.address_usage = 0

  def update_cpu_quota(self, quota: int):
    self.cpu_quota = quota

  def update_cpu_usage(self, usage: int):
    if usage <= self.cpu_quota:
      self.cpu_usage = usage
      return True
    else:
      return False

  def update_address_quota(self, quota: int):
    self.address_quota = quota

  def update_address_usage(self, usage: int):
    if usage <= self.address_quota:
      self.address_usage = usage
      return True
    else:
      return False

  def update_quota_usage(self, usages: int):
    if usage <= self.cpu_quota:
      self.cpu_usage = usage
      return True
    else:
      return False

  def update_quotas(self, quotas):
    pass

  def get_all_quotas(self) -> Dict[str,Dict[str,int]]:
    quotas = {}
    quotas[self.name] = {}
    quotas[self.name]['address_usage'] = self.address_usage
    quotas[self.name]['address_quota'] = self.address_quota
    return quotas


class GcpRegion(Region):
  """Class that represents a specific region in Google Cloud
  
  Attributes:
      quotas Dict[str,Any]: dictionary of all the different quotas for this region
  """

  def __init__(self, region_name: str, cloud: str, quotas: Dict[Any,Any], bandwidth_limit: Optional[int] = None):
    self.quotas = quotas # this is a dictionary
    Region.__init__(self, region_name, cloud, bandwidth_limit=bandwidth_limit)

  def _get_cpu_type(self, machine_type: str) -> str:
    """Get the cpu type for a machine type
    
    Args:
        machine_type (str): Full machine type string
    
    Returns:
        str: CPU type for machine type
    """
    cpu_type = cloud_util.cpu_type_from_machine_type('GCP', machine_type)
    if cpu_type.upper() == 'N1':
      cpu_type = 'CPUS'
    else:
      cpu_type = cpu_type.upper() + '_CPUS'
    return cpu_type

  def get_available_cpus(self, machine_type: str) -> int:
    """Get the number of available vCPUs in this cloud region
    
    Args:
        machine_type (str): machine type, ex n2-standard-2, c2-standard-60
    
    Returns:
        int: number of vCPUs available for the specified machine type
    """
    cpu_type = self._get_cpu_type(machine_type)
    return self.quotas[cpu_type]['limit'] - self.quotas[cpu_type]['usage']

  def has_enough_cpus(self, cpu_count: int, machine_type: str) -> bool:
    """Checks whether or not we can allocate a machine with <cpu_count> additional cpus
    
    Args:
        cpu_count (int): number of CPUs we are checking if there is room for
        machine_type (str): machine type for the cpu type we are using
    
    Returns:
        bool: True if we can fit cpu_count additional CPUs, false otherwise
    """
    return self.get_available_cpus(machine_type) >= cpu_count

  def has_enough_resources(self, cpu_count, machine_type, estimated_bandwidth):
    """Checks all the resource quotas for this cloud region.
    Returns whether or not we can add another machine
    
    Args:
        cpu_count (int): Number of cpus on machine we want to add
        machine_type (str): Machine type of instance we want to add
        estimated_bandwidth (int): estimated bandwidth vm will use
    
    Returns:
        bool: whether or not we can add another machine
    """
    if estimated_bandwidth < 0:
      estimated_bandwidth = 0
    # estimated_bandwidth = cloud_util.get_max_bandwidth_from_machine_type('GCP', machine_type)
    if (self.get_available_cpus(machine_type) >= cpu_count and self.quotas['IN_USE_ADDRESSES']['limit'] > self.quotas['IN_USE_ADDRESSES']['usage']):
      if not self.bandwidth_limit or estimated_bandwidth + self.bandwidth_usage <= self.bandwidth_limit:
        if not self.cloud.bandwidth_limit or estimated_bandwidth + self.cloud.bandwidth_usage <= self.cloud.bandwidth_limit:
          if self.meta_region_has_enough_resources(estimated_bandwidth):
            return True
  
    return False


  def meta_region_has_enough_resources(self, estimated_bandwidth) -> bool:
    return self.meta_region.has_enough_resources(estimated_bandwidth)

  def vm_has_enough_resources(self, vm: VirtualMachine) -> bool:
    """Abstraction of has_enough_resources that takes VirtualMachine as an argument
    
    Args:
        vm (VirtualMachine): VirtualMachine we want to add to region
    
    Returns:
        bool: whether or not we can add another machine
    """
    return self.has_enough_resources(vm.cpu_count, vm.machine_type, vm.estimated_bandwidth)

  def add_virtual_machine_if_possible(self, vm: VirtualMachine) -> bool:
    """Adds virtual machine to cloud region if there is enough resources
    
    Args:
        vm (VirtualMachine): VirtualMachine we want to add to region
    
    Returns:
        bool: True if successful, False if not enough space
    """
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

  def remove_virtual_machine(self, vm: VirtualMachine):
    """Remove virtual machine from cloud region
    
    Args:
        vm (VirtualMachine): VirtualMachine to remove
    """

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

  def update_cpu_quota(self, quota: int, machine_type: str):
    cpu_type = self._get_cpu_type(machine_type)
    self.quotas[cpu_type]['limit'] = quota

  def update_cpu_usage(self, usage: int, machine_type: str):
    cpu_type = self._get_cpu_type(machine_type)
    if usage <= self.quotas[cpu_type]['limit']:
      self.quotas[cpu_type]['limit'] = quota
      return True
    else:
      return False

  def update_address_quota(self, quota: int):
    self.quotas['IN_USE_ADDRESSES']['limit'] = quota

  def update_address_usage(self, usage: int) -> bool:
    if usage <= self.quotas['IN_USE_ADDRESSES']['limit']:
      self.quotas['IN_USE_ADDRESSES']['usage'] = usage
      return True
    else:
      return False

  def update_quotas(self, quotas: Dict[Any,Any]):
    self.quotas = quotas

  def get_all_quotas(self) -> Dict[str,Dict[Any,Any]]:
    quotas = {}
    quotas[self.name] = copy.deepcopy(self.quotas)
    return quotas


class AwsRegion(Region):
  """Class that represents a specific region in AWS
  
  Attributes:
      quotas Dict[str,Any]: dictionary of all the different quotas for this region
  """
  
  def __init__(self, region_name: str, cloud: str, quotas: Dict[Any,Any], bandwidth_limit: Optional[int] = None):
    self.quotas = quotas # this is a dictionary
    Region.__init__(self, region_name, cloud, bandwidth_limit=bandwidth_limit)

  def has_enough_resources(self, cpu_count: int, machine_type: str, estimated_bandwidth: Optional[int]) -> bool:
    """Checks all the resource quotas for this cloud region.
    Returns whether or not we can add another machine
    
    Args:
        cpu_count (int): Number of cpus on machine we want to add
        machine_type (str): Machine type of instance we want to add
        estimated_bandwidth (int): estimated bandwidth vm will use
    
    Returns:
        bool: whether or not we can add another machine
    """
    estimated_bandwidth = cloud_util.get_max_bandwidth_from_machine_type('AWS', machine_type)
    if (self.quotas['vm']['usage'] < self.quotas['vm']['limit']
      and self.quotas['elastic_ip']['usage'] < self.quotas['elastic_ip']['limit']
      and self.quotas['vpc']['usage'] < self.quotas['vpc']['limit']):
      # If we are checking bandwidth limits, and if this exceeds limit
      if not self.bandwidth_limit or estimated_bandwidth + self.bandwidth_usage <= self.bandwidth_limit:
        if not self.cloud.bandwidth_limit or estimated_bandwidth + self.cloud.bandwidth_usage <= self.cloud.bandwidth_limit:
          return True
  
    return False

  def vm_has_enough_resources(self, vm: VirtualMachine) -> bool:
    """Abstraction of has_enough_resources that takes VirtualMachine as an argument
    
    Args:
        vm (VirtualMachine): VirtualMachine we want to add to region
    
    Returns:
        bool: whether or not we can add another machine
    """
    return self.has_enough_resources(vm.cpu_count, vm.machine_type, -1)

  def add_virtual_machine_if_possible(self, vm: VirtualMachine) -> bool:
    """Adds virtual machine to cloud region if there is enough resources
    
    Args:
        vm (VirtualMachine): VirtualMachine we want to add to region
    
    Returns:
        bool: True if successful, False if not enough space
    """
    if self.has_enough_resources(vm.cpu_count, vm.machine_type, -1):
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

  def remove_virtual_machine(self, vm: VirtualMachine):
    """Remove virtual machine from cloud region
    
    Args:
        vm (VirtualMachine): VirtualMachine to remove
    """

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

  def update_address_quota(self, quota: int):
    self.quotas['elastic_ip']['limit'] = quota

  def update_address_usage(self, usage: int):
    if usage <= self.quotas['elastic_ip']['limit']:
      self.quotas['elastic_ip']['usage'] = usage
      return True
    else:
      return False

  def update_quotas(self, quotas: Dict[Any,Any]):
    self.quotas = quotas


# Troy, I've split region into subclasses for each cloud
# so put all the azure stuff in this class
# the only absolutely necessary functions are has_enough_resources, add_virtual_machine_if_possible and remove_virtual_machine
# What is currently in this class is just a duplicate of AwsRegion.
class AzureRegion(Region):
  """Class that represents a specific region in Azure
  
  Attributes:
      quotas Dict[str,Any]: dictionary of all the different quotas for this region
  """

  def __init__(self, region_name, cloud, quotas, bandwidth_limit=None):
    # self.my_var = 123
    self.quotas = quotas # this is a dictionary
    Region.__init__(self, region_name, cloud, bandwidth_limit=bandwidth_limit)

  def has_enough_resources(self, cpu_count: int, machine_type: str, estimated_bandwidth: Optional[int] = -1):
    """Checks all the resource quotas for this cloud region.
    Returns whether or not we can add another machine
    
    Args:
        cpu_count (int): Number of cpus on machine we want to add
        machine_type (str): Machine type of instance we want to add
        estimated_bandwidth (int): estimated bandwidth vm will use
    
    Returns:
        bool: whether or not we can add another machine
    """
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

  def vm_has_enough_resources(self, vm: VirtualMachine) -> bool:
    """Abstraction of has_enough_resources that takes VirtualMachine as an argument
    
    Args:
        vm (VirtualMachine): VirtualMachine we want to add to region
    
    Returns:
        bool: whether or not we can add another machine
    """
    return self.has_enough_resources(vm.cpu_count, vm.machine_type, estimated_bandwidth=-1)

  def add_virtual_machine_if_possible(self, vm: VirtualMachine) -> bool:
    """Adds virtual machine to cloud region if there is enough resources
    
    Args:
        vm (VirtualMachine): VirtualMachine we want to add to region
    
    Returns:
        bool: True if successful, False if not enough space
    """
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
            verified_machine_type = verified_machine_type[0:counter] + verified_machine_type[counter+1:]
          counter += 1
          previous = x
        verified_machine_type = verified_machine_type.replace("Standard_", "")
        verified_machine_type = verified_machine_type.replace("_", "")
        verified_machine_type = verified_machine_type.upper()
        full_machine_string = "STANDARD " + verified_machine_type + " FAMILY VCPUS"
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

  def remove_virtual_machine(self, vm: VirtualMachine):
    """Remove virtual machine from cloud region
    
    Args:
        vm (VirtualMachine): VirtualMachine to remove
    """

    if self.cloud.name.upper() == "AZURE":
      verified_machine_type = vm.machine_type

      previous = ""
      counter = 0 
      for x in vm.machine_type:
        if x.isdigit() and previous != 'v':
          #print(f"Before type is: {verified_machine_type}")
          verified_machine_type = verified_machine_type[0:counter] + verified_machine_type[counter+1:]
          #print(f"After type is: {verified_machine_type}")
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
