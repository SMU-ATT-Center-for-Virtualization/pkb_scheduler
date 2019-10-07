import region
import virtual_machine
import graph
import benchmark

class BenchmarkGraph(graph.Graph):
  """[summary]
  [description]
  """

  def __init__(self):
    self.regions = {}
    self.virtual_machines = []
    self.benchmarks = []
    self.benchmark_wait_list = []
    self.vm_total_count = 0
    graph.Graph.__init__(self)

    self.regions = self.node_groups
    self.virtual_machines = self.nodes

  def add_region_if_not_exists(self, new_region):
    if new_region.name not in self.regions:
      self.regions[new_region.name] = new_region

  def region_exists(self, region_name):
    return region_name in self.regions

  def get_available_cpus(self, region_name):
    return region['region_name'].get_available_cpus()

  def get_region_from_zone(self, cloud, zone):
    if cloud == 'GCP':
      return zone[:len(zone) - 2]
    else:
      return None

  def required_vm_exists(self, cloud, zone, machine_type,
                         network_tier, os_type, vpn=False):

    # print(os_type)

    for vm in self.virtual_machines:

      if (vm.cloud == cloud and
          vm.zone == zone and
          vm.machine_type == machine_type and
          vm.network_tier == network_tier and
          vm.vpn == vpn and
          vm.os_type == os_type):
        # print(vm.zone + " Exists")
        return True
      else:
        # print(vm.zone + " Does not Exist")
        pass

    return False

  def get_vm_if_exists(self, cloud, zone, machine_type,
                         network_tier, os_type, vpn=False):

    for vm in self.virtual_machines:

      if (vm.cloud == cloud and
          vm.zone == zone and
          vm.machine_type == machine_type and
          vm.network_tier == network_tier and
          vm.vpn == vpn and
          vm.os_type == os_type):
        # print(vm.zone + " Exists")
        return vm
      else:
        pass

    return None


  def check_if_can_add_vm(self, cpu_count, zone,
                          os_type, network_tier, machine_type,
                          cloud, vpn=False):
    vm_region = self.get_region_from_zone(cloud, zone)

    if self.regions[vm_region].has_enough_cpus(cpu_count):
      if self.required_vm_exists(cloud, zone, machine_type,
                                 network_tier, os_type, vpn):
        return False, "VM exists"
      # returns True if the vm doesn't already exist
      # and if region has enough space
      else:
        return True, 1
    
    return False, "Quota Exceeded"


  def add_vm_if_possible(self, cpu_count, zone,
                         os_type, network_tier, machine_type,
                         cloud, vpn=False):
    vm_region = self.get_region_from_zone(cloud, zone)

    # if VM with same specs already exists, return false 0
    tmp_vm = self.get_vm_if_exists(cloud, zone, machine_type,
                                   network_tier, os_type, vpn)
    if tmp_vm:
      return False, tmp_vm

    # create virtual_machine object
    vm = virtual_machine.VirtualMachine(node_id=self.vm_total_count,
                                        cpu_count=cpu_count,
                                        zone=zone,
                                        os_type=os_type,
                                        network_tier=network_tier,
                                        machine_type=machine_type,
                                        cloud=cloud)

    #try to add vm to region
    status = self.regions[vm_region].add_virtual_machine_if_possible(vm)

    # if successful, also add that vm to virtual_machines list
    # and increment total number of vms, return True, 1
    if status is True:
      print("adding vm in zone " + vm.zone)
      self.virtual_machines.append(vm)
      self.vm_total_count += 1
      return True, vm
    # return false, -1 if not enough space in region
    else:
      return False, None
