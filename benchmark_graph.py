import region
import virtual_machine
import benchmark
import networkx as nx
import threading
from queue import Queue
import time
import os
import subprocess

class BenchmarkGraph():
  """[summary]
  [description]
  """

  def __init__(self, ssh_pub="", ssh_priv="", ssl_cert="", pkb_location="./pkb.py"):
    self.graph = nx.MultiGraph()
    self.regions = {}
    self.virtual_machines = []
    self.benchmarks = []
    self.benchmark_wait_list = []
    self.vm_total_count = 0

    self.network = {}
    #TODO create randomized run ID
    self.network['name'] = 'pkb-scheduler'
    self.ssh_private_key_file = ssh_priv
    self.ssh_public_key_file = ssh_pub
    self.ssl_cert_file = ssl_cert
    self.pkb_location = pkb_location


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

    for index in self.graph.nodes:
      vm = self.graph.nodes[index]['vm']
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

    for index in self.graph.nodes:
      vm = self.graph.nodes[index]['vm']
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
    vm_id = self.vm_total_count
    vm = virtual_machine.VirtualMachine(node_id=vm_id,
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
      self.graph.add_node(vm_id, vm=vm)
      self.vm_total_count += 1
      return True, vm
    # return false, -1 if not enough space in region
    else:
      return False, None, None

  def get_list_of_nodes(self):
    return self.graph.nodes

  def get_list_of_edges(self):
    return self.graph.edges

  def add_benchmark(self, new_benchmark, node1, node2):
    #  M[v1][v2]
    # M.add_edges_from([(v1,v2,{'route':45645})])
    self.graph.add_edges_from([(node1, node2, {'bm':new_benchmark})])

  def maximum_matching(self):
    return nx.max_weight_matching(self.graph, maxcardinality=True)


  def create_vms(self):
    # go through nodes in network. Stand up Vms that have not been created
    node_list = list(self.graph.nodes)

    for index in node_list:
      vm = self.graph.nodes[index]['vm']
      # TODO: thread this bitch
      create_vm(vm)
    pass

  def create_vm(self, vm):
    # TODO make this more robust
    cmd = (self.pkb_location + " --benchmarks=vm_setup"
            + " --gce_network_name=pkb-scheduler"
            + " --ssh_key_file=" + self.ssh_private_key_file
            + " --ssl_cert_file=" + self.ssl_cert_file
            + " --zones=" + vm.zone
            + " --os_type=" + vm.os_type
            + " --machine_type=" + vm.machine_type
            + " --cloud=" + vm.cloud
            + " --gce_network_tier=" + vm.network_tier
            + " --run_stage=provision,prepare")

    process = subprocess.Popen(cmd.split(),
                             stdout=subprocess.PIPE)
    output, error = process.communicate()

    # TODO change vm state to created
    # get information about this vm from somewhere?

    print(output)
    print(error)