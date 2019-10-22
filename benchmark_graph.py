import networkx as nx
import threading
from queue import Queue
import time
import os
import subprocess
import yaml
import threading
import logging

from typing import List, Dict, Tuple, Set
from benchmark import Benchmark
from virtual_machine import VirtualMachine
from region import Region
from absl import flags


FLAGS = flags.FLAGS

logger = None

class BenchmarkGraph():
  """[summary]
  [description]
  """

  def __init__(self, ssh_pub="", ssh_priv="", ssl_cert="", pkb_location="./pkb.py"):

    # get logger
    global logger 
    logger = logging.getLogger('pkb_scheduler')

    self.graph = nx.MultiGraph()
    self.regions = {}
    self.virtual_machines = []
    self.benchmarks = []
    self.benchmark_wait_list = []
    self.vm_total_count = 0
    self.bm_total_count = 0

    self.network = {}
    #TODO create randomized run ID
    self.network['name'] = 'pkb-scheduler'
    self.ssh_private_key_file = ssh_priv
    self.ssh_public_key_file = ssh_pub
    self.ssl_cert_file = ssl_cert
    self.pkb_location = pkb_location

    # TODO parameterize these
    self.gce_project = 'smu-benchmarking'
    self.bigquery_table = 'daily_tests.scheduler_test'
    self.bq_project = 'smu-benchmarking'
    self.generated_config_path = '/home/derek/projects/pkb_scheduler/run_configs/'

    self.vm_creation_times = []
    self.benchmark_run_times = []
    self.deletion_times = []

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

  def required_vm_exists(self, vm):
    # print(os_type)
    for index in self.graph.nodes:
      tmp_vm = self.graph.nodes[index]['vm']
      if vm.vm_spec_is_equivalent(tmp_vm):
        return True
      else:
        continue
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

  def get_list_if_vm_exists(self, vm):
    vm_list = []
    for index in self.graph.nodes:
      tmp_vm = self.graph.nodes[index]['vm']
      if vm.vm_spec_is_equivalent(tmp_vm):
        # print(vm.zone + " Exists")
        vm_list.append(tmp_vm)
      else:
        continue
    return vm_list

  def check_if_can_add_vm(self, vm):
    vm_region = self.get_region_from_zone(vm.cloud, vm.zone)

    if self.regions[vm_region].has_enough_cpus(vm.cpu_count):
      if self.required_vm_exists(vm):
        # returns this is vm exists but there is enough space
        # for another
        return True, "VM Exists. Quota not Exceeded"
      else:
        # returns True if the vm doesn't already exist
        # and if region has enough space
        return True, "VM does not exist"

    return False, "Quota Exceeded"

  def add_vm_if_possible(self, cpu_count, zone,
                         os_type, network_tier, machine_type,
                         cloud, vpn=False):

    vm_region = self.get_region_from_zone(cloud, zone)

    # create virtual_machine object
    vm_id = self.vm_total_count
    vm = VirtualMachine(node_id=vm_id,
                        cpu_count=cpu_count,
                        zone=zone,
                        os_type=os_type,
                        network_tier=network_tier,
                        machine_type=machine_type,
                        cloud=cloud,
                        ssh_private_key=self.ssh_private_key_file,
                        ssl_cert=self.ssl_cert_file)

    # if VM with same specs already exists, return false 0
    tmp_vm_list = self.get_list_if_vm_exists(vm)

    if len(tmp_vm_list) > 0:
      can_add_another, status = self.check_if_can_add_vm(vm)
      if can_add_another and status == "VM Exists. Quota not Exceeded":
        status2 = self.regions[vm_region].add_virtual_machine_if_possible(vm)
        if status2:
          self.virtual_machines.append(vm)
          self.graph.add_node(vm_id, vm=vm)
          self.vm_total_count += 1
          return True, vm
        else:
          logger.debug("QUOTA EXCEEDED")
          return False, None
      else:
        #TODO return a tmp vm from list. fix this
        tmp_vm_index = 0
        min_degree_index = 0
        min_degree = self.graph.degree[tmp_vm_list[0].node_id]
        while tmp_vm_index < len(tmp_vm_list):
          degree = self.graph.degree[tmp_vm_list[tmp_vm_index].node_id]
          if degree < min_degree:
            min_degree_index = tmp_vm_index
          tmp_vm_index += 1
        return False, tmp_vm_list[min_degree_index]

    else:
      # try to add vm to region
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
        logger.debug("QUOTA EXCEEDED")
        return False, None

  def get_list_of_nodes(self):
    return self.graph.nodes

  def get_list_of_edges(self):
    return self.graph.edges

  def add_benchmark(self, new_benchmark, node1, node2):
    #  M[v1][v2]
    # M.add_edges_from([(v1,v2,{'route':45645})])
    new_benchmark.benchmark_id = self.bm_total_count
    self.graph.add_edges_from([(node1, node2, {'bm': new_benchmark})])
    self.bm_total_count += 1

  def maximum_matching(self):
    return nx.max_weight_matching(self.graph, maxcardinality=True)

  def create_vms(self):
    # go through nodes in network. Stand up Vms that have not been created
    node_list = list(self.graph.nodes)

    vm_threads = []

    for index in node_list:
      vm = self.graph.nodes[index]['vm']
      # TODO: thread this bitch
      if vm.status == 'Not Created':
        # vm.create_instance(self.pkb_location)
        t = threading.Thread(target=self.create_vm,
                             args=(vm,))
        vm_threads.append(t)
        t.start()

    for t in vm_threads:
      t.join()
      print("Thread Done")

    for index in node_list:
      vm = self.graph.nodes[index]['vm']
      print("VM INDEX: " + str(index))
      print(vm.status)
      print(vm.run_uri)
      print(vm.creation_output)

  def create_vm(self, vm):
    # TODO check quotas again before create
    vm.create_instance(self.pkb_location)

  def run_benchmark_set(self, bm_list: List[Tuple[int, int]] ):
    """When given a list of tuples, where each element
       in the tuple is a node id, this function figures
       out the benchmark to run between those nodes.
       It then calls a function to create a config file for
       the benchmarks and then runs the benchmarks
    
    """
    benchmarks_to_run = []
    benchmarks_to_run_tuples = []
    print("RUN BENCHMARKS")

    # create benchmark configs for each benchmark in set
    # bm_list is a list of tuples [(n1,n2), (n3,n4)]
    for node_tuple in bm_list:
      # node_tuple is a tuple (node1, node2) of nodes that have
      # an edge between them

      vm_list = []
      vm_list.append(self.graph.nodes[node_tuple[0]]['vm'])
      vm_list.append(self.graph.nodes[node_tuple[1]]['vm'])
      print(self.graph[node_tuple[0]][node_tuple[1]])

      # get dict of benchmarks from vm 0 to vm 1
      bm_dict = dict(self.graph[node_tuple[0]][node_tuple[1]])
      # get list of keys from dict
      bm_key_list = list(bm_dict.keys())

      # get last benchmark on list
      # TODO have some optimization here so benchmarks take similar times?
      bm_index_to_run = len(bm_key_list) - 1

      # edge tuple (node1, node2, key)
      bm_tuple = (node_tuple[0], node_tuple[1], bm_index_to_run)
      # get actual Benchmark object from edge in graph
      bm_to_run = self.graph[node_tuple[0]][node_tuple[1]][bm_index_to_run]['bm']
      print(bm_to_run)
      # create config file, get file name 
      bm_config_file = self.create_benchmark_config_file(bm_to_run, vm_list)
      # add config file to benchmark object
      bm_to_run.config_file = bm_config_file
      benchmarks_to_run.append(bm_to_run)
      benchmarks_to_run_tuples.append(bm_tuple)

    # run benchmark configs
    # run in parallel
    bm_threads = []
    bm_thread_results = []
    bm_thread_counter = 0

    for bm in benchmarks_to_run:
      # TODO change this into a dict?
      bm_data = [bm, benchmarks_to_run_tuples[bm_thread_counter], False]
      bm_thread_results.append(bm_data)
      logger.debug(bm.zone1 + " <-> " + bm.zone2)
      t = threading.Thread(target=self.run_benchmark, 
                           args=(bm,
                                 bm_thread_results,
                                 bm_thread_counter,))
      bm_threads.append(t)
      t.start()
      bm_thread_counter += 1

    for bm_thread in bm_threads:
      bm_thread.join()
      print("thread done")

    print("All threads done")

    # TODO remove successful benchmarks from graph
    for bm_data in bm_thread_results:
      bm = bm_data[0]
      bm_loc = bm_data[1]
      success = bm_data[2]
      if success:
        self.graph.remove_edge(bm_loc[0], bm_loc[1], bm_loc[2])
        print("benchmark removed: " + str(bm_loc))
      else:
        pass


  def run_benchmark(self, bm, result_list, result_index):
    # ./pkb.py --benchmarks=throughput_latency_jitter
    # --benchmark_config_file=static_config2.yaml

    config_file_path = self.generated_config_path + bm.config_file

    cmd = (self.pkb_location 
            + " --benchmarks=" + bm.benchmark_type
            + " --gce_network_name=pkb-scheduler"
            + " --benchmark_config_file=" + bm.config_file
            + " --bigquery_table=" + self.bigquery_table
            + " --bq_project=" + self.bq_project)

    print("RUN BM: " + cmd)
    if FLAGS.no_run:
      result_list[result_index][2] = True
      bm.status = "Executed"
      return True

    process = subprocess.Popen(cmd.split(),
                             stdout=subprocess.PIPE)
    output, error = process.communicate()

    # TODO make this actually do something on failure
    result_list[result_index][2] = True
    bm.status = "Executed"
    return True


  def create_benchmark_config_file(self, bm: Benchmark, vm_list):

    config_yaml = {}
    # config_yaml['static_vms'] = []
    counter = 1
    vm_yaml_list = []

    config_yaml[bm.benchmark_type] = {}
    config_yaml[bm.benchmark_type]['vm_groups'] = {}

    for vm in vm_list:

      temp = config_yaml[bm.benchmark_type]['vm_groups']
      vm_num = 'vm_' + str(counter)
      temp[vm_num] = {}
      temp[vm_num]['static_vms'] = []

      vm_config_dict = {}
      vm_config_dict['user_name'] = 'perfkit'
      vm_config_dict['ssh_private_key'] = vm.ssh_private_key
      vm_config_dict['ip_address'] = vm.ip_address
      vm_config_dict['internal_ip'] = vm.internal_ip
      vm_config_dict['install_packages'] = True
      temp[vm_num]['static_vms'].append(vm_config_dict)

      counter += 1
    # TODO add the flags stuff
    # config_yaml[bm.benchmark_type]['flags'] = {}

    file_name = (self.generated_config_path + "config_" 
                 + str(bm.benchmark_id) + ".yaml")
    file = open(file_name, 'w+')
    yaml.dump(config_yaml, file, default_flow_style=False)

    return file_name

  def remove_benchmark(self):
    pass

  def add_benchmarks_from_waitlist(self):
    
    bms_added = []
    for bm in self.benchmark_wait_list:

      print(bm.zone1)
      print(bm.zone2)

      if bm.zone1 != bm.zone2:
        cpu_count = cpu_count_from_machine_type(bm.cloud, bm.machine_type)

        logger.debug("Trying to add " + bm.zone1 + " and " + bm.zone2)

        success1, tmp_vm1 = self.add_vm_if_possible(cpu_count=cpu_count,
                                                    zone=bm.zone1,
                                                    os_type=bm.os_type,
                                                    network_tier=bm.network_tier,
                                                    machine_type=bm.machine_type,
                                                    cloud=bm.cloud)

        success2, tmp_vm2 = self.add_vm_if_possible(cpu_count=cpu_count,
                                                    zone=bm.zone2,
                                                    os_type=bm.os_type,
                                                    network_tier=bm.network_tier,
                                                    machine_type=bm.machine_type,
                                                    cloud=bm.cloud)

        add_vms_and_benchmark = False
        # added both vms
        if (success1 and success2):
          logger.debug("Added Both")
          add_vms_and_benchmark = True
        # added one, other exists
        elif (success1 and tmp_vm2):
          logger.debug("Added 1")
          add_vms_and_benchmark = True
        # added one, other exsists
        elif (success2 and tmp_vm1):
          logger.debug("Added 1")
          add_vms_and_benchmark = True
        # both exist already
        elif (tmp_vm1 and tmp_vm2):
          logger.debug("Both Exist")
          add_vms_and_benchmark = True

        if add_vms_and_benchmark:
          bm.vms.append(tmp_vm1)
          bm.vms.append(tmp_vm2)
          bm.status = "Not Executed"
          self.benchmarks.append(bm)
          self.add_benchmark(bm, tmp_vm1.node_id, tmp_vm2.node_id)
          bms_added.append(bm)
        else:
          print("WAITLISTED")
          full_graph.benchmark_wait_list.append(bm)

      else:
        print("VM 1 and VM 2 are the same zone")

    for bm in bms_added:
      self.benchmark_wait_list.remove(bm)

    print("Number of benchmarks to run: " + str(len(self.graph.edges)))




  def remove_orphaned_nodes(self):
    node_degree_dict = dict(nx.degree(self.graph))

    # TODO thread this bit
    for key in node_degree_dict.keys():
      if node_degree_dict[key] == 0:
        vm = self.graph.nodes[key]['vm']
        if vm.status == "Running":
          vm.delete_instance(self.pkb_location)
        print("VM removed: " + str(key))
        self.graph.remove_node(key)

        vm_region = self.get_region_from_zone(vm.cloud, vm.zone)
        self.regions[vm_region].remove_virtual_machine(vm)


  def do_it_all(self):
    # this function does it all
    # it finds the maximal set
    # it runs benchmarks
    pass

  def benchmarks_left(self):
    # TODO fix this
    return len(self.graph.edges) + len(self.benchmark_wait_list)


  @staticmethod
  def cpu_count_from_machine_type(cloud, machine_type):
    if cloud == 'GCP':
      return int(machine_type.split('-')[2])
    elif cloud == 'AWS':
      return None
    elif cloud == 'Azure':
      return None
    else:
      return None

