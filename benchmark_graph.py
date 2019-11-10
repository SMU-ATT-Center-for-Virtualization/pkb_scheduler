import networkx as nx
import matplotlib.pyplot as plt
import threading
import multiprocessing as mp
from queue import Queue
import time
import os
import subprocess
import yaml
import threading
import logging
import math

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
    self.bigquery_table = 'daily_tests.scheduler_test_1'
    self.bq_project = 'smu-benchmarking'
    self.generated_config_path = 'run_configs/'

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

  def print_graph(self):
    nx.draw(self.graph)
    plt.show()


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

    if self.regions[vm_region].has_enough_resources(vm.cpu_count):
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

    # if a vm already exists
    if len(tmp_vm_list) > 0:
      can_add_another, status = self.check_if_can_add_vm(vm)
      # if there is room to add a duplicate vm and if flags allow it 
      if (can_add_another 
          and status == "VM Exists. Quota not Exceeded" 
          and FLAGS.allow_duplicate_vms == True):
        status2 = self.regions[vm_region].add_virtual_machine_if_possible(vm)
        if status2:
          self.virtual_machines.append(vm)
          self.graph.add_node(vm_id, vm=vm)
          self.vm_total_count += 1
          return True, vm
        else:
          logger.debug("QUOTA EXCEEDED")
          return False, None
      #if not room in quota, return duplicate vm with lowest degree
      else:
        tmp_vm_index = 0
        min_degree_index = 0
        min_degree = self.graph.degree[tmp_vm_list[0].node_id]
        while tmp_vm_index < len(tmp_vm_list):
          degree = self.graph.degree[tmp_vm_list[tmp_vm_index].node_id]
          if degree < min_degree:
            min_degree_index = tmp_vm_index
          tmp_vm_index += 1
        return False, tmp_vm_list[min_degree_index]
    # if vm does not exist yet
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

  def get_benchmark_set(self):
    # return bm_list: List[Tuple[int, int]]   list of tuples  [(node1, node2)]


    # convert multigraph to simplified graph with weighted edges
    node_degree_dict = dict(nx.degree(self.graph))
    tmp_graph = nx.Graph()
    tmp_graph.add_nodes_from(self.graph.nodes)
    edges_list = []
    for e in self.graph.edges:
      edges_list.append(e[0:2])

    edges_set = set(edges_list)
    edges_to_add = []

    # calculate weight based on degree of each node (n1.degree * n2.degree)
    for e in edges_set:
      c = 1 / (node_degree_dict[e[0]] * node_degree_dict[e[1]])
      t = (e[0], e[1], c)
      edges_to_add.append(t)

    tmp_graph.add_weighted_edges_from(edges_to_add)
    print("NODES:")
    print(tmp_graph.nodes)
    print("EDGES:")
    print(tmp_graph.edges)
    for e in tmp_graph.edges:
      print(tmp_graph.edges[e])

    node_list = self.graph.nodes

    # for i in node_list:
    #   node_list[i]['evenlevel'] = math.inf
    #   node_list[i]['oddlevel'] = math.inf
    #   node_list[i]['blossom'] = None
    #   node_list[i]['predecessors'] = []
    #   node_list[i]['anomalies'] = []
    #   node_list[i]['visited'] = False

    # for e in self.graph.edges:
    #   self.graph.edges[e]['visited'] = False
    #   self.graph.edges[e]['used'] = False

    return nx.max_weight_matching(tmp_graph, maxcardinality=True, weight='weight')


  def create_vms(self):
    # TODO add max thread logic here, make sure things are stood up in a reasonable way
    # go through nodes in network. Stand up Vms that have not been created
    max_processes = FLAGS.max_processes

    node_list = list(self.graph.nodes)
    node_index = 0
    created_nodes = []

    logger.debug("LENGTH NODE LIST: " + str(len(node_list)))

    while node_index < len(node_list):
      vm_processes = []
      thread_count = 0
      while (thread_count < max_processes or max_processes < 0) and node_index < len(node_list) :
      # for index in node_list:

        logger.debug(node_index)
        vm = self.graph.nodes[node_index]['vm']

        vm_proc_container = {}
        vm_proc_container['vm'] = vm


        if vm.status == 'Not Created':
          # vm.create_instance(self.pkb_location)
          # t = threading.Thread(target=self.create_vm,
          #                      args=(vm,))
          queue = mp.Queue()
          p = mp.Process(target=self.create_vm_process,
                         args=(vm, queue))
          vm_proc_container['process'] = p
          vm_proc_container['data'] = queue

          vm_processes.append(vm_proc_container)
          p.start()
          created_nodes.append(node_index)
          thread_count += 1

        node_index += 1

      for container in vm_processes:
        p = container['process']
        new_vm = container['data'].get()
        container['vm'].copy_contents(new_vm)
        print("VM PROCESS INFO STUFF HERE:")
        print("IN PROCESS VM:")
        print(container['vm'].creation_output)
        print("OUT PROCESS VM:")
        print(new_vm.creation_output)
        p.join()
        print("Process Done")

    for index in created_nodes:
      vm = self.graph.nodes[index]['vm']
      self.vm_creation_times.append(vm.creation_time)
      print("VM INDEX: " + str(index))
      print(vm.status)
      print(vm.run_uri)
      print(vm.creation_output)

  def create_vm_process(self, vm, queue):
    vm.create_instance(self.pkb_location)
    queue.put(vm)

  def create_vm(self, vm):
    vm.create_instance(self.pkb_location)

  def run_benchmark_set(self, bm_list: List[Tuple[int, int]]):
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
    
    bm_all_thread_results = []
    # bm_thread_result_counter = 0
    bm_index = 0

    max_threads = FLAGS.max_threads

    while bm_index < len(benchmarks_to_run):
      bm_threads = []
      
      thread_count = 0
      while (thread_count < max_threads or max_threads < 0) and bm_index < len(benchmarks_to_run):
        # TODO change this into a dict?
        bm = benchmarks_to_run[bm_index]

        # make sure that all vms for benchmark have been created
        # TODO try to ssh into it to make sure it is up
        #      or at least ping it
        vms_created = True
        for vm in bm.vms:
          if vm.status != "Running":
            vms_created = False

        if not vms_created:
          print("DO NOT RUN")
          bm_index += 1
          continue

        queue = mp.Queue()
        logger.debug(bm.zone1 + " <-> " + bm.zone2)
        p = mp.Process(target=self.run_benchmark_process, 
                             args=(bm,
                                   benchmarks_to_run_tuples[bm_index],
                                   bm_index,
                                   queue))
        
        bm_data = {}
        bm_data['bm'] = bm
        bm_data['tuple'] = benchmarks_to_run_tuples[bm_index]
        bm_data['process'] = p
        bm_data['queue'] = queue
        # bm_data = [bm, benchmarks_to_run_tuples[bm_index], p, queue]
        bm_all_thread_results.append(bm_data)
        bm_threads.append(bm_data)
        p.start()
        bm_index += 1
        thread_count += 1

        # TODO check to make sure both VMS are created

      for bm_data in bm_threads:
        bm_data['process'].join()
        results_dict = bm_data['queue'].get()
        self.benchmark_run_times.append(results_dict['run_time'])
        bm_data['bm'].status = results_dict['status']
        bm_data['success'] = results_dict['success']
        print("thread done")

    print("All threads done")

    # TODO remove only successful benchmarks from graph
    for bm_data in bm_all_thread_results:
      bm = bm_data['bm']
      bm_loc = bm_data['tuple']
      success = bm_data['success']
      if success:
        self.graph.remove_edge(bm_loc[0], bm_loc[1], bm_loc[2])
        print("benchmark removed: " + str(bm_loc))
      else:
        pass


  def run_benchmark_process(self, bm, bm_tuple, result_index, queue):
    # ./pkb.py --benchmarks=throughput_latency_jitter
    # --benchmark_config_file=static_config2.yaml

    config_file_path = self.generated_config_path + bm.config_file

    results_dict = {}
    results_dict['bm_tuple'] = bm_tuple
    results_dict['result_index'] = result_index
    results_dict['bm'] = bm
    results_dict['success'] = False
    results_dict['run_time'] = None

    cmd = (self.pkb_location 
            + " --benchmarks=" + bm.benchmark_type
            + " --gce_network_name=pkb-scheduler"
            + " --benchmark_config_file=" + bm.config_file
            + " --bigquery_table=" + self.bigquery_table
            + " --bq_project=" + self.bq_project)

    print(bm_tuple)
    print(bm.zone1)
    print(bm.zone2)
    print(bm.config_file)
    print("RUN BM: " + cmd)
    if FLAGS.no_run:
      results_dict['status'] = "Executed"
      bm.status = "Executed"
      results_dict['success'] = True
      queue.put(results_dict)
      return

    start_time = time.time()
    process = subprocess.Popen(cmd.split(),
                             stdout=subprocess.PIPE)
    output, error = process.communicate()
    end_time = time.time()
    run_time = end_time - start_time
    # self.benchmark_run_times.append(run_time)

    # TODO make this actually do something on failure
    bm.status = "Executed"
    results_dict['status'] = "Executed"
    results_dict['success'] = True
    results_dict['run_time'] = run_time
    queue.put(results_dict)
    return


  def create_benchmark_config_file(self, bm, vm_list):

    config_yaml = {}
    counter = 1
    vm_yaml_list = []

    config_yaml[bm.benchmark_type] = {}
    config_yaml[bm.benchmark_type]['vm_groups'] = {}
    config_yaml[bm.benchmark_type]['flags'] = bm.flags
    config_flags = config_yaml[bm.benchmark_type]['flags']

    config_flags.pop("zones", None)
    config_flags.pop("extra_zones", None)
    config_flags.pop("machine_type", None)
    # config_flags.pop("cloud", None)
    config_flags["static_cloud_metadata"] = bm.cloud
    config_flags["static_network_tier_metadata"] = bm.network_tier


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
      vm_config_dict['zone'] = vm.zone
      vm_config_dict['machine_type'] = vm.machine_type
      temp[vm_num]['static_vms'].append(vm_config_dict)

      counter += 1

    file_name = (self.generated_config_path + "config_" 
                 + str(bm.benchmark_id) + ".yaml")
    file = open(file_name, 'w+')
    yaml.dump(config_yaml, file, default_flow_style=False)
    file.close()

    return file_name

  def remove_benchmark(self):
    pass

  def add_benchmarks_from_waitlist(self):
    
    bms_added = []
    for bm in self.benchmark_wait_list:

      print(bm.zone1)
      print(bm.zone2)

      if bm.zone1 != bm.zone2:
        cpu_count = self.cpu_count_from_machine_type(bm.cloud, bm.machine_type)

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
          self.benchmark_wait_list.append(bm)

      else:
        print("VM 1 and VM 2 are the same zone")

    # Remove bm from waitlist if it is added to graph
    for bm in bms_added:
      self.benchmark_wait_list.remove(bm)

    print("Number of benchmarks to run: " + str(len(self.graph.edges)))




  def remove_orphaned_nodes(self):
    # TODO check waitlist before remove node

    # get dictionary of node degrees from graph
    node_degree_dict = dict(nx.degree(self.graph))

    vm_threads = []
    keys_to_remove = []
    vm_removed_count = 0

    # start threads to remove vms
    for key in node_degree_dict.keys():
      # if a node has no edges
      if node_degree_dict[key] == 0:
        vm = self.graph.nodes[key]['vm']
        if vm.status == "Running":

          # TODO check if waitlist needs this node before removal

          # vm.delete_instance(self.pkb_location)
          keys_to_remove.append(key)
          t = threading.Thread(target=vm.delete_instance,
                             args=(self.pkb_location,))
          vm_threads.append(t)
          t.start()
       
    # join threads
    for t in vm_threads:
      t.join()
      print("Thread Done")

    for key in keys_to_remove:
      vm = self.graph.nodes[key]['vm']
      print("VM removed: " + str(key))
      self.graph.remove_node(key)
      vm_region = self.get_region_from_zone(vm.cloud, vm.zone)
      self.regions[vm_region].remove_virtual_machine(vm)
      vm_removed_count += 1

    return vm_removed_count

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

