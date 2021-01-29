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
import cloud_util
import time

import json
from deprecated import deprecated
from typing import List, Dict, Tuple, Set
from benchmark import Benchmark
from virtual_machine import VirtualMachine
from region import Region
from absl import flags


FLAGS = flags.FLAGS

logger = None

class BenchmarkGraph():
  """Graph of VMs and benchmarks to be run between them
  
  A container of several datastructures and associated logic
  1. A graph of VMs (nodes) and benchmarks (edges)
  2. Region objects, representing regional quotas for clouds
  3. benchmark waitlist, a list of benchmarks and associated VMs 
     that have not been allocated or added to the graph yet
  4. metrics about the VMs and benchmarks
  """

  def __init__(self, ssh_pub="", ssh_priv="", ssl_cert="", pkb_location="./pkb.py",
               bigquery_table="daily_tests.scheduler_test_1",
               bq_project="smu-benchmarking"):

    # get logger
    global logger 
    logger = logging.getLogger('pkb_scheduler')

    self.graph = nx.MultiGraph()
    self.regions = {}
    self.clouds = {}
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
    self.bigquery_table = bigquery_table
    self.bq_project = bq_project
    self.generated_config_path = 'run_configs/'

    self.vm_creation_times = []
    self.benchmark_run_times = []
    self.deletion_times = []

  def add_cloud_if_not_exists(self, cloud):
    if cloud.name not in self.clouds:
      self.clouds[cloud.name] = cloud

  def cloud_exists(self, cloud):
    return cloud in self.clouds

  def add_region_if_not_exists(self, new_region):
    print(f"new_region is: {new_region}")
    if new_region.name not in self.regions:
      print(f"\n\n***********adding new region: {new_region.__dict__}******************\n\n")
      self.regions[new_region.name] = new_region

  def region_exists(self, region_name):
    return region_name in self.regions

  def get_available_cpus(self, region_name):
    return region['region_name'].get_available_cpus()

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

  @deprecated(reason="Use get_list_if_vm_exists instead")
  def get_vm_if_exists(self, cloud, zone, machine_type,
                       network_tier, os_type, vpn=False):
    """Tries to find a VM in the graph that match the given parameters

    Searches the graph nodes to find a VM
    that matches the parameters passed in
    """
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
    """Tries to find a VM in the graph with equivalent specs

    Searches graph nodes and returns a list of VMs
    with identical specs to the argument vm

    Args:
      vm: vm with specs to search for

    Returns:
      list of VMs with matching specs to parameter
      list[virtual_machine.VirtualMachine]
    """
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
    """Checks if there is enough space in the region to add VM

    Checks region quotas to see if it can add VM

    Args:
      vm: Virtual Machine to add

    Returns:
      Boolean and description of if it can/cannot add and why
      bool, String
    """
    vm_region = cloud_util.get_region_from_zone(vm.cloud, vm.zone)
    print(f"vm is: {vm.__dict__}")
    print(f"self.regions: {self.regions}")
    print(f"vm_region: {vm_region}")
    quota_not_exceeded = True
    print(f"\n\n You are in the check if can add method \n\n")
    print(f"vm.cloud.lower() is: {vm.cloud.lower()}")
    if vm.cloud.lower() == "aws":
      print(f"inside the aws add portion")
      if self.regions[vm_region].has_enough_resources(vm.cpu_count, vm.cloud.lower(), vm_region):
        if self.required_vm_exists(vm):
          # returns this is vm exists but there is enough space
          # for another
          print(f"VM exists")
          return True, "VM Exists. Quota not Exceeded"
        else:
          # returns True if the vm doesn't already exist
          # and if region has enough space
          print(f"VM does not exist")
          return True, "VM does not exist"
    elif vm.cloud.lower() == 'gcp':
      print(f"inside the gcp add portion")
      if self.regions[vm_region].has_enough_resources(vm.cpu_count, vm.cloud.lower(), vm_region):
        if self.required_vm_exists(vm):
          # returns this is vm exists but there is enough space
          # for another
          return True, "VM Exists. Quota not Exceeded"
        else:
          # returns True if the vm doesn't already exist
          # and if region has enough space
          return True, "VM does not exist"
    # if quota_not_exceeded:
    #   return True
    print(f"Missed both portions, or quota was exceeded")
    return False, "Quota Exceeded"

  @deprecated(reason="Use add_vms_for_benchmark_if_possible instead")
  def add_vm_if_possible(self, cpu_count, zone,
                         os_type, network_tier, machine_type,
                         cloud, vpn=False, same_zone=False):
    """[summary]

    [description]

    Args:
      cpu_count: [description]
      zone: [description]
      os_type: [description]
      network_tier: [description]
      machine_type: [description]
      cloud: [description]
      vpn: [description] (default: {False})

    Returns:
      [description]
      [type]
    """

    # Need region because quotas are regional
    vm_region = cloud_util.get_region_from_zone(cloud, zone)
    print(f"\n\nThe CPU Count is: {cpu_count}\n\n")
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
      # then add the VM
      if (can_add_another 
          and status == "VM Exists. Quota not Exceeded" 
          and (FLAGS.allow_duplicate_vms == True or same_zone==True)):
        success = self.regions[vm_region].add_virtual_machine_if_possible(vm)
        if success:
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
      # and increment total number of vms, return True, and the vm
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

  def add_or_waitlist_benchmark_and_vms(self, bm, region_dict=0):
    print(f"bm is {bm.__dict__}")
    vms = self.add_vms_for_benchmark_if_possible(bm, region_dict)
    print(f"\n\nAdded the vms to benchmark\n\n")
    vms_no_none = list(filter(None, vms))

    if len(bm.vm_specs) == len(vms_no_none) == len(vms):
      bm.vms = vms
      self.benchmarks.append(bm)
      self.add_benchmark_as_edge(bm, vms[0].node_id, vms[1].node_id)
      return bm.vms, "Added"
    else:
      logger.debug("BM WAITLISTED")
      bm.status = "Waitlist"
      self.benchmark_wait_list.append(bm)
      return [], "Waitlisted"


  def add_vms_for_benchmark_if_possible(self, bm, region_dict=0):
    """[summary]
    
    [description]
    
    Args:
      bm: [description]
    
    Returns:
      [description]
      bool
    """
    vm_ids = []
    vms = []

    print(f"\n\nadd_vms_for_benchmark: {bm.__dict__}\n\n")
    print(f"self in add_vms_for_benchmark_if_possible: {self.__dict__}")
    for vm_spec in bm.vm_specs:
      print(f"\nThe vm_specs are: {vm_spec.__dict__}\n")
      print(vm_spec.id)
      vm_region = cloud_util.get_region_from_zone(vm_spec.cloud, vm_spec.zone)
      print(f"\n\nVM_Region is: {vm_region}\n\n")
      print(f"\n\nThe CPU Count is: {vm_spec.cpu_count}\n\n")
      if vm_spec.cloud.lower() == "aws" :
        region_list_command = "aws ec2 describe-instances --query Reservations[].Instances[]"
        process = subprocess.Popen(region_list_command, stdout=subprocess.PIPE, shell=True)
        output, error = process.communicate()
        number_of_spun_up_machines = json.loads(output.decode('utf-8'))
        print(f"\n\nnumber_of_spun_up_machines: {len(number_of_spun_up_machines)}\n\n")
      #here we will get the number of computers spun up for aws
      vm_id = self.vm_total_count
      if vm_spec.cloud.lower() == "aws":
        vm = VirtualMachine(node_id=vm_id,
                            cpu_count=vm_spec.cpu_count,
                            zone=vm_spec.zone,
                            os_type=vm_spec.os_type,
                            network_tier=vm_spec.network_tier,
                            machine_type=vm_spec.machine_type,
                            cloud=vm_spec.cloud,
                            min_cpu_platform=vm_spec.min_cpu_platform,
                            ssh_private_key=self.ssh_private_key_file,
                            ssl_cert=self.ssl_cert_file,
                            vm_spec=vm_spec,
                            vm_spec_id=vm_spec.id,
                            temp_vm_aws_limit = 1920,
                            temp_vm_spun_up_machines = len(number_of_spun_up_machines))
      else:
        vm = VirtualMachine(node_id=vm_id,
                            cpu_count=vm_spec.cpu_count,
                            zone=vm_spec.zone,
                            os_type=vm_spec.os_type,
                            network_tier=vm_spec.network_tier,
                            machine_type=vm_spec.machine_type,
                            cloud=vm_spec.cloud,
                            min_cpu_platform=vm_spec.min_cpu_platform,
                            ssh_private_key=self.ssh_private_key_file,
                            ssl_cert=self.ssl_cert_file,
                            vm_spec=vm_spec,
                            vm_spec_id=vm_spec.id)
      # if VM with same specs already exists, return false 0
      tmp_vm_list = self.get_list_if_vm_exists(vm)

      suitable_vm_found = False

      # if a vm already exists
      print(f"\n\ntmp_vm_list: {tmp_vm_list}\n\n")
      if len(tmp_vm_list) > 0:
        print(f"made it into tmp_vm_list if")
        can_add_another, status = self.check_if_can_add_vm(vm)

        add_from_list = True

        # if there is room to add a duplicate vm and if flags allow it 
        # then add the VM
        if (can_add_another 
            and status == "VM Exists. Quota not Exceeded"
            and FLAGS.allow_duplicate_vms == True
            and len(tmp_vm_list) < FLAGS.max_duplicate_vms + 1):
          print("here1")
          # checks if there is enough space in a region to add another vm
          new_region = Region(region_name=vm_region, cloud=vm_spec.cloud.lower())
          self.add_region_if_not_exists(new_region)
          print(f"\n\nself, but the first time it adds a region: {self.__dict__}\n\n")
          success = self.regions[vm_region].add_virtual_machine_if_possible(vm)
          if success:
            add_from_list = False
            self.virtual_machines.append(vm)
            self.graph.add_node(vm_id, vm=vm)
            vms.append(vm)
            vm_ids.append(vm.node_id)
            self.vm_total_count += 1
            suitable_vm_found = True
            continue
          else:
            logger.debug("QUOTA EXCEEDED. add from existing")
            # TODO add to tmp_list
            add_from_list = True
        # if not room in quota, return duplicate vm with lowest degree
        elif add_from_list:
          tmp_vm_index = 0
          min_degree_index = 0
          # find initial min_degree_index that has not been used 
          # by a vm in this benchmark

          while tmp_vm_index < len(tmp_vm_list):
            if tmp_vm_list[tmp_vm_index].node_id in vm_ids:
              tmp_vm_index += 1
            else:
              min_degree = self.graph.degree[tmp_vm_list[tmp_vm_index].node_id]
              min_degree_index = tmp_vm_index
              tmp_vm_index += 1
              suitable_vm_found = True
              break

          while tmp_vm_index < len(tmp_vm_list):
            degree = self.graph.degree[tmp_vm_list[tmp_vm_index].node_id]
            # if degree is smaller and vm_id not already in this benchmark
            if (degree < min_degree and not (tmp_vm_list[tmp_vm_index].node_id in vm_ids)):
              min_degree_index = tmp_vm_index
            tmp_vm_index += 1

          if suitable_vm_found:
            vms.append(tmp_vm_list[min_degree_index])
            vm_ids.append(tmp_vm_list[min_degree_index].node_id)
            continue

      # if vm does not exist yet
      elif (not suitable_vm_found):
        # try to add vm to region
        print("here2")
        #vm_region = "us-east-2"
        print(f"\n\nSelf: {self.__dict__} \n Type: {self}\n\n")
        print(f"\n\nvm region is: {vm_region}\n\n")
        print(f"\n\nself.regions is: {self.regions} : the type of (regions) is: {type(self.regions)}\n\n region: ")

       #print(f"\n\nself.regions is: {self.regions} : the type of (regions) is: {type(self.regions)}\n\n region: {self.regions[vm_region]}")
       
        # myRegion = Region(vm_region, "AWS")
        # myRegion.name = vm_region
        # self.add_region_if_not_exists(myRegion) # So the reason the code is breaking later because I add the region here myself, and it never gets populated
        print(f"\n\nSelf: {self.__dict__} \n Type: {self}\n\n")
        #print(f"\n\nself.regions: {self.regions['us-east-2'].__dict__}\n")
        #the self here doesn't have any vm_cpu's allocated to it ###################################################################################
        print(f" vm is: {vm.__dict__}")
        status = self.regions[vm_region].add_virtual_machine_if_possible(vm)
        print("Status ", status)
        print(f"\n\nStatus: {status}\n\n")

        # if successful, also add that vm to virtual_machines list
        # and increment total number of vms, return True, and the vm
        if status is True:
          print("adding vm in zone " + vm.zone)
          self.virtual_machines.append(vm)
          self.graph.add_node(vm_id, vm=vm)
          vms.append(vm)
          vm_ids.append(vm.node_id)
          self.vm_total_count += 1
        # return false, None if not enough space in region
        else:
          logger.debug("QUOTA EXCEEDED. VM waitlisted")
          vms.append(None)

    print(vms)
    return vms

  def add_same_zone_vms(self, vm1, vm2):
    vm1_list = get_list_if_vm_exists(vm1)
    pass

  def get_list_of_nodes(self):
    return self.graph.nodes

  def get_list_of_edges(self):
    return self.graph.edges

  def add_benchmark_as_edge(self, new_benchmark, node1, node2):
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

    logger.info("LENGTH NODE LIST: " + str(len(node_list)))

    # for each node in the graph
    while node_index < len(node_list):
      vm_processes = []
      thread_count = 0
      while ((thread_count < max_processes or max_processes < 0) and
             node_index < len(node_list)):
        # for index in node_list:

        # print("NODE INDEX", str(node_index))
        # print(self.graph.nodes)
        # print(node_list)
        vm = self.graph.nodes[node_list[node_index]]['vm']

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
        # print("VM PROCESS INFO STUFF HERE:")
        # print("IN PROCESS VM:")
        # print(container['vm'].creation_output)
        # print("OUT PROCESS VM:")
        # print(new_vm.creation_output)
        p.join()
        print("Process Done")

    for index in created_nodes:
      vm = self.graph.nodes[node_list[index]]['vm']
      self.vm_creation_times.append(vm.creation_time)
      logging.debug("VM INDEX: " + str(index))
      logging.debug(vm.status)
      logging.debug(vm.run_uri)
      logging.debug(vm.creation_output)

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
      bm_config_file = None

      if FLAGS.precreate_and_share_vms:
        bm_config_file = self.create_benchmark_config_file_static_vm(bm_to_run, vm_list)
      else:
        bm_config_file = self.create_benchmark_config_file_traditional(bm_to_run, vm_list)
      # add config file to benchmark object
      bm_to_run.config_file = bm_config_file
      benchmarks_to_run.append(bm_to_run)
      benchmarks_to_run_tuples.append(bm_tuple)

    # run benchmark configs
    # run in parallel

    bm_all_thread_results = []
    # bm_thread_result_counter = 0
    bm_index = 0

    max_processes = FLAGS.max_processes

    # run benchmarks
    while bm_index < len(benchmarks_to_run):
      bm_threads = []

      thread_count = 0
      while ((thread_count < max_processes or max_processes < 0) and
              bm_index < len(benchmarks_to_run)):
        # TODO change this into a dict?
        bm = benchmarks_to_run[bm_index]

        # make sure that all vms for benchmark have been created
        # TODO try to ssh into it to make sure it is up
        #      or at least ping it
        vms_created = True

        if FLAGS.precreate_and_share_vms:
          for vm in bm.vms:
            if vm.status != "Running":
              print("Needed VM is ", vm.status)
              vms_created = False

          if not vms_created:
            print("DO NOT RUN")
            bm_index += 1
            continue
        else:
          for vm in bm.vms:
            vm.create_timestamp = time.time()

        # create
        queue = mp.Queue()
        logger.debug(bm.vm_specs[0].zone + " <-> " + bm.vm_specs[1].zone)
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
        # TODO make sure this works
        if not FLAGS.precreate_and_share_vms:
          for vm in bm_data['bm'].vms:
            vm.deletion_timestamp = time.time()
        bm_data['success'] = results_dict['success']
        logging.debug("thread done")

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

    cmd = (self.pkb_location +
           " --benchmarks=" + bm.benchmark_type +
           " --gce_network_name=pkb-scheduler" +
           " --benchmark_config_file=" + bm.config_file +
           " --bigquery_table=" + bm.bigquery_table +
           " --bq_project=" + bm.bq_project +
           " --ignore_package_requirements=True")

    # TODO figure out what to do if only one vm is windows
    if 'windows' in bm.vm_specs[0].os_type:
      cmd = (cmd + " --os_type=" + bm.vm_specs[0].os_type +
                   " --skip_package_cleanup=True")

    #TODO fix this for intercloud
    if not FLAGS.precreate_and_share_vms:
      if bm.vm_specs[0].cloud == 'GCP':
        cmd = (cmd + " --gce_remote_access_firewall_rule=allow-ssh"
                   + " --skip_firewall_rules=True"
                   + " --gcp_min_cpu_platform=" + bm.vm_specs[0].min_cpu_platform)

    # TODO do install_packages if vm has already been used

    print(bm_tuple)
    print(bm.vm_specs[0].zone)
    print(bm.vm_specs[1].zone)
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


  def create_benchmark_config_file_static_vm(self, bm, vm_list):

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

    # TODO fix these
    config_flags["static_cloud_metadata"] = bm.vm_specs[0].cloud
    config_flags["static_network_tier_metadata"] = bm.vm_specs[0].network_tier


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

      if 'windows' in vm.os_type:
        vm_config_dict['os_type'] = vm.os_type
        vm_config_dict['password'] = vm.password

      temp[vm_num]['static_vms'].append(vm_config_dict)

      counter += 1

    file_name = (self.generated_config_path + "config_" 
                 + str(bm.benchmark_id) + ".yaml")
    file = open(file_name, 'w+')
    yaml.dump(config_yaml, file, default_flow_style=False)
    file.close()

    return file_name

  def create_benchmark_config_file_traditional(self, bm, vm_list):

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

    # TODO fix these
    # config_flags["static_cloud_metadata"] = bm.vm_specs[0].cloud
    # config_flags["static_network_tier_metadata"] = bm.vm_specs[0].network_tier


    for vm in vm_list:
      temp = config_yaml[bm.benchmark_type]['vm_groups']
      vm_num = 'vm_' + str(counter)
      temp[vm_num] = {}
      # temp[vm_num]['static_vms'] = []
      temp[vm_num]['cloud'] = vm.cloud

      if 'windows' in vm.os_type:
        temp[vm_num]['os_type'] = vm.os_type
        # vm_config_dict['password'] = vm.password

      temp[vm_num]['vm_spec'] = {}
      temp[vm_num]['vm_spec'][vm.cloud] = {}
      vm_config_dict = {}
      # vm_config_dict['user_name'] = 'perfkit'
      # vm_config_dict['ssh_private_key'] = vm.ssh_private_key
      # vm_config_dict['ip_address'] = vm.ip_address
      # vm_config_dict['internal_ip'] = vm.internal_ip
      vm_config_dict['install_packages'] = True
      vm_config_dict['zone'] = vm.zone
      vm_config_dict['machine_type'] = vm.machine_type

      temp[vm_num]['vm_spec'][vm.cloud] = vm_config_dict

      counter += 1

    file_name = (self.generated_config_path + "config_" 
                 + str(bm.benchmark_id) + ".yaml")
    file = open(file_name, 'w+')
    yaml.dump(config_yaml, file, default_flow_style=False)
    file.close()

    return file_name

  def remove_benchmark(self):
    pass

  # TODO improve this
  def add_benchmarks_from_waitlist(self):

    if len(self.benchmark_wait_list) == 0:
      logging.info("No benchmarks on waitlist")
      return

    logging.info("Adding benchmarks from waitlist")

    bms_added = []
    for bm in self.benchmark_wait_list:
      print("here4, ", str(len(self.benchmark_wait_list)))
      vms = self.add_vms_for_benchmark_if_possible(bm)
      vms_no_none = list(filter(None, vms))

      if len(bm.vm_specs) == len(vms_no_none) == len(vms):
        bm.vms = vms
        self.benchmarks.append(bm)
        self.add_benchmark_as_edge(bm, vms[0].node_id, vms[1].node_id)
        bms_added.append(bm)

    # Remove bm from waitlist if it is added to graph

    logging.info("ADDED " + str(len(bms_added)) + " BENCHMARKS")
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

    if FLAGS.precreate_and_share_vms:
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
        logging.debug("Thread Done")

    else:
      # start threads to remove vms
      for key in node_degree_dict.keys():
        # if a node has no edges
        if node_degree_dict[key] == 0:
          keys_to_remove.append(key)

    for key in keys_to_remove:
      vm = self.graph.nodes[key]['vm']
      logging.debug("VM removed: " + str(key))
      self.graph.remove_node(key)
      vm_region = cloud_util.get_region_from_zone(vm.cloud, vm.zone)
      print(f"\n\nself.regions: {self.regions}\n\n")
      print(f"vm_region: {vm_region}")
      self.regions[vm_region].remove_virtual_machine(vm)
      vm_removed_count += 1

    return vm_removed_count

  def benchmarks_left(self):
    # TODO fix this
    return len(self.graph.edges) + len(self.benchmark_wait_list)
