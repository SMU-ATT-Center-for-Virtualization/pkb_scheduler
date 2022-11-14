from __future__ import annotations
import networkx as nx
import matplotlib.pyplot as plt
import threading
import multiprocessing as mp
import time
import os
import subprocess
import yaml
import threading
import logging
import math
import cloud_util
import time

from queue import Queue
from deprecated import deprecated
from typing import List, Dict, Tuple, Set, Any, Optional
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
    self.multiedge_benchmarks = []
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


  def add_cloud_if_not_exists(self, cloud: str):
    """Add a new cloud provider to the benchmark graph
    
    Args:
        cloud (str): name of cloud provider
    """
    if cloud.name not in self.clouds:
      self.clouds[cloud.name] = cloud

  def cloud_exists(self, cloud: str) -> bool:
    """Return if cloud currently exists benchmark graph
    
    Args:
        cloud (str): name of cloud provider
    
    Returns:
        bool: True if cloud exists in benchmark graph, false otherwise
    """
    return cloud in self.clouds

  def add_region_if_not_exists(self, new_region: Region):
    """Add a new region to the benchmark graph
    
    Args:
        new_region (Region): Add a new Region object
    """
    if new_region.name not in self.regions:
      self.regions[new_region.name] = new_region

  def region_exists(self, region_name: str) -> bool:
    """Return whether region currently exists benchmark graph
    
    Args:
        region_name (str): name of cloud region
    
    Returns:
        bool: True if region exists in benchmark graph, false otherwise
    """
    return region_name in self.regions

  def required_vm_exists(self, vm: VirtualMachine) -> bool:
    """Checks if there is a VM with equivalent specs/location in the graph
    
    Args:
        vm (VirtualMachine): VirtualMachine object 
    
    Returns:
        bool: if there is an equivalent VM
    """
    for index in self.graph.nodes:
      tmp_vm = self.graph.nodes[index]['vm']
      if vm.vm_spec_is_equivalent(tmp_vm):
        return True
      else:
        continue
    return False

  def print_graph(self):
    """Uses matplotlib to show a visual representation of the graph in its current state
    """
    nx.draw(self.graph)
    plt.show()

  def get_list_if_vm_exists(self, vm: VirtualMachine) -> list[VirtualMachine]:
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

  def get_node_id_list_if_vm_exists(self, vm: VirtualMachine) -> list[int]:
    """Tries to find a VM in the graph with equivalent specs

    Searches graph nodes and returns a list of VMs
    with identical specs to the argument vm

    Args:
      vm: vm with specs to search for

    Returns:
      list of node_ids with VMs with matching specs to parameter
      list[int]
    """
    node_list = []
    vm_list = self.get_list_if_vm_exists(vm)
    for vm in vm_list:
      node_list.append(vm.node_id)
    return node_list

  def check_if_should_add_vm(self, vm_list: list[VirtualMachine]) -> bool:
    """Checks if the degree of the vms on the list is less than the max of the
       whoel network

    Args:
        vm_list (list[VirtualMachine]): [description]

    Returns:
        bool: True if we should add a new VM, false is we should try to use existing
    """
    max_benchmarks = max(dict(self.graph.degree).values())
    max_degree = max_benchmarks
    for vm in vm_list:
      benchmarks_for_this_vm = self.graph.degree(vm.node_id)
      if benchmarks_for_this_vm > max_degree:
        max_degree = benchmarks_for_this_vm
    logger.info(f"MAX DEGREE {max_benchmarks}, MIN DEG FOR THIS BENCHMARK: {max_degree}")
    if benchmarks_for_this_vm < max_benchmarks:
      return True
    return False

  def check_if_can_add_vm(self, vm: VirtualMachine):
    """Checks if there is enough space in the region to add VM

    Checks region quotas to see if it can add VM

    Args:
      vm: Virtual Machine to add

    Returns:
      Boolean and description of if it can/cannot add and why
      bool, String
    """
    vm_region = cloud_util.get_region_from_zone(vm.cloud, vm.zone)

    #TODO change implementation to just take vm.machine_type
    if self.regions[vm_region].has_enough_resources(vm.cpu_count, vm.machine_type, vm.estimated_bandwidth):
      if self.required_vm_exists(vm):
        # returns this is vm exists but there is enough space
        # for another
        return True, "VM Exists. Quota not Exceeded"
      else:
        # returns True if the vm doesn't already exist
        # and if region has enough space
        return True, "VM does not exist"

    return False, "Quota Exceeded"

  def add_or_waitlist_benchmark_and_vms(self, bm: Benchmark) -> Tuple[List[VirtualMachine], str]:
    vms = self.add_vms_for_benchmark_if_possible(bm)

    # Filter None values from list
    vms_no_none = list(filter(None, vms))

    # If it was able to add all vms from benchmark, add bm as edge. Else -> waitlist
    if len(bm.vm_specs) == len(vms_no_none) == len(vms):
      self.add_benchmark_to_graph(bm, vms)
      return bm.vms, "Added"
    else:
      logger.debug("BM WAITLISTED")
      bm.status = "Waitlist"
      self.benchmark_wait_list.append(bm)
      return [], "Waitlisted"

  def add_vms_for_benchmark_if_possible(self, bm: Benchmark) -> List[VirtualMachine]:
    """Summary
    
    Args:
        bm (Benchmark): Description
    
    Returns:
        List[VirtualMachine]: Description
    """
    vm_ids = []
    vms = []


    for vm_spec in bm.vm_specs:

      vm_region = cloud_util.get_region_from_zone(vm_spec.cloud, vm_spec.zone)
      vm_id = self.vm_total_count
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
                          network_name=vm_spec.network_name,
                          subnet_name=vm_spec.subnet_name,
                          preexisting_network=vm_spec.preexisting_network,
                          estimated_bandwidth=vm_spec.estimated_bandwidth)

      # if VM with same specs already exists, return false 0
      tmp_vm_list = self.get_list_if_vm_exists(vm)

      suitable_vm_found = False

      # if a vm already exists
      if len(tmp_vm_list) > 0:
        logger.debug(f"MATCHING VM FOR {vm} EXISTS")
        can_add_another, status = self.check_if_can_add_vm(vm)

        add_from_list = True

        # if there is room to add a duplicate vm and if flags allow it 
        # then add the VM
        if (can_add_another 
            and status == "VM Exists. Quota not Exceeded"
            and FLAGS.allow_duplicate_vms == True
            # and self.check_if_should_add_vm(tmp_vm_list)
            and len(tmp_vm_list) < FLAGS.max_duplicate_vms + 1):
          # checks if there is enough space in a region to add another vm
          success = self.regions[vm_region].add_virtual_machine_if_possible(vm)
          if success:
            logger.debug(f"ADD DUPLICATE VM {vm}")
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
        if add_from_list:
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
            logger.debug(f"USING EXISTING VM {tmp_vm_list[min_degree_index]}FOR BENCMARK")
            vms.append(tmp_vm_list[min_degree_index])
            vm_ids.append(tmp_vm_list[min_degree_index].node_id)
            continue

      # if vm does not exist yet
      if (not suitable_vm_found):
        # try to add vm to region
        logger.debug("NO SUITABLE EXISTING VM FOUND")
        status = self.regions[vm_region].add_virtual_machine_if_possible(vm)

        # if successful, also add that vm to virtual_machines list
        # and increment total number of vms, return True, and the vm
        if status is True:
          logger.debug("NO SUITABLE VM FOUND, CREATING NEW VM")
          self.virtual_machines.append(vm)
          self.graph.add_node(vm_id, vm=vm)
          vms.append(vm)
          vm_ids.append(vm.node_id)
          self.vm_total_count += 1
        else:
          logger.debug("QUOTA EXCEEDED. VM waitlisted")
          vms.append(None)
    return vms

  def equalize_graph(self):
    logger.debug("RUN EQUALIZE GRAPH")
    PERCENTAGE_TO_EQUALIZE = 1.0
    highest_degree_node_list = self.get_list_of_nodes_by_highest_degree()
    number_of_nodes_to_equalize = int(len(highest_degree_node_list)*PERCENTAGE_TO_EQUALIZE)
    for i in range(0, number_of_nodes_to_equalize):
      max_node_id, max_node_degree = highest_degree_node_list[i]
      max_node_degree = self.graph.degree(max_node_id)
      if max_node_degree <= 1:
        continue
      logger.debug(f'EQUALIZING NODE {max_node_id} with degree {max_node_degree}')
      if max_node_id == None:
        continue
      max_node_adjacency_list = list(self.graph[max_node_id])
      if len(max_node_adjacency_list) == 0:
        continue
      max_node_adjacency_degree_list = sorted(list(self.graph.degree(max_node_adjacency_list)),
                                              key=lambda x: x[1], reverse=False)
      # check if an equivalent VM exists with a lower degree
      max_node_vm = self.graph.nodes[max_node_id]['vm']
      equivalent_max_node_list = self.get_node_id_list_if_vm_exists(max_node_vm)
      logger.debug(f'EQUIVALENT NODE LIST: {equivalent_max_node_list}')
      # get and sort list of equivalent vms
      number_changed = 0
      equality_improved = True
      while equality_improved:
        equality_improved = False
        equivalent_max_node_degree_list = list(self.graph.degree(equivalent_max_node_list))
        equivalent_max_node_degree_list = sorted(equivalent_max_node_degree_list,
                                                key=lambda x: x[1], reverse=False)

        for new_node_id, new_node_degree in equivalent_max_node_degree_list:
          # if degree of equivalent node < degree of max node, transfer over a benchmark
          if new_node_degree < (max_node_degree - 1):
            # pick a node adjacent to max_degree_node to transfer to equivalent node
            new_node_vm = self.graph.nodes[new_node_id]['vm']
            bm_to_change = None
            key_to_remove = None
            node_to_transfer = None
            node_to_tranfer_degree = None
            for node, node_degree in max_node_adjacency_degree_list:
              incident_edges = self.graph.get_edge_data(max_node_id, node)
              logger.debug(f'incident edges {incident_edges}')
              for key in incident_edges:
                if incident_edges[key]['bm'].status == "Not Executed":
                  bm_to_change = incident_edges[key]['bm']
                  key_to_remove = key
                  node_to_transfer = node
                  node_to_tranfer_degree = node_degree
                  break
                else:
                  logger.debug(incident_edges[key]['bm'].status)
              if node_to_transfer:
                break

            if bm_to_change != None:
              logger.debug(f'Moving bm from nodes {max_node_id} with degree {max_node_degree} to node {new_node_id} with degree {new_node_degree}')
              equality_improved = True
              # change bm.vms[]
              bm_to_change.vms.remove(max_node_vm)
              bm_to_change.vms.append(new_node_vm)
              # remove old edge
              self.graph.remove_edge(max_node_id, node_to_transfer, key_to_remove)
              # add new edge
              self.graph.add_edges_from([(new_node_id, node_to_transfer, {'bm': bm_to_change})])
              max_node_degree = self.graph.degree(max_node_id)
              number_changed += 1
              max_node_adjacency_list = list(self.graph[max_node_id])
              max_node_adjacency_degree_list = sorted(list(self.graph.degree(max_node_adjacency_list)),
                                                      key=lambda x: x[1], reverse=False)
              # answer = input("...").lower()


      if max_node_degree > 1:
        vm_region = cloud_util.get_region_from_zone(max_node_vm.cloud, max_node_vm.zone)
        new_vm_id = self.vm_total_count
        new_vm = VirtualMachine(node_id=new_vm_id,
                                cpu_count=max_node_vm.cpu_count,
                                zone=max_node_vm.zone,
                                os_type=max_node_vm.os_type,
                                network_tier=max_node_vm.network_tier,
                                machine_type=max_node_vm.machine_type,
                                cloud=max_node_vm.cloud,
                                min_cpu_platform=max_node_vm.min_cpu_platform,
                                ssh_private_key=self.ssh_private_key_file,
                                ssl_cert=self.ssl_cert_file,
                                vm_spec=max_node_vm.vm_spec,
                                vm_spec_id=max_node_vm.vm_spec_id,
                                network_name=max_node_vm.network_name,
                                subnet_name=max_node_vm.subnet_name,
                                preexisting_network=max_node_vm.preexisting_network)
        # if VM with same specs already exists, return false 0
        tmp_vm_list = self.get_list_if_vm_exists(new_vm)
        can_add_another, status = self.check_if_can_add_vm(new_vm)
        # if there is room to add a duplicate vm and if flags allow it
        # then add the VM
        if (can_add_another
            and status == "VM Exists. Quota not Exceeded"
            and FLAGS.allow_duplicate_vms == True
            # and self.check_if_should_add_vm(tmp_vm_list)
            and len(tmp_vm_list) < FLAGS.max_duplicate_vms + 1):
          # checks if there is enough space in a region to add another vm
          success = self.regions[vm_region].add_virtual_machine_if_possible(new_vm)
          if success:
            logger.debug("DUPLICATE VM ADDED")
            self.virtual_machines.append(new_vm)
            self.graph.add_node(new_vm_id, vm=new_vm)
            self.vm_total_count += 1
            bm_to_change = None
            key_to_remove = None
            node_to_transfer = None
            node_to_tranfer_degree = None
            for node, node_degree in max_node_adjacency_degree_list:
              incident_edges = self.graph.get_edge_data(max_node_id, node)
              logger.debug(f'incident edges {incident_edges}')
              for key in incident_edges:
                if incident_edges[key]['bm'].status == "Not Executed":
                  bm_to_change = incident_edges[key]['bm']
                  key_to_remove = key
                  node_to_transfer = node
                  node_to_tranfer_degree = node_degree
                  break
              if node_to_transfer:
                break
            # logger.debug(f'node to transfer: {node_to_transfer}')
            # logger.debug(f'edge to change {key}')
            # logger.debug(f'bm to change {bm_to_change}')
            if bm_to_change != None:
              # change bm.vms[]
              bm_to_change.vms.remove(max_node_vm)
              bm_to_change.vms.append(new_vm)
              # remove old edge
              self.graph.remove_edge(max_node_id, node_to_transfer, key_to_remove)
              # add new edge
              self.graph.add_edges_from([(new_vm_id, node_to_transfer, {'bm': bm_to_change})])
              max_node_degree = self.graph.degree(max_node_id)
              number_changed += 1
              max_node_adjacency_list = list(self.graph[max_node_id])
              logger.debug(f'new max node adjacency list {max_node_adjacency_list}')
              logger.debug(f'new other node adjacency list {list(self.graph[new_node_id])}')
              max_node_adjacency_degree_list = sorted(list(self.graph.degree(max_node_adjacency_list)),
                                                      key=lambda x: x[1], reverse=False)
              logger.debug("MOVED BM TO NEW VM")

            # answer = input("...").lower()

          else:
            logger.debug("QUOTA EXCEEDED. CANNOT ADD ANOTHER VM")
      logger.debug(f"NUMBER CHANGED {number_changed}")
    # answer = input("...").lower()

  def get_list_of_nodes_by_highest_degree(self) -> List[Tuple[int, int]]:
    """Returns list of nodes with highest degree and its degree in a tuple

    Returns:
        list[tuple[int, int]]: node_id, degree
    """
    # get list of tuples (node_id, degree) sorted in descending order
    degree_list = sorted(list(self.graph.degree), key=lambda x: x[1], reverse=True)
    return degree_list

  def get_node_with_highest_degree(self) -> Tuple[int, int]:
    """Returns node with highest degree and its degree in a tuple

    Returns:
        tuple[int, int]: node_id, degree
    """
    # get list of tuples (node_id, degree) sorted in descending order
    degree_list = sorted(list(self.graph.degree), key=lambda x: x[1], reverse=True)
    if len(degree_list) == 0:
      return (None,None)
    return degree_list[0]

  def redistribute_edges_for_node(self, node_id: int):
    pass

  def add_same_zone_vms(self, vm1, vm2):
    vm1_list = self.get_list_if_vm_exists(vm1)
    pass

  def get_list_of_nodes(self):
    return self.graph.nodes

  def get_list_of_edges(self):
    return self.graph.edges

  def add_benchmark_to_graph(self, bm: Benchmark, vms: List[VirtualMachine]):
    #  M[v1][v2]
    # M.add_edges_from([(v1,v2,{'route':45645})])
    bm.vms = vms
    self.benchmarks.append(bm)
    if len(bm.vms) == 1:
      self.graph.add_edges_from([(vms[0].node_id, vms[0].node_id, {'bm': bm})])
    elif len(bm.vms) == 2:
      self.graph.add_edges_from([(vms[0].node_id, vms[1].node_id, {'bm': bm})])
    else:
      node_ids = []
      for i in range(0, len(bm.vms)):
        node_ids.append(bm.vms[i].node_id)
        if i < len(bm.vms) - 1:
          self.graph.add_edges_from([(vms[i].node_id, vms[i+1].node_id, {'bm': bm})])
        elif i == len(bm.vms) - 1:
          self.graph.add_edges_from([(vms[i].node_id, vms[0].node_id, {'bm': bm})])
      self.multiedge_benchmarks.append((bm,node_ids))

    bm.benchmark_id = self.bm_total_count
    self.bm_total_count += 1

  def maximum_matching(self) -> List[Tuple[int,int]]:
    return nx.max_weight_matching(self.graph, maxcardinality=True)

  def get_benchmark_set(self) -> List[Tuple[int,int]]:
    # return bm_list: List[Tuple[int, int]]   list of tuples  [(node1, node2)]

    logger.debug("GET BENCHMARK SET")

    # convert multigraph to simplified graph with weighted edges
    for i in self.graph.nodes:
      logger.debug(self.graph.nodes[i]['vm'].status)
    # print(self.graph.edges)
    node_degree_dict = dict(nx.degree(self.graph))
    logger.debug(node_degree_dict)
    tmp_graph = nx.Graph()
    tmp_graph.add_nodes_from(self.graph.nodes)
    edges_list = []
    for e in self.graph.edges:
      edges_list.append(e[0:2])

    edges_set = set(edges_list)
    logger.debug("EDGE SET")
    logger.debug(edges_set)
    edges_to_add = []
    dummy_node_index = -1
    dummy_nodes = []
    dummy_edges = []

    # calculate weight based on degree of each node 1/(n1.degree * n2.degree)
    # also take into account if vms in benchmark are already created
    for e in edges_set:
      # if self-loop, replace with dummy node and edge
      if e[0] == e[1]:
        dummy_node = dummy_node_index
        c = 10 / (node_degree_dict[e[0]])
        if self.graph.nodes[e[0]]['vm'].status == 'Running':
          c = c + 10
        t = (e[0], dummy_node, c)
        edges_to_add.append(t)
        dummy_nodes.append(dummy_node)
        dummy_edges.append((e[0], dummy_node))
        dummy_node_index -= 1
      else:
        c = 10 / (node_degree_dict[e[0]] * node_degree_dict[e[1]])
        if self.graph.nodes[e[0]]['vm'].status == 'Running':
          c = c + 10
        if self.graph.nodes[e[1]]['vm'].status == 'Running':
          c = c + 10
        t = (e[0], e[1], c)
        edges_to_add.append(t)

    tmp_graph.add_weighted_edges_from(edges_to_add)
    logger.debug(tmp_graph)
    logger.debug("NODES:")
    logger.debug(tmp_graph.nodes)
    logger.debug("EDGES:")
    logger.debug(tmp_graph.edges)
    for e in tmp_graph.edges:
      logger.debug(tmp_graph.edges[e])

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

    maximum_match = list(nx.max_weight_matching(tmp_graph, maxcardinality=True, weight='weight'))

    # convert dummy nodes back to self-loops
    for i in range(0, len(maximum_match)):
      if maximum_match[i][0] < 0:
        maximum_match[i] = (maximum_match[i][1], maximum_match[i][1])
      elif maximum_match[i][1] < 0:
        maximum_match[i] = (maximum_match[i][0], maximum_match[i][0])
    return maximum_match

  def create_vms(self, vm_list: List[int] = []) -> List[int]:
    """Create Virtual Machines that have not yet been created

    Args:
        vm_list (list[int], optional): List of VMs to create if not already created. Defaults to [].

    Returns:
        list[int]: List of VM IDs that were created
    """
    # TODO add max thread logic here, make sure things are stood up in a reasonable way
    # go through nodes in network. Stand up Vms that have not been created
    max_processes = FLAGS.max_processes

    node_list = vm_list
    if len(node_list) == 0:
      node_list = list(self.graph.nodes)
    node_index = 0
    created_nodes = []

    logger.info("LENGTH NODE LIST: " + str(len(node_list)))
    logger.debug("NODE LIST")
    logger.debug(node_list)
    # for each node in the graph
    while node_index < len(node_list):
      vm_processes = []
      thread_count = 0
      while ((thread_count < max_processes or max_processes < 0) and
             node_index < len(node_list)):
        # for index in node_list:
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
        p.join()
        print("Process Done")

    for index in created_nodes:
      vm = self.graph.nodes[node_list[index]]['vm']
      self.vm_creation_times.append(vm.creation_time)
      logging.debug("VM INDEX: " + str(index))
      logging.debug(vm.status)
      logging.debug(vm.run_uri)
      logging.debug(vm.creation_output)

    return created_nodes

  def create_vm_process(self, vm: VirtualMachine, queue: mp.Queue):
    vm.create_instance(self.pkb_location)
    queue.put(vm)

  def create_vm(self, vm: VirtualMachine):
    vm.create_instance(self.pkb_location)

  def run_benchmark_set(self, bm_list: list[tuple[int, int]]):
    """When given a list of tuples, where each element
       in the tuple is a node id, this function figures
       out the benchmark to run between those nodes.
       It then calls a function to create a config file for
       the benchmarks and then runs the benchmarks

    Args:
        bm_list (list[tuple[int, int]]): list of node_id tuples (node_id, node_id)
    """
    benchmarks_to_run = []
    benchmarks_to_run_tuples = []
    logger.debug("RUN BENCHMARKS")

    # Go through list of benchmarks to run and see what benchmarks are in each list
    # try to run benchmark with most occurences
    benchmark_count_dict = {}
    for node_tuple in bm_list:
      bm_dict = dict(self.graph[node_tuple[0]][node_tuple[1]])
      # get list of keys from dict
      bm_key_list = list(bm_dict.keys())
      for bm_key in bm_key_list:
        benchmark_type = self.graph[node_tuple[0]][node_tuple[1]][bm_key]['bm'].benchmark_type
        if benchmark_type in benchmark_count_dict:
          benchmark_count_dict[benchmark_type] += 1
        else:
          benchmark_count_dict[benchmark_type] = 1

    highest_count_bm = ''
    highest_count = 0
    for key in benchmark_count_dict:
      if benchmark_count_dict[key] > highest_count:
        highest_count_bm = key
        highest_count = benchmark_count_dict[key]
    
    # create benchmark configs for each benchmark in set
    # bm_list is a list of tuples [(n1,n2), (n3,n4)]
    for node_tuple in bm_list:
      # node_tuple is a tuple (node1, node2) of nodes that have
      # an edge between them

      vm_list = []
      vm_list.append(self.graph.nodes[node_tuple[0]]['vm'])
      vm_list.append(self.graph.nodes[node_tuple[1]]['vm'])
      # print(f'benchmarks between nodes ({node_tuple[0]},{node_tuple[1]}): {self.graph[node_tuple[0]][node_tuple[1]]}')

      # get dict of benchmarks from vm 0 to vm 1
      bm_dict = dict(self.graph[node_tuple[0]][node_tuple[1]])
      # get list of keys from dict
      bm_key_list = list(bm_dict.keys())

      # TODO have some optimization here so benchmarks take similar times?
      bm_chosen_key = bm_key_list[len(bm_key_list) - 1]
      for key in bm_key_list:
        if bm_dict[key]['bm'].benchmark_type == highest_count_bm:
          bm_chosen_key = key
          break
      bm_index_to_run = bm_chosen_key
      # edge tuple (node1, node2, key)
      bm_tuple = (node_tuple[0], node_tuple[1], bm_index_to_run)
      # get actual Benchmark object from edge in graph
      bm_to_run = self.graph[node_tuple[0]][node_tuple[1]][bm_index_to_run]['bm']
      logger.debug(f'BM TO RUN TYPE: {bm_to_run.benchmark_type}')
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

    # run benchmarks threaded
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
              logger.debug("Needed VM is ", vm.status)
              vms_created = False

          if not vms_created:
            logger.debug("DO NOT RUN")
            bm_index += 1
            continue
        else:
          for vm in bm.vms:
            vm.create_timestamp = time.time()

        # create
        queue = mp.Queue()
        logger.debug(bm)
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

    logging.info("All threads done")

    # TODO remove only successful benchmarks from graph
    for bm_data in bm_all_thread_results:
      bm = bm_data['bm']
      bm_loc = bm_data['tuple']
      success = bm_data['success']
      if success:
        self.graph.remove_edge(bm_loc[0], bm_loc[1], bm_loc[2])
        logging.debug("benchmark removed: " + str(bm_loc))
      else:
        pass


  def run_benchmark_process(self,
                            bm: Benchmark,
                            bm_tuple: Tuple[int,int,int],
                            result_index: int,
                            queue: mp.Queue):
    # ./pkb.py --benchmarks=throughput_latency_jitter
    # --benchmark_config_file=static_config2.yaml

    config_file_path = self.generated_config_path + bm.config_file

    results_dict = {}
    results_dict['bm_tuple'] = bm_tuple
    results_dict['result_index'] = result_index
    results_dict['bm'] = bm
    results_dict['success'] = False
    results_dict['run_time'] = None

    bm_clouds = []
    all_vms_have_preexisting_network = True
    for vm in bm.vms:
      logger.debug(vm.__dict__)
      bm_clouds.append(vm.cloud)
      if vm.preexisting_network == False:
        all_vms_have_preexisting_network = False

    cmd = (self.pkb_location +
           " --benchmarks=" + bm.benchmark_type +
           " --benchmark_config_file=" + bm.config_file +
           " --bigquery_table=" + bm.bigquery_table +
           " --bq_project=" + bm.bq_project +
           " --ignore_package_requirements=True")

    # TODO add logic for different networks in GCP
    # TODO add logic for existing network in AWS
    # or perhaps that should be in the config file area
    if 'GCP' in bm_clouds:
      # Maybe this should be in the config file area
      # cmd = (cmd + " --gce_network_name=pkb-scheduler")
      pass
    if 'AWS' in bm_clouds:
      pass
    if 'Azure' in bm_clouds:
      pass

    # TODO figure out what to do if only one vm is windows
    if 'windows' in bm.vm_specs[0].os_type:
      cmd = (cmd + " --os_type=" + bm.vm_specs[0].os_type +
                   " --skip_package_cleanup=True")

    if all_vms_have_preexisting_network:
      cmd = cmd + " --skip_firewall_rules=True"

    if not FLAGS.precreate_and_share_vms:
      # TODO move both of these to individual config files
      cmd = (cmd + " --gce_remote_access_firewall_rule=allow-ssh")
      if bm.vm_specs[0].min_cpu_platform:
        cmd = (cmd + " --gcp_min_cpu_platform=" + bm.vm_specs[0].min_cpu_platform)
    if FLAGS.skip_prepare:
      cmd = (cmd + " --skip_prepare=True")

    cmd = (cmd + f" --log_level={FLAGS.pkb_log_level}")

    # TODO do install_packages if vm has already been used
    logger.debug("BM TUPLE")
    logger.debug(bm_tuple)
    # print(bm.vm_specs[0].zone)
    # print(bm.vm_specs[1].zone)
    logger.debug(bm.config_file)
    logger.debug("RUN BM: " + cmd)
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


  def create_benchmark_config_file_static_vm(self,
                                             bm: Benchmark,
                                             vm_list: List[VirtualMachine]):

    config_yaml = {}
    counter = 1

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

  def create_benchmark_config_file_traditional(self,
                                               bm: Benchmark,
                                               vm_list: List[VirtualMachine]) -> str:

    config_yaml = {}
    counter = 1

    config_yaml[bm.benchmark_type] = {}
    if bm.vpc_peering:
      config_yaml[bm.benchmark_type]['vpc_peering'] = bm.vpc_peering
    config_yaml[bm.benchmark_type]['vm_groups'] = {}
    config_yaml[bm.benchmark_type]['flags'] = bm.flags
    config_flags = config_yaml[bm.benchmark_type]['flags']

    config_flags.pop("zones", None)
    config_flags.pop("extra_zones", None)
    config_flags.pop("machine_type", None)

    # TODO fix these
    # config_flags["static_cloud_metadata"] = bm.vm_specs[0].cloud
    # config_flags["static_network_tier_metadata"] = bm.vm_specs[0].network_tier

    if bm.benchmark_type in ['omb', 'mpi']:
      # default:
      #   vm_count: 2
      #   vm_spec: *default_single_core
      logger.debug("VM TO WRITE TO CONFIG")
      logger.debug(vm_list[0].__dict__)
      temp = config_yaml[bm.benchmark_type]['vm_groups']
      vm_num = len(vm_list)

      temp['default'] = {}
      temp['default']['vm_count'] = vm_num
      temp['default']['cloud'] = vm_list[0].cloud
      temp['default']['vm_spec'] = {}
      temp['default']['vm_spec'][vm_list[0].cloud] = {}
      vm_config_dict = {}
      # vm_config_dict['user_name'] = 'perfkit'
      # vm_config_dict['ssh_private_key'] = vm.ssh_private_key
      # vm_config_dict['ip_address'] = vm.ip_address
      # vm_config_dict['internal_ip'] = vm.internal_ip
      vm_config_dict['install_packages'] = True
      vm_config_dict['zone'] = vm_list[0].zone
      vm_config_dict['machine_type'] = vm_list[0].machine_type

      # TODO add network stuff here
      # IF VM HAS NETWORK CONFIG, put it here
      if vm_list[0].network_name:
        vm_config_dict['vpc_id'] = vm_list[0].network_name
        if vm_list[0].subnet_name: 
          vm_config_dict['subnet_id']  = vm_list[0].subnet_name

      temp['default']['vm_spec'][vm_list[0].cloud] = vm_config_dict

    else:
      for vm in vm_list:
        logger.debug("VM TO WRITE TO CONFIG")
        logger.debug(vm.__dict__)
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

        # TODO add network stuff here
        # IF VM HAS NETWORK CONFIG, put it here
        if vm.network_name:
          vm_config_dict['vpc_id'] = vm.network_name
          if vm.subnet_name: 
            vm_config_dict['subnet_id']  = vm.subnet_name

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
      vms = self.add_vms_for_benchmark_if_possible(bm)
      vms_no_none = list(filter(None, vms))

      if len(bm.vm_specs) == len(vms_no_none) == len(vms):
        bm.vms = vms
        bm.status = 'Not Executed'
        self.benchmarks.append(bm)
        self.add_benchmark_to_graph(bm, vms)
        bms_added.append(bm)

    # Remove bm from waitlist if it is added to graph
    logging.info("ADDED " + str(len(bms_added)) + " BENCHMARKS")
    for bm in bms_added:
      self.benchmark_wait_list.remove(bm)

    logger.debug("Number of benchmarks to run: " + str(len(self.graph.edges)))


  def remove_orphaned_nodes(self) -> List[int]:
    """Remove nodes in graph that have no edges

    Removes nodes in graph that have no edges.
    (Removes VMs that have no associated benchmarks)

    Returns:
        list: list of removed node IDs
    """
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
      self.regions[vm_region].remove_virtual_machine(vm)
      vm_removed_count += 1

    return keys_to_remove

  def benchmarks_left(self) -> int:
    # TODO fix this
    return len(self.graph.edges) + len(self.benchmark_wait_list)


  def get_all_quota_usage(self):
    region_quotas = []
    for region_name in self.regions:
      region_quotas.append(self.regions[region_name].get_all_quotas())
    return region_quotas