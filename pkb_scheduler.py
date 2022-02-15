# This tool requires the SMU AT&T CENTER pkb_autopilot_update branch
# of PerfkitBenchmarker to run properly

# It also requires config files passed to it to be formatted
# in a certain way. They should include the cloud and
# machine type flags, as well as other things that might be
# default when using just PKB
from __future__ import annotations
import yaml
import six
import copy
import itertools
import os
import benchmark_graph
import subprocess
import json
import time
import logging
import cloud_util
import uuid


from typing import List, Dict, Tuple, Set
from benchmark import Benchmark
from virtual_machine import VirtualMachine
from virtual_machine_spec import VirtualMachineSpec
from region import Region, GcpRegion, AwsRegion, AzureRegion
from cloud import Cloud
from absl import flags
from absl import app

# TODO
# tighter cohesion with pkb (use pkb classes)?

# put configs into unique directory
#   generate unique id per pkb_scheduler run
#   put all configs into that directory
# add in logic to not teardown a vm if a benchmark on the waitlist needs it


# TODO disk_stuff/dedicated host
#      to every config file. Edit the static vm stuff in pkb to handle it

# TODO add in the linear programming optimization stuff
# TODO support AWS and multicloud
# TODO thread and optimize what is happening at once when max threads is used
# TODO experiment with install_packages flag
# TODO check ssh key permissions

# TODO try reuse_ssh_connections
# TODO get to work with windows vms
# TODO get to work with VPNs
# TODO add defaults all in one place

# TODO change algorithm to try to limit egress/ingress per region
# per test

# TODO move skylake to config file 

# TODO add ability to reconfigure graph after each benchmark run


# python3

#EXAMPLE execution

#python3 pkb_scheduler.py --no_run=True --config=run_test/daily_interzone --precreate_and_share_vms=False --pkb_location=</full/path/to/PerfKitBenchmarker/pkb.py

FLAGS = flags.FLAGS


flags.DEFINE_boolean('no_run', False, 
                     'Prints out commands, but does not actually '
                     'run them')

flags.DEFINE_string('log_level', "info", 'info, warn, debug, error '
                    'prints debug statements')

flags.DEFINE_string('pkb_log_level', "info", 'info, warn, debug, error '
                    'prints debug statements for pkb processes')

# not implemented
flags.DEFINE_enum('optimize', 'TIME', ['TIME', 'SPACE'],
                  'Chooses whether algorithm should be more time or '
                  'space efficient.')

flags.DEFINE_boolean('allow_duplicate_vms', True,
                     'Defines whether or not tool should create '
                     'multiple identical VMs if there is capacity '
                     'and run tests in parallel or if it should '
                     'wait for existing vm to become available')

flags.DEFINE_integer('max_duplicate_vms', 1000,
                     'Amount of duplicate vms allowed')

flags.DEFINE_string('config', 'config.yaml',
                    'pass config file or directory')

flags.DEFINE_integer('max_processes', 30,
                     'max threads to use. A value of -1 will give '
                     'the system permission to use as many threads '
                     'as it wants. This may result in system slow downs '
                     'or hang ups')

flags.DEFINE_string('pkb_location',
                    "/home/derek/projects/virt_center/pkb_autopilot_branch/PerfKitBenchmarker/pkb.py",
                    'location of pkb on disk')

flags.DEFINE_boolean('print_graph', False,
                     'If True, tool will use pyplot to print a visual '
                     'representation of the benchmark_graph after every '
                     'iteration')

flags.DEFINE_string('bigquery_table', 'daily_tests.scheduler_test_1',
                    'bigquery table to push results to')

flags.DEFINE_string('bq_project', 'smu-benchmarking',
                    'bigquery project to push results to')

flags.DEFINE_boolean('precreate_and_share_vms', True,
                     'If true, this will precreate and reuse vms. '
                     'If false, every benchmark will create and destroy '
                     'its own VMS')

flags.DEFINE_boolean('use_maximum_matching', True,
                    'If true, this run VMs based on maximum matching')

flags.DEFINE_boolean('skip_prepare', True,
                     'skips the prepare phase for benchmarks where this is implemented')

flags.DEFINE_integer('regional_bandwidth_limit', None,
                     'Applies a bandwidth limit per region (Gbps)')
flags.DEFINE_integer('cloud_bandwidth_limit', None,
                     'Applies a bandwidth limit to all tests on a cloud (Gbps)')

flags.DEFINE_integer('max_retries', 20,
                     'Amount of times it will keep attempting to allocate and run tests that there are not space for. -1 for infinite')

logger = None

maximum_sets = []
vms_created = []
vms_removed = []
region_quota_usage = []

def main(argv):

  start_time = time.time()

  # setup logging and debug
  setup_logging()
  logger.debug("DEBUG LOGGING MODE")
  config_location = FLAGS.config
  pkb_command = "python3 " + FLAGS.pkb_location

  benchmark_config_list = []
  if(config_location.endswith(".yaml")):
    benchmark_config_list = parse_config_file(config_location)
  else:
    benchmark_config_list = parse_config_folder(config_location)

  # print(benchmark_config_list)
  

  logger.debug("\nNUMBER OF CONFIGS")
  logger.debug(len(benchmark_config_list))
  # for config in benchmark_config_list:
  #   print(config)

  # Create the initial graph from the config directory or file
  full_graph = create_graph_from_config_list(benchmark_config_list,
                                             pkb_command)

  # logger.debug("\nVMS TO CREATE:")
  # print(full_graph.virtual_machines)
  # for vm in full_graph.virtual_machines:
  #   print(vm.__dict__)

  logger.debug("\nBENCHMARKS TO RUN:")
  for bm in full_graph.benchmarks:
    logger.debug(f"Benchmark {bm}")

  logger.debug("\n\nFULL GRAPH:")
  logger.debug(full_graph.get_list_of_nodes())
  logger.debug(full_graph.get_list_of_edges())
  logger.debug("\n\n")

  # This method does almost everything
  run_benchmarks(full_graph)
  # test_stuff(full_graph)

  end_time = time.time()
  total_run_time = (end_time - start_time)

  # Print out Timing Metrics
  if len(list(filter(None, full_graph.vm_creation_times))) > 0:
    avg_vm_create_time = (sum(filter(None, full_graph.vm_creation_times)) /
                          len(list(filter(None, full_graph.vm_creation_times))))
    logging.info("AVG VM CREATION TIME: " + str(avg_vm_create_time))

  if len(list(filter(None, full_graph.benchmark_run_times))) > 0:
    avg_benchmark_run_time = (sum(filter(None, full_graph.benchmark_run_times)) /
                              len(list(filter(None, full_graph.benchmark_run_times))))
    logging.info("AVG BENCHMARK RUN TIME: " + str(avg_benchmark_run_time))

  # print("ALL MAXIMUM SETS")
  # for max_set in maximum_sets:
  #   print(max_set)
  # print("ALL VMS CREATED")
  # for vm_set in vms_created:
  #   print(vm_set)
  # print("ALL VMS DESTROYED")
  # for vm_set in vms_removed:
  #   print(vm_set)
    
  print("BENCHMARK SCHEDULE HISTORY")
  vms_at_time = 0
  if FLAGS.precreate_and_share_vms:
    for i in range(0, len(maximum_sets)):
      vms_at_time = vms_at_time + len(vms_created[i])
      vms_used_this_round = len(list(itertools.chain(*maximum_sets[i])))
      print(f"ROUND \n{i}")
      print(f"VM USAGE: {vms_used_this_round}/{vms_at_time}")
      print(f"{len(vms_created[i])} VMS CREATED:")
      print(vms_created[i])
      print(f"MAXIMUM SET LENGTH: {len(maximum_sets[i])}, ARRAY:")
      print(maximum_sets[i])
      print(f"{len(vms_removed[i])} VMS DESTROYED:")
      print(vms_removed[i])
      vms_at_time = vms_at_time - len(vms_removed[i])
      print(f"ALL REGION QUOTA USAGE:")
      for region_quota in region_quota_usage[i]:
        for key in region_quota:
          quota = region_quota[key]
          #print(f"region: {key}, CPU QUOTA: {quota['CPUS']['limit']}, "
          #      f"CPU USE: {quota['CPUS']['usage']}, "
          #      f"ADDRS QUOTA: {quota['IN_USE_ADDRESSES']['limit']}, "
          #      f"ADDRS USE: {quota['IN_USE_ADDRESSES']['usage']}")
      # print(region_quota_usage[i])
  else:
    for i in range(0, len(maximum_sets)):
      print(f"ROUND \n{i}")
      print(f"MAXIMUM SET LENGTH: {len(maximum_sets[i])}, ARRAY:")
      print(maximum_sets[i])
  
  print("ALL BENCHMARK TIMES:")
  print(full_graph.benchmark_run_times)
  try:
    print(f"TOTAL BENCHMARK TIME: {sum(full_graph.benchmark_run_times)}")
  except:
    pass

  print("TOTAL VM UPTIME: ")
  total_time = 0
  for vm in full_graph.virtual_machines:
    total_time = total_time + vm.uptime()
  print(total_time)

  print("TOTAL RUN TIME: " + str(total_run_time) + " seconds")

  exit(0)


def setup_logging():

  global logger
  numeric_level = getattr(logging, FLAGS.log_level.upper(), None)
  # create logger
  logger = logging.getLogger('pkb_scheduler')
  logger.setLevel(numeric_level)
  # create console handler and set level to debug
  ch = logging.StreamHandler()
  ch.setLevel(numeric_level)
  formatter = logging.Formatter('%(message)s')
  ch.setFormatter(formatter)
  logger.propagate = False
  logger.addHandler(ch)

  return logger


def test_stuff(benchmark_graph):
  maximum_set = benchmark_graph.get_benchmark_set()
  print("MAXIMUM SET")
  print(maximum_set)
  print(len(maximum_set))


def run_benchmarks(benchmark_graph):

  benchmarks_run = []
  benchmark_graph.equalize_graph()
  if FLAGS.print_graph:
    benchmark_graph.print_graph()
  max_set_empty_counter = 0

  while benchmark_graph.benchmarks_left() > 0:
    logging.info(f"graph nodes remaining: {len(benchmark_graph.graph.nodes)}")
    logging.info(f"graph edges remaining: {len(benchmark_graph.graph.edges)}")
    logging.info(f"benchmarks on waitlist: {len(benchmark_graph.benchmark_wait_list)}" )
    logging.info(f"benchmarks left: {benchmark_graph.benchmarks_left()}")
    logging.info(f"multiedge benchmarks: {benchmark_graph.multiedge_benchmarks}")

    # TODO make get_benchmark_set work better than maximum matching
    maximum_set = benchmark_graph.get_benchmark_set()
    # maximum_set = list(benchmark_graph.maximum_matching())

    if len(maximum_set) == 0:
      max_set_empty_counter += 1
    else:
      max_set_empty_counter = 0

    if FLAGS.max_retries >= 0 and max_set_empty_counter > FLAGS.max_retries:
      exit()
    print("MAXIMUM SET")
    print(maximum_set)

    max_set_vms = list(itertools.chain(*maximum_set))
    if FLAGS.precreate_and_share_vms:
      created_list = benchmark_graph.create_vms(vm_list=max_set_vms)
      vms_created.append(created_list)

    maximum_sets.append(maximum_set)
    benchmarks_run.append(maximum_set)
    quota_usage = benchmark_graph.get_all_quota_usage()
    for region_quota in quota_usage:
      for key in region_quota:
        quota = region_quota[key]
        #print(f"region: {key}, CPU QUOTA: {quota['CPUS']['limit']}, "
        #      f"CPU USE: {quota['CPUS']['usage']}, "
        #      f"ADDRS QUOTA: {quota['IN_USE_ADDRESSES']['limit']}, "
        #      f"ADDRS USE: {quota['IN_USE_ADDRESSES']['usage']}")
    region_quota_usage.append(quota_usage)
    # This actually runs all the benchmarks in this set
    benchmark_graph.run_benchmark_set(maximum_set)
    # TODO possibly check completion status
    # Completion statuses can be found at: 
    # /tmp/perfkitbenchmarker/runs/7fab9158/completion_statuses.json
    # before removal of edges
    benchmark_graph.add_benchmarks_from_waitlist()
    benchmark_graph.equalize_graph()
    removed_list = benchmark_graph.remove_orphaned_nodes()
    vms_removed.append(removed_list)
    logging.info("UPDATE REGION QUOTAS")
    update_quota_usage(benchmark_graph)
    logging.debug("create vms and add benchmarks")
    benchmark_graph.add_benchmarks_from_waitlist()
    benchmark_graph.equalize_graph()
    logging.debug("benchmarks left: " + str(benchmark_graph.benchmarks_left()))
    time.sleep(2)
    if FLAGS.print_graph:
      benchmark_graph.print_graph()

  logging.debug(len(benchmarks_run))
  logging.debug("BMS RUN EACH LOOP")
  for bmset in benchmarks_run:
    logging.debug(len(bmset))

  logging.debug("VMS REMOVED EACH LOOP")
  for vm_list in vms_removed:
    logging.debug(vm_list)


def update_quota_usage(benchmark_graph):
  """update the regional quotas based on data pulled from the cloud provider

  Pulls current usage information from the cloud provider and updates
  quota information based off of that

  Args:
    benchmark_graph: Benchmark/VM Graph to update
  """

  if FLAGS.no_run:
    return

  for cloud in benchmark_graph.clouds:
    #TODO change this
    print(f"\n\n\n\nHELLO? Cloud is: {cloud}\n\n\n\n")
    if cloud == 'GCP':
      print(benchmark_graph.regions)
      region_dict = cloud_util.get_region_info(cloud='GCP')
      for region_name in benchmark_graph.regions:
        if benchmark_graph.regions[region_name].cloud == 'GCP':
          quotas = region_dict[region_name]
          benchmark_graph.regions[region_name].update_quotas(quotas)
    elif cloud == 'AWS':
      region_dict = cloud_util.get_region_info(cloud='AWS')
      print(benchmark_graph.regions)
      print(region_dict)
      for region_name in benchmark_graph.regions:
        if benchmark_graph.regions[region_name].cloud == 'AWS':
          quotas = region_dict[region_name]
          benchmark_graph.regions[region_name].update_quotas(quotas)
    elif cloud == 'Azure':
      region_dict = cloud_util.get_region_info(cloud='Azure')
      for region_name in benchmark_graph.regions:
        if benchmark_graph.regions[region_name].cloud == 'Azure':
          quotas = region_dict[region_name]
          print(f"\n\n\nQuotas from main: {quotas}\n\n\n")
          
          benchmark_graph.regions[region_name].update_quotas(quotas)
          print(f"\n\n\nQuotas from main: {quotas}\n\n\n")
    else:
      return


def create_benchmark_from_config(benchmark_config, benchmark_id):
  bm = None
  # print(config[1]['flags']['zones'])
  # print(benchmark_config[1]['flags']['extra_zones'])
  # full_graph.add_region_if_not_exists(region_name)


  # print('benchmark_config------------------')
  # print(type(benchmark_config))
  # print(benchmark_config)
  # print('----------------------------------')

  # print(benchmark_config[1].keys())
  # benchmark_config[0] is name of benchmark
  # benchmark_config[1] is the config for the benchmark

  if 'vm_groups' in benchmark_config[1]:
    vm_config_list = []
    for key in benchmark_config[1]['vm_groups']:
      vm = benchmark_config[1]['vm_groups'][key]
      # print(vm)
      vm_config_tmp = {}
      vm_config_tmp['cloud'] = vm['cloud']
      vm_config_tmp['os_type'] = 'ubuntu1804'
      if 'os_type' in vm:
        vm_config_tmp['os_type'] = vm['os_type']
      elif 'os_type' in benchmark_config[1]['flags']:
        vm_config_tmp['os_type'] = benchmark_config[1]['flags']['os_type']

      vm_config_tmp['machine_type'] = vm['vm_spec'][vm['cloud']]['machine_type']
      vm_config_tmp['zone'] = vm['vm_spec'][vm['cloud']]['zone']
      vm_config_tmp['cpu_count'] = cloud_util.cpu_count_from_machine_type(vm_config_tmp['cloud'],
                                                                          vm_config_tmp['machine_type'])
      if 'vpc_id' in vm['vm_spec'][vm['cloud']]:
        vm_config_tmp['network_name'] = vm['vm_spec'][vm['cloud']]['vpc_id']
      else:
        if vm_config_tmp['cloud'] == 'GCP' and 'gce_network_name' in benchmark_config[1]['flags']:
          vm_config_tmp['network_name'] = benchmark_config[1]['flags']['gce_network_name']
        elif vm_config_tmp['cloud'] == 'AWS' and 'aws_vpc' in benchmark_config[1]['flags']:
          vm_config_tmp['network_name'] = benchmark_config[1]['flags']['aws_vpc']
        elif vm_config_tmp['cloud'] == 'Azure':
          pass
        else:
          vm_config_tmp['network_name'] = None

      if 'subnet_id' in vm['vm_spec'][vm['cloud']]:
        vm_config_tmp['subnet_name'] = vm['vm_spec'][vm['cloud']]['subnet_id']
      else:
        if vm_config_tmp['cloud'] == 'GCP' and 'gce_subnet_name' in benchmark_config[1]['flags']:
          vm_config_tmp['subnet_name'] = None
        elif vm_config_tmp['cloud'] == 'AWS' and 'aws_subnet' in benchmark_config[1]['flags']:
          vm_config_tmp['subnet_name'] = benchmark_config[1]['flags']['aws_subnet']
        elif vm_config_tmp['cloud'] == 'Azure':
          pass
        else:
          vm_config_tmp['subnet_name'] = None
      # TODO add disk config

      # gce network tier is a flag, but we add it to the vm spec to better compare equivalent vms
      vm_config_tmp['network_tier'] = None
      vm_config_tmp['min_cpu_platform'] = None
      if vm['cloud'] == 'GCP':
        # network_tier = 'premium'
        vm_config_tmp['network_tier'] = 'premium'
        if 'gce_network_tier' in benchmark_config[1]['flags']:
          vm_config_tmp['network_tier'] = benchmark_config[1]['flags']['gce_network_tier']
        if 'gcp_min_cpu_platform' in benchmark_config[1]['flags'] :
         vm_config_tmp['min_cpu_platform'] = benchmark_config[1]['flags']['gcp_min_cpu_platform']

      # print("VM CONFIG TMP")
      # print(vm_config_tmp)
      if 'vm_count' in vm:
        for i in range(0,vm['vm_count']):
          vm_config_list.append(vm_config_tmp)
      else:
        vm_config_list.append(vm_config_tmp)

    bigquery_table = FLAGS.bigquery_table
    bq_project = FLAGS.bq_project
    if 'bigquery_table' in benchmark_config[1]['flags']:
      bigquery_table = benchmark_config[1]['flags']['bigquery_table']
    if 'bq_project' in benchmark_config[1]['flags']:
      bq_project = benchmark_config[1]['flags']['bq_project']

    vm_specs = []

    for vm_config in vm_config_list:
      uuid_tmp = uuid.uuid1().int
      vm_spec = VirtualMachineSpec(uid=uuid_tmp,
                                   cpu_count=vm_config['cpu_count'],
                                   zone=vm_config['zone'],
                                   cloud=vm_config['cloud'],
                                   machine_type=vm_config['machine_type'],
                                   network_tier=vm_config['network_tier'],
                                   os_type=vm_config['os_type'],
                                   min_cpu_platform=vm_config['min_cpu_platform'],
                                   network_name=vm_config['network_name'],
                                   subnet_name=vm_config['subnet_name'])
      vm_specs.append(vm_spec)

    bm = Benchmark(benchmark_id=benchmark_id,
                   benchmark_type=benchmark_config[0],
                   vm_specs=vm_specs,
                   bigquery_table=bigquery_table,
                   bq_project=bq_project,
                   flags=benchmark_config[1]['flags'])

  else:
    cloud = benchmark_config[1]['flags']['cloud']
    machine_type=benchmark_config[1]['flags']['machine_type']
    cpu_count = cloud_util.cpu_count_from_machine_type(cloud, machine_type)

    # TODO, set default somewhere else
    network_tier='premium'
    if 'gce_network_tier' in benchmark_config[1]['flags']:
      network_tier = benchmark_config[1]['flags']['gce_network_tier']

    bigquery_table = FLAGS.bigquery_table
    bq_project = FLAGS.bq_project
    if 'bigquery_table' in benchmark_config[1]['flags']:
      bigquery_table = benchmark_config[1]['flags']['bigquery_table']
    if 'bq_project' in benchmark_config[1]['flags']:
      bq_project = benchmark_config[1]['flags']['bq_project']

    os_type = 'ubuntu1804'
    if 'os_type' in benchmark_config[1]['flags']:
      os_type = benchmark_config[1]['flags']['os_type']


    # TODO expand for aws and azure
    network_name = None
    if 'gce_network_name' in benchmark_config[1]['flags']:
      network_name = benchmark_config[1]['flags']['gce_network_name']

    subnet_name = None
    if 'gce_subnet_name' in benchmark_config[1]['flags']:
      subnet_name = benchmark_config[1]['flags']['gce_subnet_name']

    min_cpu_platform = None
    if 'gcp_min_cpu_platform' in benchmark_config[1]['flags']:
      min_cpu_platform = benchmark_config[1]['flags']['gcp_min_cpu_platform']

    uuid_1 = uuid.uuid1().int
    uuid_2 = uuid.uuid1().int

    vm_spec_1 = VirtualMachineSpec(uid=uuid_1,
                                   cpu_count=cpu_count,
                                   zone=benchmark_config[1]['flags']['zones'],
                                   cloud=cloud,
                                   machine_type=machine_type,
                                   network_tier=network_tier,
                                   os_type=os_type,
                                   min_cpu_platform=min_cpu_platform,
                                   network_name=network_name,
                                   subnet_name=subnet_name)
    vm_specs = [vm_spec_1]
    if 'extra_zones' in benchmark_config[1]['flags']:
      vm_spec_2 = VirtualMachineSpec(uid=uuid_2,
                                     cpu_count=cpu_count,
                                     zone=benchmark_config[1]['flags']['extra_zones'],
                                     cloud=cloud,
                                     machine_type=machine_type,
                                     network_tier=network_tier,
                                     os_type=os_type,
                                     min_cpu_platform=min_cpu_platform,
                                     network_name=network_name,
                                     subnet_name=subnet_name)
      vm_specs.append(vm_spec_2)
    bm = Benchmark(benchmark_id=benchmark_id,
                   benchmark_type=benchmark_config[0],
                   vm_specs=vm_specs,
                   bigquery_table=bigquery_table,
                   bq_project=bq_project,
                   flags=benchmark_config[1]['flags'])
    # print("FLAGS STUFF HERE")
    # print(benchmark_config[1]['flags'])
    # print(bm.flags)

  return bm

def create_graph_from_config_list(benchmark_config_list, pkb_command):
  """[summary]

  [description]

  Args:
    benchmark_config_list: list of dictionaries containing benchmark configs

  Returns:
    [description]
    [type]
  """

  full_graph = benchmark_graph.BenchmarkGraph(ssh_pub="ssh_key.pub",
                                              ssh_priv="ssh_key",
                                              ssl_cert="cert.pem",
                                              pkb_location=pkb_command,
                                              bigquery_table=FLAGS.bigquery_table,
                                              bq_project=FLAGS.bq_project)

  # First pass, find all the regions and add them to the graph
  # config[0] is the benchmark_name
  # config[1] is all the flags

  # TODO MAKE CLOUDS
  # ADD REGIONS TO CLOUDS
  # IF AWS REGION, GIVE VPC QUOTA OF 5
  # IF AWS CLOUD, give CPU resource of 1900
  # USE THIS COMMAND TO GET USAGE
  # aws ec2 describe-vpcs --region us-east-1

  # get all regions from gcloud
  # make regions
  clouds_in_benchmark_set = []
  for benchmark_config in benchmark_config_list: 
    if 'vm_groups' in benchmark_config[1]:
      for key in benchmark_config[1]['vm_groups']:
        cloud = benchmark_config[1]['vm_groups'][key]['cloud']
        clouds_in_benchmark_set.append(cloud)
    else:
      cloud = benchmark_config[1]['flags']['cloud']
      clouds_in_benchmark_set.append(cloud)

  clouds_in_benchmark_set = list(set(clouds_in_benchmark_set))
  print("CLOUDS IN BENCHMARK SET")
  print(clouds_in_benchmark_set)

  # CREATE and ADD regions for GCP

  if 'GCP' in clouds_in_benchmark_set:
    region_dict = cloud_util.get_region_info(cloud='GCP')

    new_cloud = Cloud('GCP', instance_quota=None, cpu_quota=None, address_quota=None, bandwidth_limit=FLAGS.cloud_bandwidth_limit)
    full_graph.add_cloud_if_not_exists(new_cloud)
    for key in region_dict:
      # if region['description'] in full_graph.regions
      new_region = GcpRegion(region_name=key,
                             cloud=new_cloud,
                             quotas=region_dict[key],
                             bandwidth_limit=FLAGS.regional_bandwidth_limit)
      full_graph.add_region_if_not_exists(new_region=new_region)

  if 'AWS' in clouds_in_benchmark_set:
    # CREATE and ADD regions for AWS
    new_cloud = Cloud('AWS', instance_quota=None, cpu_quota=None, address_quota=None, bandwidth_limit=FLAGS.cloud_bandwidth_limit)
    full_graph.add_cloud_if_not_exists(new_cloud)
    region_dict = cloud_util.get_region_info(cloud='AWS')
    for key in region_dict:
      # if region['description'] in full_graph.regions
      new_region = AwsRegion(region_name=key,
                             cloud=new_cloud,
                             quotas=region_dict[key],
                             bandwidth_limit=FLAGS.regional_bandwidth_limit)
      full_graph.add_region_if_not_exists(new_region=new_region)

  if 'Azure' in clouds_in_benchmark_set:
    # CREATE and ADD regions for Azure
    # Troy if there are any cloud wide quotas, we should deal with them here
    # Also let me know, because I'll need to add better support for cloud-wide quotas (currently there is ~None)
    new_cloud = Cloud('Azure', instance_quota=None, cpu_quota=None, address_quota=None, bandwidth_limit=FLAGS.cloud_bandwidth_limit)
    full_graph.add_cloud_if_not_exists(new_cloud)
    region_dict = cloud_util.get_region_info(cloud='Azure')
    for key in region_dict:
      # if region['description'] in full_graph.regions
      print(f"\n\n\nADDING QUOTAS HERE: {region_dict[key]}\n\n\n")
      new_region = AzureRegion(region_name=key,
                               cloud=new_cloud,
                               quotas=region_dict[key],
                               bandwidth_limit=FLAGS.regional_bandwidth_limit)
      full_graph.add_region_if_not_exists(new_region=new_region)


  # This takes all the stuff from the config dictionaries
  # and puts them in benchmark objects
  # will need more logic for differently formatted configs
  benchmark_counter = 0
  temp_benchmarks = []
  for config in benchmark_config_list:
    # print(config[1]['flags']['zones'])
    # region_name = config[1]['flags']['zones']
    # print(config[1]['flags']['extra_zones'])
    # full_graph.add_region_if_not_exists(region_name)

    new_benchmark = create_benchmark_from_config(config,
                                                 benchmark_counter)
    temp_benchmarks.append(new_benchmark)
    benchmark_counter += 1

  logger.debug("Number of benchmarks: " + str(len(temp_benchmarks)))
  temp_benchmarks.sort(key=lambda x: x.largest_vm, reverse=True)
  # create virtual machines (node)
  # attach with edges and benchmarks
  for bm in temp_benchmarks:
    logger.debug(f"Trying to add {bm}")
    vms = full_graph.add_or_waitlist_benchmark_and_vms(bm)

  logger.debug("Number of benchmarks: " + str(len(full_graph.benchmarks)))

  # Second pass, add
  for config in benchmark_config_list:
    pass

  return full_graph


def parse_config_folder(path="configs/", ignore_hidden_folders=True):
  """Parse all config files found in a directory and sub directories

  Parse all .yaml and .yml config files in a folder
  and subfolders

  Args:
    path: The folder path to look for config files (default: {"configs/"})
  """
  file_list = []
  for r, d, f in os.walk(path):
    for file in f:
      if ('.yaml' in file) or ('.yml' in file):
        file_path = os.path.join(r, file)
        if ('/.' not in file_path) or (ignore_hidden_folders is False):
          file_list.append(os.path.join(r, file))

  for f in file_list:
    print(f)

  benchmark_config_list = []
  for file in file_list:
    benchmark_config_list.extend(parse_config_file(file))

  return benchmark_config_list


def parse_config_file(path="configs/file.yaml"):
  """Parse config file functions

  largely taken from the PKB parsing function

  Args:
    path: [description] (default: {"configs/file.yaml"})

  Returns:
    [description]
    [type]
  """

  crossed_axes = []
  benchmark_config_list = []

  # open file and parse yaml
  # return empty list if not correct yaml
  f = open(path, "r")
  contents = f.read()
  yaml_contents = yaml.safe_load(contents)
  if not isinstance(yaml_contents, dict):
    return []

  print('YAML CONTENTS---------------------------')
  print(yaml_contents)
  print('----------------------------------------')

  # print(yaml_contents.keys())
  benchmark_name = list(yaml_contents.keys())[0]
  config_dict = yaml_contents[benchmark_name]

  flag_matrix_name = config_dict.get('flag_matrix', None)
  flag_matrix = config_dict.pop(
      'flag_matrix_defs', {}).get(flag_matrix_name, {})

  flag_matrix_filter = config_dict.pop(
      'flag_matrix_filters', {}).get(flag_matrix_name, {})

  config_dict.pop('flag_matrix', None)
  config_dict.pop('flag_zip', None)

  # for f in flag_matrix:
  #   print(flag_matrix[f])

  # for c in config_dict:
  #   print(config_dict[c])

  for flag, values in sorted(six.iteritems(flag_matrix)):
      crossed_axes.append([{flag: v} for v in values])

  # print("crossed_axes")
  # print(crossed_axes)

  for flag_config in itertools.product(*crossed_axes):
    config = {}
    # print("FLAG CONFIG")
    # print(flag_config)
    config = _GetConfigForAxis(config_dict, flag_config)
    # print("CONFIG")
    # print(config)
    if (flag_matrix_filter and not eval(flag_matrix_filter, {},
                                        config['flags'])):
      print("Did not pass Flag Matrix Filter")
      continue

    benchmark_config_list.append((benchmark_name, config))
    # print("BENCHMARK CONFIG")
    # print(benchmark_config_list[0])

  # print("BENCHMARK CONFIG LIST")
  # for config in benchmark_config_list:
  #   print(config)

  f.close()

  return benchmark_config_list


def _GetConfigForAxis(benchmark_config, flag_config):
  config = copy.copy(benchmark_config)
  config_local_flags = config.get('flags', {})
  # config['flags'] = []
  config['flags'].update(config_local_flags)
  for setting in flag_config:
    config['flags'].update(setting)
  return_config = copy.deepcopy(config)
  return return_config


def parse_named_configs(config):
  pass


def parse_named_config(config):
  pass


if __name__ == "__main__":
  app.run(main)
