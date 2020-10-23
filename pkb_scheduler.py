# This tool requires the SMU AT&T CENTER pkb_autopilot branch
# of PerfkitBenchmarker to run properly

# It also requires config files passed to it to be formatted
# in a certain way. They should include the cloud and
# machine type flags, as well as other things that might be
# default when using just PKB

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
from region import Region
from absl import flags
from absl import app

# TODO
# parse diff types of files
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


# python3

#EXAMPLE execution

#python3 pkb_scheduler.py --no_run=True --config=run_test/daily_interzone --precreate_and_share_vms=False --pkb_location=</full/path/to/PerfKitBenchmarker/pkb.py

FLAGS = flags.FLAGS


flags.DEFINE_boolean('no_run', False, 
                     'Prints out commands, but does not actually '
                     'run them')

flags.DEFINE_string('log_level', "INFO", 'info, warn, debug, error '
                    'prints debug statements')

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

logger = None

maximum_sets = []

def main(argv):

  start_time = time.time()

  # setup logging and debug
  setup_logging()
  logger.debug("DEBUG LOGGING MODE")
  config_location = FLAGS.config
  pkb_command = "python " + FLAGS.pkb_location

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

  # benchmark_config_list = parse_config_folder("/home/derek/projects/pkb_scheduler")

  print("COMPLETE BENCHMARK CONFIG LIST")
  for tmpbm in benchmark_config_list:
    print(tmpbm)

  # Create the initial graph from the config directory or file
  full_graph = create_graph_from_config_list(benchmark_config_list,
                                             pkb_command)

  logger.debug("\nVMS TO CREATE:")
  for vm in full_graph.virtual_machines:
    logger.debug(vm.zone + " " + vm.network_tier + " " + vm.machine_type +
                 " " + vm.os_type + " " + vm.cloud)

  logger.debug("\nBENCHMARKS TO RUN:")
  for bm in full_graph.benchmarks:
    logger.debug("Benchmark " + bm.vm_specs[0].zone + "--" + bm.vm_specs[1].zone)

  logger.debug("\n\nFULL GRAPH:")
  logger.debug(full_graph.get_list_of_nodes())
  logger.debug(full_graph.get_list_of_edges())
  logger.debug("\n\n")

  # This method does almost everything
  run_benchmarks(full_graph)
  # test_stuff(full_graph)

  end_time = time.time()
  total_run_time = (end_time - start_time)
  print("TOTAL RUN TIME: " + str(total_run_time) + " seconds")

  # Print out Timing Metrics
  if len(list(filter(None, full_graph.vm_creation_times))) > 0:
    avg_vm_create_time = (sum(filter(None, full_graph.vm_creation_times)) /
                          len(list(filter(None, full_graph.vm_creation_times))))
    logging.info("AVG VM CREATION TIME: " + str(avg_vm_create_time))

  if len(list(filter(None, full_graph.benchmark_run_times))) > 0:
    avg_benchmark_run_time = (sum(filter(None, full_graph.benchmark_run_times)) /
                              len(list(filter(None, full_graph.benchmark_run_times))))
    logging.info("AVG BENCHMARK RUN TIME: " + str(avg_benchmark_run_time))

  print("ALL MAXIMUM SETS")
  for max_set in maximum_sets:
    print(max_set)

  print("ALL BENCHMARK TIMES:")
  print(full_graph.benchmark_run_times)

  print("TOTAL VM UPTIME: ")
  total_time = 0
  for vm in full_graph.virtual_machines:
    total_time = total_time + vm.uptime()
  print(total_time)


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
  
  if FLAGS.precreate_and_share_vms:
    benchmark_graph.create_vms()

  benchmarks_run = []
  # benchmark_graph.print_graph()
  vms_removed = []
  while benchmark_graph.benchmarks_left() > 0:

    # TODO make get_benchmark_set work better than maximum matching
    maximum_set = list(benchmark_graph.maximum_matching())
    print("MAXIMUM SET")
    print(maximum_set)
    maximum_sets.append(maximum_set)
    benchmarks_run.append(maximum_set)
    benchmark_graph.run_benchmark_set(maximum_set)
    # possibly check
    # Completion statuses can be found at: 
    # /tmp/perfkitbenchmarker/runs/7fab9158/completion_statuses.json
    # before removal of edges
    removed_count = benchmark_graph.remove_orphaned_nodes()
    vms_removed.append(removed_count)
    logging.info("UPDATE REGION QUOTAS")
    update_quota_usage(benchmark_graph)
    logging.debug("create vms and add benchmarks")
    benchmark_graph.add_benchmarks_from_waitlist()
    if FLAGS.precreate_and_share_vms:
      benchmark_graph.create_vms()
    logging.debug("benchmarks left: " + str(benchmark_graph.benchmarks_left()))
    time.sleep(2)
    # benchmark_graph.print_graph()

  logging.debug(len(benchmarks_run))
  logging.debug("BMS RUN EACH LOOP")
  for bmset in benchmarks_run:
    logging.debug(len(bmset))

  logging.debug("VMS REMOVED EACH LOOP")
  for vm_count in vms_removed:
    logging.debug(vm_count)


def update_quota_usage(benchmark_graph):
  """update the regional quotas based on data pulled from the cloud provider

  Pulls current usage information from the cloud provider and updates
  quota information based off of that

  Args:
    benchmark_graph: Benchmark/VM Graph to update
  """

  for cloud in benchmark_graph.clouds:
    #TODO change this
    region_dict = cloud_util.get_region_info(cloud='GCP')
    # print(region_dict)
    for region_name in benchmark_graph.regions:
      cpu_usage = region_dict[region_name]['CPUS']['usage']
      address_usage = region_dict[region_name]['IN_USE_ADDRESSES']['usage']
      benchmark_graph.regions[region_name].update_cpu_usage(cpu_usage)
      benchmark_graph.regions[region_name].update_address_usage(address_usage)


def create_benchmark_from_config(benchmark_config, benchmark_id):
  bm = None
  # print(config[1]['flags']['zones'])
  # print(benchmark_config[1]['flags']['extra_zones'])
  # full_graph.add_region_if_not_exists(region_name)

  if 'vm_groups' in benchmark_config[1]:
    logging.error("Configs with vm groups not supported yet")
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

    # TODO change to none and put gcp_min_cpu_platform: skylake in all the configs
    min_cpu_platform = 'skylake'
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
                                   min_cpu_platform=min_cpu_platform)
    vm_spec_2 = VirtualMachineSpec(uid=uuid_2,
                                   cpu_count=cpu_count,
                                   zone=benchmark_config[1]['flags']['extra_zones'],
                                   cloud=cloud,
                                   machine_type=machine_type,
                                   network_tier=network_tier,
                                   os_type=os_type,
                                   min_cpu_platform=min_cpu_platform)
    vm_specs = [vm_spec_1, vm_spec_2]
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
  #print("pkb_scheduler Cloud Variable is: {}".format(cloud))
  #print("config is : {}".format(benchmark_config_list))
  print("\n\nconfig[0] is : {}".format(benchmark_config_list[0]))
  print("\n\nconfig[0][1] is : {}".format(benchmark_config_list[0][1]))
  print("\n\nconfig[0][1]['flags'] is : {}".format(benchmark_config_list[0][1]['flags']))
  print("\n\nconfig[0][1]['flags']['cloud'] is : {}".format(benchmark_config_list[0][1]['flags']['cloud']))
 # region_dict = cloud_util.get_region_info(cloud='GCP')
  print(f"FLAGS is {flags}")
 # region_dict = cloud_util.get_region_info(benchmark_config_list[0][1]['flags']['cloud'].upper())
  # full_graph.add_region_if_not_exists(new_region=benchmark_config_list[0][1]['flags']['zones'])
  


#THIS CRASHES EVERY TIME
  region_dict = cloud_util.get_region_info(benchmark_config_list[0][1]['flags'])
  print(f"\n\nRegion_Dict has been declared!\n\n")
  
  
  
  for key in region_dict:
    # if region['description'] in full_graph.regions
    print(f"\n\nTrying to make a new region\n\n")
    new_region = Region(region_name=key,
                        cloud='GCP',
                        cpu_quota=region_dict[key]['CPUS']['limit'],
                        cpu_usage=region_dict[key]['CPUS']['usage'])
    new_region.update_address_quota(region_dict[key]['IN_USE_ADDRESSES']['limit'])
    new_region.update_address_usage(region_dict[key]['IN_USE_ADDRESSES']['usage'])
    full_graph.add_region_if_not_exists(new_region=new_region)
  print(f"\n\nThe region has been made\n\n")
  # This takes all the stuff from the config dictionaries
  # and puts them in benchmark objects
  # will need more logic for differently formatted configs
  benchmark_counter = 0
  temp_benchmarks = []
  print(f"\n\nEntering the for loop\n\n")
  for config in benchmark_config_list:
    # print(config[1]['flags']['zones'])
    # region_name = config[1]['flags']['zones']
    # print(config[1]['flags']['extra_zones'])
    # full_graph.add_region_if_not_exists(region_name)
    print(f"\n\nMaking new benchmark\n\n")
    new_benchmark = create_benchmark_from_config(config,
                                                 benchmark_counter)
    print(f"\n\nBenchmark has been made!\n\n")
    # new_benchmark = Benchmark(benchmark_id=benchmark_counter,
    #                           benchmark_type=config[0],
    #                           zone1=config[1]['flags']['zones'],
    #                           zone2=config[1]['flags']['extra_zones'],
    #                           machine_type=config[1]['flags']['machine_type'],
    #                           cloud=config[1]['flags']['cloud'],
    #                           flags=config[1]['flags'])
    temp_benchmarks.append(new_benchmark)
    benchmark_counter += 1
  print(f"\n\nLeaving the for loops\n\n")
  logger.debug("Number of benchmarks: " + str(len(temp_benchmarks)))

  # create virtual machines (node)
  # attach with edges and benchmarks q
  print(f"temp_benchmarks is {temp_benchmarks[0].__dict__}")
  for bm in temp_benchmarks:
    logger.debug("Trying to add " + bm.vm_specs[0].zone + " and " + bm.vm_specs[1].zone)
    print(f"Early BM is {bm.__dict__}")
    vms = full_graph.add_or_waitlist_benchmark_and_vms(bm)
    print(f"\n\nVMS declared successfuly.\n\n")
  logger.debug("Number of benchmarks: " + str(len(full_graph.benchmarks)))

  # Second pass, add
  for config in benchmark_config_list:
    pass
  print(f"\n\nExiting from create_graph_from_config_list\n\n")
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
  yaml_contents = yaml.load(contents)
  if not isinstance(yaml_contents, dict):
    return []

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
