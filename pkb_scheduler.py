# import graph
import yaml
import six
# import collections
import copy
import itertools
import os
import benchmark_graph
import subprocess
import json
import time
import logging
# import networkx as nx

from typing import List, Dict, Tuple, Set
from benchmark import Benchmark
from virtual_machine import VirtualMachine
from region import Region
from absl import flags
from absl import app

# TODO
# parse diff types of files
# tighter cohesion with pkb (use pkb classes)?
# add in timing metrics
# add phase to make more VMs

# put configs into unique directory
#   generate unique id per pkb_scheduler run
#   put all configs into that directory
# add in logic to not teardown a vm if a benchmark on the waitlist needs it

# TODO add logic to add identical VM if there is space

# TODO make work for config directories

# re get quota on every creation/deletion

FLAGS = flags.FLAGS


flags.DEFINE_boolean('no_run', False, 
                     'Prints out commands, but does not actually '
                     'run them')
flags.DEFINE_string('log_level', "INFO", 'info, warn, debug, error '
                    'prints debug statements')

#not implemented
flags.DEFINE_enum('optimize', 'TIME', ['TIME', 'SPACE'] 
                  'Chooses whether algorithm should be more time or ' 
                   'space efficient.')

flags.DEFINE_boolean('allow_duplicate_vms', True, 
                     'Defines whether or not tool should create '
                     'multiple identical VMs if there is capacity '
                     'and run tests in parallel or if it should '
                     'wait for existing vm to become available')

flags.DEFINE_string('config', 'config.yaml', 
                    'pass config file or directory')


logger = None

def main(argv):

  setup_logging()

  start_time = time.time()

  logger.debug("DEBUG LOGGING MODE")
  config_file = FLAGS.config
  benchmark_config_list = parse_config_file(config_file)

  print(benchmark_config_list)

  logger.debug("\nNUMBER OF CONFIGS")
  logger.debug(len(benchmark_config_list))
  # for config in benchmark_config_list:
  #   print(config)

  # benchmark_config_list = parse_config_folder("/home/derek/projects/pkb_scheduler")

  # print(benchmark_config_list)

  full_graph = create_graph_from_config_list(benchmark_config_list)


##########################

  logger.debug("\nVMS TO CREATE:")
  for vm in full_graph.virtual_machines:
    logger.debug(vm.zone + " " + vm.network_tier + " " + vm.machine_type
          + " " + vm.os_type + " " + vm.cloud)

  logger.debug("\nBENCHMARKS TO RUN:")
  for bm in full_graph.benchmarks:
    logger.debug("Benchmark " + bm.zone1 + "--" + bm.zone2)

  create_benchmark_schedule(full_graph)

  logger.debug("\n\nFULL GRAPH:")
  logger.debug(full_graph.get_list_of_nodes())
  logger.debug(full_graph.get_list_of_edges())
  logger.debug("\n\n")

  run_benchmarks(full_graph)

  end_time = time.time()
  total_run_time = (end_time - start_time)
  print("TOTAL RUN TIME: " + str(total_run_time) + " seconds")

############################################################

  # full_graph.create_benchmark_config_file(full_graph.benchmarks[0], full_graph.benchmarks[0].vms)

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

def create_benchmark_schedule(benchmark_graph):
  pass


def run_benchmarks(benchmark_graph):
  benchmark_graph.create_vms()
  benchmarks_run = []
  while benchmark_graph.benchmarks_left() > 0:
    maximum_set = list(benchmark_graph.maximum_matching())
    benchmarks_run.append(maximum_set)
    benchmark_graph.run_benchmark_set(maximum_set)
    # possibly check
    # Completion statuses can be found at: 
    # /tmp/perfkitbenchmarker/runs/7fab9158/completion_statuses.json
    # before removal of edges
    benchmark_graph.remove_orphaned_nodes()
    benchmark_graph.add_benchmarks_from_waitlist()
    print(benchmark_graph.benchmarks_left())
    time.sleep(2)

  print(len(benchmarks_run))
  print("BMS RUN EACH LOOP")
  for bmset in benchmarks_run:
    print(len(bmset))
    


def create_graph_from_config_list(benchmark_config_list):
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
                                              pkb_location="python /home/derek/projects/virt_center/pkb_autopilot_branch/PerfKitBenchmarker/pkb.py")

  # First pass, find all the regions and add them to the graph
  # config[0] is the benchmark_name
  # config[1] is all the flags

  # get all regions from gcloud
  # make regions
  region_dict = get_region_info()
  for key in region_dict:
    # if region['description'] in full_graph.regions
    new_region = Region(region_name=key,
                        cpu_quota=region_dict[key]['CPUS']['limit'],
                        usage=region_dict[key]['CPUS']['usage'])
    full_graph.add_region_if_not_exists(new_region=new_region)

  # This takes all the stuff from the config dictionaries
  # and puts them in benchmark objects
  # will need more logic for differently formatted configs
  benchmark_counter = 0
  temp_benchmarks = []
  for config in benchmark_config_list:
    # print(config[1]['flags']['zones'])
    region_name = config[1]['flags']['zones']
    # print(config[1]['flags']['extra_zones'])
    # full_graph.add_region_if_not_exists(region_name)
    new_benchmark = Benchmark(benchmark_id=benchmark_counter,
                              benchmark_type=config[0],
                              zone1=config[1]['flags']['zones'],
                              zone2=config[1]['flags']['extra_zones'],
                              machine_type=config[1]['flags']['machine_type'],
                              cloud=config[1]['flags']['cloud'],
                              flags=config[1]['flags'])
    temp_benchmarks.append(new_benchmark)
    benchmark_counter += 1

  logger.debug("Number of benchmarks: " + str(len(temp_benchmarks)))

  # create virtual machines (node)
  # attach with edges and benchmarks
  for bm in temp_benchmarks:
    if bm.zone1 != bm.zone2:
      cpu_count = cpu_count_from_machine_type(bm.cloud, bm.machine_type)
      logger.debug("Trying to add " + bm.zone1 + " and " + bm.zone2)

      success1, tmp_vm1 = full_graph.add_vm_if_possible(cpu_count=cpu_count,
                                                        zone=bm.zone1,
                                                        os_type=bm.os_type,
                                                        network_tier=bm.network_tier,
                                                        machine_type=bm.machine_type,
                                                        cloud=bm.cloud)

      success2, tmp_vm2 = full_graph.add_vm_if_possible(cpu_count=cpu_count,
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
        full_graph.benchmarks.append(bm)
        full_graph.add_benchmark(bm, tmp_vm1.node_id, tmp_vm2.node_id)
      else:
        logger.debug("BM WAITLISTED")
        bm.status = "Waitlist"
        full_graph.benchmark_wait_list.append(bm)

    else:
      logger.debug("VM 1 and VM 2 are the same zone")

  logger.debug("Number of benchmarks: " + str(len(full_graph.benchmarks)))


  # Second pass, add
  for config in benchmark_config_list:
    pass

  return full_graph


def cpu_count_from_machine_type(cloud, machine_type):
  if cloud == 'GCP':
    return int(machine_type.split('-')[2])
  elif cloud == 'AWS':
    return None
  elif cloud == 'Azure':
    return None
  else:
    return None


def get_region_info():
  region_list_command = "gcloud compute regions list --format=json"
  process = subprocess.Popen(region_list_command.split(),
                             stdout=subprocess.PIPE)
  output, error = process.communicate()

  # load json and convert to a more useable output
  region_json = json.loads(output)
  region_dict = {}
  for region_iter in region_json:
    region_dict[region_iter['description']] = {}
    for quota in region_iter['quotas']:
      region_dict[region_iter['description']][quota['metric']] = quota
      region_dict[region_iter['description']][quota['metric']].pop('metric', None)

      # print(region_dict['us-central1']['CPUS']['limit'])
      # print(region_dict['us-central1']['CPUS']['usage'])
  # print(region_dict['us-west2'])

  return region_dict


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
  """[summary]

  [description]

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

  print(yaml_contents.keys())
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
      print("FMF failed")
      continue
    benchmark_config_list.append((benchmark_name, config));
    # print("BENCHMARK CONFIG")
    # print(benchmark_config_list[0])

  # print("BENCHMARK CONFIG LIST")
  # for config in benchmark_config_list:
  #   print(config)

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