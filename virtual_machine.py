import subprocess
import time
from absl import flags
import logging

from virtual_machine_spec import VirtualMachineSpec


FLAGS = flags.FLAGS
logger = None


class VirtualMachine():
  """[summary]

  [description]
  """

  def __init__(self, node_id, cpu_count, zone, os_type=None, machine_type=None,
               cloud=None, network_tier=None, vpn=False, vpn_gateway_count=0,
               vpn_tunnel_count=0, ssh_private_key=None, ssl_cert=None,
               vm_spec=None, vm_spec_id=None, min_cpu_platform=None,vm_aws_limit = 0,
                          vm_spun_up_machines = 0:

    # get logger
    global logger
    logger = logging.getLogger('pkb_scheduler')

    self.node_id = node_id
    self.cpu_count = cpu_count
    self.os_type = os_type
    self.zone = zone
    self.machine_type = machine_type
    self.cloud = cloud
    self.network_tier = network_tier
    self.vpn = vpn
    self.ssh_private_key = ssh_private_key
    self.ssl_cert = ssl_cert
    self.status = 'Not Created'
    self.min_cpu_platform = min_cpu_platform
    self.internal_ip = None
    self.ip_address = None
    self.name = None
    self.run_uri = None
    self.uid = None
    self.creation_output = ""
    self.create_timestamp = None
    self.delete_timestamp = None
    self.creation_time = None
    self.deletion_time = None
    # TODO use this instead of static network name
    self.network_name = None
    # self.ip_address = None
    self.vm_spec = vm_spec
    self.vm_spec_id = vm_spec_id
    self.password = None

  def vm_spec_is_equivalent(self, vm):
    """Returns true if the spec of a vm that is
       passed in is equivalent to this VM
    """
    if (self.cloud == vm.cloud and
        self.zone == vm.zone and
        self.machine_type == vm.machine_type and
        self.network_tier == vm.network_tier and
        self.vpn == vm.vpn and
        self.os_type == vm.os_type):
      return True

    return False

  def uptime(self):
    if self.status == "Running":
      current_time = time.time()
      return current_time - self.create_timestamp

    elif self.status == "Shutdown":
      return self.delete_timestamp - self.create_timestamp

    else:
      return 0

  def create_instance(self, pkb_location):
    """Creates a VM on the cloud from a VM object

    Creates a VM on the cloud from a VM object
    Currently only works for GCP VMs
    TODO add AWS and Azure

    Args:
      vm: [description]
    """
    # TODO make this more robust
    if self.status == "Running":
      return (False, self.status)

    cmd = (pkb_location)

    if 'windows' in self.os_type:
      cmd = cmd + " --benchmarks=vm_setup_windows"
    else:
      cmd = cmd + " --benchmarks=vm_setup"

    cmd = (cmd +
           " --gce_network_name=pkb-scheduler" +
           " --gcp_min_cpu_platform=" + self.min_cpu_platform +
           " --ssh_key_file=" + self.ssh_private_key +
           " --ssl_cert_file=" + self.ssl_cert +
           " --zones=" + self.zone +
           " --os_type=" + self.os_type +
           " --machine_type=" + self.machine_type +
           " --cloud=" + self.cloud +
           " --gce_network_tier=" + self.network_tier +
           " --run_stage=provision,prepare" +
           " --gce_remote_access_firewall_rule=allow-ssh" +
           " --ignore_package_requirements=True")

    if FLAGS.no_run:
      print("CREATE INSTANCE: " + cmd)
      self.run_uri = "no_run"
      self.uid = "no_run"
      self.ip_address = "9.9.9.9"
      self.internal_ip = "172.0.0.1"
      self.name = "no run"
      self.status = "Running"
      self.create_timestamp = time.time()
      return (True, self.status)

    start_time = time.time()
    self.create_timestamp = time.time()

    process = subprocess.Popen(cmd.split(),
                               stdout=subprocess.PIPE)
    output, error = process.communicate()

    end_time = time.time()
    self.creation_time = end_time - start_time

    logging.debug("PARSING OUTPUT")
    output = output.decode("utf-8")
    logging.debug(output)
    ext_ip = ""
    int_ip = ""
    name = ""
    password = ""

    self.creation_output = output
    # info_section_found = False
    time.sleep(1)

    # have to use this instead of line in output
    # because that splits by letter when threading?
    for line in self.creation_output.split('\n'):
      if "INTERNAL_IP:" in line:
        int_ip = line.split()[1]
      elif "EXTERNAL_IP:" in line:
        ext_ip = line.split()[1]
      elif "NAME:" in line:
        name = line.split()[1]
      elif "RUN_URI" in line:
        self.run_uri = line.split()[1]
      elif "UID" in line:
        self.uid = line.split()[1]
      # only for windows VMs
      elif "PASSWORD" in line:
        self.password = line.split()[1]

    if self.password:
      print("THIS IS THE PASSWORD: " + self.password)
    else:
      print("NO PASSWORD FOUND")


    if self.run_uri is None:
      print("INFO SECTION NOT FOUND")
      print("CREATION OUTPUT: ")
      print(self.creation_output)
      return (False, self.status)

    self.ip_address = ext_ip
    self.internal_ip = int_ip
    self.name = name
    self.status = "Running"

    return (True, self.status)

  def delete_instance(self, pkb_location):
    """Deletes an existing vm instance on the cloud

       Deletes an existing vm instances using the run_uri
       of that vm and PKB's run_stage functionality

    Args:
      vm: [description]
    """
    # TODO make this more robust
    if self.status == "Not Created" or self.status == "Shutdown":
      return (False, self.status)

    # ./pkb.py --benchmarks=vm_setup --gce_network_name=pkb-scheduler
    # --run_stage=cleanup,teardown --run_uri=074af5cd

    # TODO make the network a parameter
    print("DELETING VM INSTANCE")

    cmd = (pkb_location)
    if 'windows' in self.os_type:
      cmd = (cmd + " --benchmarks=vm_setup_windows" +
             " --os_type=" + self.os_type)
    else:
      cmd = cmd + " --benchmarks=vm_setup"

    cmd = (cmd +
           " --gce_network_name=pkb-scheduler" +
           " --cloud=" + self.cloud +
           " --run_uri=" + self.run_uri +
           " --run_stage=cleanup,teardown" +
           " --ignore_package_requirements=True")

    if FLAGS.no_run:
      logging.debug("DELETING INSTANCE: " + cmd)
      self.status = "Shutdown"
      self.delete_timestamp = time.time()
      return (True, self.status)

    start_time = time.time()

    process = subprocess.Popen(cmd.split(),
                               stdout=subprocess.PIPE)
    output, error = process.communicate()

    end_time = time.time()
    self.delete_timestamp = time.time()
    self.deletion_time = end_time - start_time
    self.status = "Shutdown"

    return (True, self.status)

  def copy_contents(self, vm):
    self.node_id = vm.node_id
    self.cpu_count = vm.cpu_count
    self.os_type = vm.os_type
    self.zone = vm.zone
    self.machine_type = vm.machine_type
    self.cloud = vm.cloud
    self.network_tier = vm.network_tier
    self.vpn = vm.vpn
    self.ssh_private_key = vm.ssh_private_key
    self.ssl_cert = vm.ssl_cert
    self.status = vm.status
    self.internal_ip = vm.internal_ip
    self.ip_address = vm.ip_address
    self.name = vm.name
    self.run_uri = vm.run_uri
    self.uid = vm.uid
    self.password = vm.password
    self.creation_output = vm.creation_output
    self.creation_time = vm.creation_time
    self.deletion_time = vm.deletion_time
    self.network_name = vm.network_name
    self.create_timestamp = vm.create_timestamp
    self.delete_timestamp = vm.delete_timestamp

    # TODO, do something like this instead
    # vm2.__dict__ = vm1.__dict__.copy()
    # or this
    # destination.__dict__.update(source.__dict__).
