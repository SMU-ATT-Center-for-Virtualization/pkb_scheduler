import subprocess
import time
from absl import flags
import logging


FLAGS = flags.FLAGS
logger = None

class VirtualMachine():
  """[summary]
  
  [description]
  """

  def __init__(self, node_id, cpu_count, zone, os_type=None, machine_type=None, cloud=None, 
               network_tier=None, vpn=False, vpn_gateway_count=0, vpn_tunnel_count=0,
               ssh_private_key=None, ssl_cert=None):   

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
    self.internal_ip = None
    self.ip_address = None
    self.name = None
    self.run_uri = None
    self.uid = None
    self.creation_output = ""

    #TODO use this instead of static network name
    self.network_name = None
    # self.ip_address = None

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

    cmd = (pkb_location + " --benchmarks=vm_setup"
            + " --gce_network_name=pkb-scheduler"
            + " --ssh_key_file=" + self.ssh_private_key
            + " --ssl_cert_file=" + self.ssl_cert
            + " --zones=" + self.zone
            + " --os_type=" + self.os_type
            + " --machine_type=" + self.machine_type
            + " --cloud=" + self.cloud
            + " --gce_network_tier=" + self.network_tier
            + " --run_stage=provision,prepare"
            + " --gce_remote_access_firewall_rule=allow-ssh")

    if FLAGS.no_run:
      print("CREATE INSTANCE: " + cmd)
      self.run_uri = "no_run"
      self.uid = "no_run"
      self.ip_address = "9.9.9.9"
      self.internal_ip = "172.0.0.1"
      self.name = "no run"
      self.status = "Running"
      return (True, self.status)


    process = subprocess.Popen(cmd.split(),
                             stdout=subprocess.PIPE)
    output, error = process.communicate()



    print("PARSING OUTPUT")
    output = output.decode("utf-8")
    print(output)
    ext_ip = ""
    int_ip = ""
    name = ""

    self.creation_output = output
    # info_section_found = False
    time.sleep(5)

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

    # TODO add in logic if we cant create the VM
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


  #TODO, for delete instance store run uri on create
  #      continue run of that run uri
  def delete_instance(self, pkb_location):
    """Deletes an existing vm instance on the cloud
    
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
    cmd = (pkb_location + " --benchmarks=vm_setup"
            + " --gce_network_name=pkb-scheduler"
            + " --cloud=" + self.cloud
            + " --run_uri=" + self.run_uri
            + " --run_stage=cleanup,teardown")

    if FLAGS.no_run:
      print("DELETING INSTANCE: " + cmd)
      self.status = "Shutdown"
      return (True, self.status)

    process = subprocess.Popen(cmd.split(),
                             stdout=subprocess.PIPE)
    output, error = process.communicate()

    self.status = "Shutdown"

    return (True, self.status)
