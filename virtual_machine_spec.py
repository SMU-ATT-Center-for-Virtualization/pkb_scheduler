from absl import flags
import logging


FLAGS = flags.FLAGS
logger = None


class VirtualMachineSpec():
  """[summary]

  [description]
  """

  def __init__(self, cpu_count, zone, uid=None, os_type='ubuntu1804', machine_type=None,
               cloud=None, network_tier=None, vpn=False, vpn_gateway_count=0,
               vpn_tunnel_count=0):

    # get logger
    global logger
    logger = logging.getLogger('pkb_scheduler')
    self.id = uid
    self.cpu_count = cpu_count
    self.os_type = os_type
    self.zone = zone
    self.machine_type = machine_type
    self.cloud = cloud
    self.network_tier = network_tier
    self.vpn = vpn
    # TODO use this instead of static network name
    self.network_name = None
    # self.ip_address = None

  def vm_spec_is_equivalent(self, vm_spec):
    """Returns true if the spec of a vm that is
       passed in is equivalent to this VM
    """
    if (self.cloud == vm_spec.cloud and
        self.zone == vm_spec.zone and
        self.machine_type == vm_spec.machine_type and
        self.network_tier == vm_spec.network_tier and
        self.vpn == vm_spec.vpn and
        self.os_type == vm_spec.os_type):
      return True

    return False

  def copy_contents(self, vm):
    self.cpu_count = vm.cpu_count
    self.os_type = vm.os_type
    self.zone = vm.zone
    self.machine_type = vm.machine_type
    self.cloud = vm.cloud
    self.network_tier = vm.network_tier
    self.vpn = vm.vpn
    self.network_name = vm.network_name

    # TODO, do something like this instead
    # vm2.__dict__ = vm1.__dict__.copy()
    # or this
    # destination.__dict__.update(source.__dict__).
