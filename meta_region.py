import cloud_util
import re
import copy
from typing import List, Dict, Tuple, Set, Any, Sequence, Optional
from cloud import Cloud
from region import Region
from virtual_machine import VirtualMachine

class MetaRegion():

  def __init__(self, 
               meta_region_name: str, 
               cloud: str,
               bandwidth_limit: Optional[int] = None):
    self.bandwidth_limit = bandwidth_limit
    self.bandwidth_usage = 0

class GcpMetaRegion(MetaRegion):
  def __init__(self, meta_region_name, cloud, quotas, bandwidth_limit=None):
    self.quotas = quotas
    MetaRegion.__init__(self, meta_region_name, cloud, bandwidth_limit=bandwidth_limit)


class AwsMetaRegion(MetaRegion):
  def __init__(self, meta_region_name, cloud, quotas, bandwidth_limit=None):
    self.quotas = quotas
    MetaRegion.__init__(self, meta_region_name, cloud, bandwidth_limit=bandwidth_limit)  


class AzureMetaRegion(MetaRegion):
  def __init__(self, meta_region_name, cloud, quotas, bandwidth_limit=None):
    self.quotas = quotas
    MetaRegion.__init__(self, meta_region_name, cloud, bandwidth_limit=bandwidth_limit)