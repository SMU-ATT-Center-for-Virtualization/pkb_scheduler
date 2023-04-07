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
    self.name = meta_region_name
    self.cloud = cloud
    self.bandwidth_limit = bandwidth_limit
    self.regions = []

  def has_enough_resources(self, estimated_bandwidth) -> bool:
    if self.bandwidth_limit is None:
      return True
    bandwidth_sum = estimated_bandwidth
    for region in self.regions:
      bandwidth_sum += region.bandwidth_usage

    print(f"{bandwidth_sum} exceed meta-region limit")
    return (bandwidth_sum <= self.bandwidth_limit)

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