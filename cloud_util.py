import subprocess
import json
import re
import logging
from typing import List, Dict, Tuple, Set, Any, Sequence, Optional

# TODO add support for additional clouds


def cpu_count_from_machine_type(cloud: str, machine_type: str) -> int:
  """Given a cloud and a machine type, return the associated cpu count
  
  Args:
      cloud (str): name of cloud provider
      machine_type (str): name of machine type
  
  Returns:
      int: cpu count
  """
  if cloud == 'GCP':
    return int(machine_type.split('-')[2])
  elif cloud == 'AWS':
    machine_array = machine_type.split('.')
    machine_category = machine_array[0]
    machine_size = machine_array[1].lower()
    cpu_count = None
    if 'm' in machine_category.lower():
      if machine_size == 'large':
        cpu_count = 2
      elif machine_size == 'xlarge':
        cpu_count = 4
      elif 'xlarge' in machine_size:
        print(machine_size)
        multiplier = int(re.findall(r'\d+', machine_size)[0])
        cpu_count = 4 * multiplier
    elif 't2' in machine_category.lower():
      if 'micro' in machine_size.lower():
        cpu_count = 1

    return cpu_count

  # Troy. Only bother editing this if Azure has CPU quotas we need to track
  #Troy Update: We need to keep track of vCPU's. Additionally, we need to track total regional vCPU's
  elif cloud.upper() == 'AZURE':
    cpu_count = None
    if machine_type == 'D2s_v3':
      cpu_count = 2
    elif machine_type == 'Standard_D2s_v3':
      cpu_count = 2
    else:
      try:
        cpu_count = int(machine_type[1])
      except:
        print(f"Error when trying to get cpu_count of virtual machine.")
    return cpu_count
  else:
    return None


def cpu_type_from_machine_type(cloud: str, machine_type: str) -> str:
  """Given a cloud and a machine type, return the associated cpu type
  
  Args:
      cloud (str): name of cloud provider
      machine_type (str): name of machine type
  
  Returns:
      str: cpu type
  """
  if cloud == 'GCP':
    return machine_type.split('-')[0]
  elif cloud == 'AWS':
    return machine_type.split('.')[0]
  elif cloud.upper() == 'AZURE':
    return None
  else:
    return None


def get_region_info(cloud: str):
  """get quota info for all regions in a specified cloud
  
  Args:
    cloud: string in ['AWS','GCP','AZURE']
  
  Returns:
    region_dict: dictionary containing quota info about each region in a cloud
                 key: region-name, value: dictionary with quota info
  """
  region_dict = {}
  if cloud == 'GCP':
    logging.info("Querying data from gcloud")
    region_list_command = "gcloud compute regions list --format=json"
    process = subprocess.Popen(region_list_command.split(),
                               stdout=subprocess.PIPE)
    output, error = process.communicate()

    # load json and convert to a more useable output
    region_json = json.loads(output)
    for region_iter in region_json:
      region_dict[region_iter['description']] = {}
      for quota in region_iter['quotas']:
        region_dict[region_iter['description']][quota['metric']] = quota
        region_dict[region_iter['description']][quota['metric']].pop('metric', None)

    return region_dict
  elif cloud == 'AWS':
    logging.info("Querying data from AWS")
    # Get list of all AWS regions
    region_list_command = 'aws ec2 describe-regions'
    process = subprocess.Popen(region_list_command.split(),
                               stdout=subprocess.PIPE)
    output, error = process.communicate()
    # load json and convert to a more useable output
    region_json = json.loads((output.decode('utf-8')))

    # Get current VPC and VM usage info for each AWS region
    for region_iter in region_json['Regions']:
      region_name = region_iter['RegionName']
      region_dict[region_name] = {}

      # region_list_command = f"aws configure set region {region_name}"
      # process = process = subprocess.Popen(region_list_command, stdout=subprocess.PIPE, shell=True)
      # output, error = process.communicate()

      region_list_command = f"aws ec2 describe-instances --query Reservations[].Instances[] --region={region_name}"
      process = process = subprocess.Popen(region_list_command, stdout=subprocess.PIPE, shell=True)
      output, error = process.communicate()
      output = json.loads(output.decode('utf-8'))

      region_dict[region_name]['vm'] ={}
      region_dict[region_name]['vm']['limit'] = 1920
      region_dict[region_name]['vm']['usage'] = len(output)

      region_list_command = f"aws ec2 describe-vpcs --region={region_name}"
      process = process = subprocess.Popen(region_list_command, stdout=subprocess.PIPE, shell=True)
      output, error = process.communicate()
      output = json.loads(output.decode('utf-8'))

      region_dict[region_name]['vpc'] ={}
      region_dict[region_name]['vpc']['limit'] = 5
      region_dict[region_name]['vpc']['usage'] = len(output['Vpcs'])

      region_list_command = f"aws ec2 describe-addresses --region={region_name}"
      process = process = subprocess.Popen(region_list_command, stdout=subprocess.PIPE, shell=True)
      output, error = process.communicate()
      output = json.loads(output.decode('utf-8'))

      region_dict[region_name]['elastic_ip'] ={}
      region_dict[region_name]['elastic_ip']['limit'] = 5
      region_dict[region_name]['elastic_ip']['usage'] = len(output['Addresses'])

    return region_dict
  elif cloud.upper() == "AZURE":
    # Troy PUT CODE HERE
    # fetch quota data from az cli (NOTE: there are two versions of azure cli. Use the newer version that uses the command 'az')
    # figure out important quota data per region, if quotas are for the whole cloud instead of each region, we'll need to
    # do something else

    # region_dict should have the following structure (or something similar):
    #
    # region_dict = {'region1': {'quota1': {'limit': 10, 
    #                                        'usage': 3}, 
    #                             'quota2': {'limit': 50,
    #                                        'usage': 30}
    #                            }
    #                'region2': {'quota1': {'limit': 10, 
    #                                       'usage': 3}, 
    #                            'quota2': {'limit': 50,
    #                                       'usage': 30}
    #                           }
    #               }
    # basically its dictionaries all the way down
    region_list_command = 'az account list-locations'
    process = subprocess.Popen(region_list_command.split(),
                               stdout=subprocess.PIPE)
    output, error = process.communicate()
    # load json and convert to a more useable output
    region_json = json.loads((output.decode('utf-8')))
     
    for region_iter in region_json:
      try:
        #print(f"region_iter is: {region_iter}")
        # if region_iter['metadata']['regionCategory'] != 'Recommended':
        #   continue
        region_name = region_iter['name']
        region_dict[region_name] = {"region_name" : region_name}
        region_list_command = f'az vm list-usage --location "{region_name}"'
        #print(f"region list commmand is: {region_list_command}")
        process = process = subprocess.Popen(region_list_command, stdout=subprocess.PIPE, shell=True)
        output, error = process.communicate()
        #print(f"region diect: {region_dict}")
        output = json.loads(output.decode('utf-8'))
        #print(f"region_list_command is: {output}")
        #print(f"Pre For Loop")
        for quota_iter in output:
          #print(f" variable is  and the result is: {quota_iter['currentValue']} and  {quota_iter['limit']}")
          quotaName = quota_iter["localName"]
          #print(f"region_dict is : {region_dict} \n\n quota name is: {quotaName}")
          region_dict[region_name][quotaName.upper()] = [int(quota_iter["currentValue"]), int(quota_iter["limit"])]
        #az network list-usages --location eastus --out table
        region_list_command = f'az network list-usages --location "{region_name}"'
        #print(f"region list commmand is: {region_list_command}")
        #Public IP Addresses - Basic
        process = process = subprocess.Popen(region_list_command, stdout=subprocess.PIPE, shell=True)
        output, error = process.communicate()
        #print(f"region diect: {region_dict}")
        output = json.loads(output.decode('utf-8'))
        #print(f"region_list_command is: {output}")
        #print(f"Pre For Loop")
        for quota_iter in output:
          #print(f" variable is  and the result is: {quota_iter['currentValue']} and  {quota_iter['limit']}")
          quotaName = quota_iter["localName"]
          #print(f"region_dict is : {region_dict} \n\n quota name is: {quotaName}")
          region_dict[region_name][quotaName.upper()] = [int(quota_iter["currentValue"]), int(quota_iter["limit"])]
        #print(f"the region dict should be: {region_dict[region_name]}")
      except:
        print(f"Error occurred when reading in quotas. Region was {region_name}")
    print(f"Region Dict: {region_dict}")

    return region_dict
  else:
    pass

  return region_dict


def get_cloud_quotas(cloud: str) -> Dict[str,Any]:
  """Get cloud-wide quotas for each cloud service
  
  Args:
      cloud (str): Name of cloud provider
  
  Returns:
      Dict[str, Any]: dictionary of quotas
  """
  quota_dict = {}
  if cloud == 'GCP':
    quota_list_command = "gcloud compute project-info describe --format=json"
    process = subprocess.Popen(quota_list_command.split(),
                               stdout=subprocess.PIPE)
    output, error = process.communicate()

    # load json and convert to a more useable output
    quota_json = json.loads(output)
    for quota_iter in quota_json['quotas']:
      if quota_iter['metric'] == 'max-instances':
        quota_dict['instance_quota'] = None
      elif quota_iter['metric'] == 'STATIC_ADDRESSES':
        quota_dict['static_address_quota'] = quota_iter['limit']

  elif cloud == 'AWS':
    quota_list_command = 'aws ec2 describe-account-attributes'
    process = subprocess.Popen(quota_list_command.split(),
                               stdout=subprocess.PIPE)
    output, error = process.communicate()
    # load json and convert to a more useable output
    quota_json = json.loads(output.decode('utf-8'))
    for quota_iter in quota_json['AccountAttributes']:
      if quota_iter['AttributeName'] == 'max-instances':
        quota_dict['instance_quota'] = quota_iter['AttributeValues'][0]['AttributeValue']
      elif quota_iter['AttributeName'] == 'max-elastic-ips':
        quota_dict['static_address_quota'] = quota_iter['AttributeValues'][0]['AttributeValue']

  elif cloud.upper() == 'AZURE':
    # Troy, if quotas are cloud-wide, get the info here and put it in quota_dict
    pass

  return quota_dict


def get_region_from_zone(cloud: str, zone: str) -> Optional[str]:
  """Given a cloud and a zone, returns the region for the zone
  
  Args:
      cloud (str): cloud provider name
      zone (str): availability zone name
  
  Returns:
      Optional[str]: name of region, or None if not found
  """
  if cloud == 'GCP':
    return zone[:len(zone) - 2]
  elif cloud == 'AWS':
    zone_split = zone.split('-')
    if len(zone_split) != 3:
      logging.warn('Improperly formatted AWS zone:' + zone + ' This may cause errors.')
      return zone
    else:
      # TODO change to regex to look for number followed by a letter
      if len(zone_split[2]) > 1:
        # strip off zone letter  us-east-1a -> us-east-1
        return zone[:len(zone) - 1]
      else:
        # return zone as it is. us-east-1
        return zone
  elif cloud.upper() == 'AZURE':
    # TODO Troy, this may be all thats needed. Regions and zones have less distinction in azure, but i forget the specifics
    
    return zone
  else:
    return None


def get_max_bandwidth_from_machine_type(cloud: str, machine_type: str) -> int:
  """Given a cloud and machine type, returns the maximum bandwidth for that machine type
  
  Args:
      cloud (str): Cloud name
      machine_type (str): machine type name
  
  Returns:
      int: bandwidth in Gbps
  """
  if cloud == 'GCP':
    machine_type = machine_type.lower()
    cpu_type = cpu_type_from_machine_type('GCP', machine_type).upper()
    cpu_count = cpu_count_from_machine_type('GCP', machine_type)
    if cpu_type in ['N1', 'N2', 'N2D']:
      if cpu_count == 1:
        return 2
      elif cpu_count <= 5:
        return 10
      elif cpu_count < 16:
        return 2 * cpu_count
      elif cpu_count >= 16:
        return 32
    elif cpu_type in ['E2']:
      return -1 #TODO

  # Troy, I'll take care of this later
  elif cloud == 'AWS':
    return -1

  elif cloud.upper() == 'AZURE':
    return -1
