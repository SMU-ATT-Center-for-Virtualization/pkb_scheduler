
import subprocess
import json
import re

# TODO support other clouds


def cpu_count_from_machine_type(cloud, machine_type):

  print("Cloud is set to: {}".format(cloud))
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
        multiplier = int(re.findall(r'\d+', machine_size))
        cpu_count = 4 * multiplier

    elif 't' in machine_category.lower():
      cpu_count = None
    return cpu_count

  elif cloud == 'Azure':
    return None
  else:
    return None


def get_region_info(cloud):

  region_dict = {}
  if cloud == 'GCP':
    region_list_command = "gcloud compute regions list --format=json"
    process = subprocess.Popen(region_list_command.split(),
                               stdout=subprocess.PIPE)
    output, error = process.communicate()
    #print("output: {}".format(str(output)))
    # load json and convert to a more useable output
    region_json = json.loads(output.decode('utf-8'))
    for region_iter in region_json:
      region_dict[region_iter['description']] = {}
      for quota in region_iter['quotas']:
        region_dict[region_iter['description']][quota['metric']] = quota
        region_dict[region_iter['description']][quota['metric']].pop('metric', None)

  elif cloud == 'AWS':
    region_list_command = 'aws ec2 describe-regions'
    process = subprocess.Popen(region_list_command.split(),
                               stdout=subprocess.PIPE)
    output, error = process.communicate()
    # load json and convert to a more useable output
    region_json = json.loads(output)
    for region_iter in region_json['Regions']:
      region_dict[region_iter['RegionName']] = {}
  else:
    pass

  return region_dict


def get_cloud_quotas(cloud):
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
    quota_json = json.loads(output)
    for quota_iter in quota_json['AccountAttributes']:
      if quota_iter['AttributeName'] == 'max-instances':
        quota_dict['instance_quota'] = quota_iter['AttributeValues'][0]['AttributeValue']
      elif quota_iter['AttributeName'] == 'max-elastic-ips':
        quota_dict['static_address_quota'] = quota_iter['AttributeValues'][0]['AttributeValue']

  else:
    pass

  return region_dict


def get_region_from_zone(cloud, zone):
  if cloud == 'GCP':
    return zone[:len(zone) - 2]
  elif cloud == 'AWS':
    return zone[:len(zone) - 1]
  elif cloud == 'Azure':
    return zone
  else:
    return None
