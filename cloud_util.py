
import subprocess
import json

# TODO support other clouds


def cpu_count_from_machine_type(cloud, machine_type):
  if cloud == 'GCP':
    return int(machine_type.split('-')[2])
  elif cloud == 'AWS':
    return None
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

    # load json and convert to a more useable output
    region_json = json.loads(output)
    for region_iter in region_json:
      region_dict[region_iter['description']] = {}
      for quota in region_iter['quotas']:
        region_dict[region_iter['description']][quota['metric']] = quota
        region_dict[region_iter['description']][quota['metric']].pop('metric', None)
  else:
    pass

  return region_dict


def get_region_from_zone(cloud, zone):
  if cloud == 'GCP':
    return zone[:len(zone) - 2]
  else:
    return None
