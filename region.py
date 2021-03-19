import subprocess
import json
class Region():
  """[summary]

  [description]
  """

  def __init__(self, region_name, cloud, cpu_quota=0.0, cpu_usage=0.0):
    self.cpu_quota = cpu_quota
    self.address_quota = None
    self.address_usage = 0
    self.cpu_usage = cpu_usage
    self.reserved_usage = 0.0
    self.virtual_machines = []
    self.name = region_name
    self.cloud = cloud

    if(cloud.lower() == "aws"):
      region_list_command = f"aws configure set region {region_name}"
      process = process = subprocess.Popen(region_list_command, stdout=subprocess.PIPE, shell=True)
      output, error = process.communicate()
      region_list_command = "aws ec2 describe-instances --query Reservations[].Instances[]"
      process = process = subprocess.Popen(region_list_command, stdout=subprocess.PIPE, shell=True)
      output, error = process.communicate()
      output = json.loads(output.decode('utf-8'))
      self.aws_vm_count = len(output)
      self.aws_vm_quota = 1920
      region_list_command = "aws ec2 describe-vpcs"
      process = process = subprocess.Popen(region_list_command, stdout=subprocess.PIPE, shell=True)
      output, error = process.communicate()
      output = json.loads(output.decode('utf-8'))
      self.aws_vpc_count = len(output)
      self.aws_vpc_quota = 5

  def get_available_cpus(self):
    return self.cpu_quota - self.cpu_usage

  def has_enough_cpus(self, cpu_count):
    return self.get_available_cpus() >= cpu_count 

  def has_enough_resources(self, cpu_count, cloud=0, region=0):
    #add a variable that updates each time thi method is run.
    # for every unique region a machine is in, a vpc is spun up.  
    if cloud == 'gcp':
      if (self.get_available_cpus() >= cpu_count 
          and self.address_quota > self.address_usage):
        return True
      else:
        return False
    elif cloud == 'aws':
      print(f"region is: {self.name}")
      # region_list_command = f"aws configure set region {self.name}"
      # process = process = subprocess.Popen(region_list_command, stdout=subprocess.PIPE, shell=True)
      # output, error = process.communicate()
      # region_list_command = "aws ec2 describe-instances --query Reservations[].Instances[]"
      # process = process = subprocess.Popen(region_list_command, stdout=subprocess.PIPE, shell=True)
      # output, error = process.communicate()
      # output = json.loads(output.decode('utf-8'))
      # print(f" output is, type: {type(output)}, length is: {len(output)}, and is {output} and error is {error} in has_enough_resources")
      #used to compare to the len(output) but because VM's are not created between this instance of the method and the next, it just gives the same results
      #print(f"\naws_quota_tracker: {aws_quota_tracker}\n")
      if self.aws_vm_count >= self.aws_vm_quota :
        return False
      # region_list_command = "aws ec2 describe-vpcs"
      # process = process = subprocess.Popen(region_list_command, stdout=subprocess.PIPE, shell=True)
      # output, error = process.communicate()
      # output = json.loads(output.decode('utf-8'))
      # print(f"The output is: {output}")
      if self.aws_vpc_count >= self.aws_vpc_quota :
        print(f"\n\n\n VPC LIMIT REACHED\n\n\n")
        return False
      self.aws_vm_count = self.aws_vm_count + 1
      self.aws_vpc_count = self.aws_vpc_count + 1
      return True

      

  def add_virtual_machine_if_possible(self, vm):
    print(f"\n\n in add_virtual_machine_if_possible: {vm.__dict__}\n\n")
    print(f"This tests to see if we have enough CPU's to run the tests: self.get_available_cpus():{self.get_available_cpus()} should be >= vm.cpu_count:{vm.cpu_count}")
    print(f"vm cloud is: {vm.cloud}")
    #print(breadk[99])
    if vm.cloud == 'gcp' or vm.cloud == 'GCP':
      if (self.get_available_cpus() >= vm.cpu_count 
          and self.address_quota > self.address_usage):
        self.virtual_machines.append(vm)
        self.cpu_usage += vm.cpu_count
        self.address_usage += 1
        print("CPU USAGE: " + str(self.cpu_usage) + " QUOTA: " + str(self.cpu_quota))
        print("ADDR USAGE: " + str(self.address_usage) + " QUOTA: " + str(self.address_quota))
        return True
      else:
        print("Quota reached for region: " + self.name)
        return False
    elif vm.cloud == 'aws' or vm.cloud == 'AWS':
      #here we want to see the number of machines we have up
      if vm.vm_aws_limit > vm.vm_spun_up_machines:
        self.virtual_machines.append(vm)
        self.cpu_usage += vm.cpu_count
        self.address_usage += 1
        return True
      else:
        return False
    return False
  def remove_virtual_machine(self, vm):
    # TODO add safety checks here
    print(f"self is {self.__dict__}")
    print(f"\n\nvm is {vm.__dict__}")
    if len(self.virtual_machines) > 0 :
      self.virtual_machines.remove(vm)
    self.cpu_usage -= vm.cpu_count
    self.address_usage -= 1

    if self.cpu_usage < 0:
      self.cpu_usage = 0
    if self.address_usage < 0:
      self.address_usage = 0

  def update_cpu_quota(self, quota):
    self.cpu_quota = quota

  def update_cpu_usage(self, usage):
    if usage <= self.cpu_quota:
      self.cpu_usage = usage
      return True
    else:
      return False

  def update_address_quota(self, quota):
    self.address_quota = quota

  def update_address_usage(self, usage):
    if usage <= self.address_quota:
      self.address_usage = usage
      return True
    else:
      return False