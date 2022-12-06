from datetime import datetime
import yaml
import json
import time

import awsutil 

def start(tag):
   """ starts an instance """
   jsonData = awsutil.ec2status()
   jsonBlob = json.loads(jsonData)
   mynew = jsonBlob.get("Reservations")

   for myDict in mynew:
      ids = myDict["Instances"]
      for id in ids:
         tagName = awsutil.get_tag_name(id)

         instanceId = id["InstanceId"]
         iState = id["State"]
         if (tagName == tag):
            print ("The INSTANCE %s is %s " % (tagName, iState["Name"]))
            print ("Starting instance ID %s " % instanceId)
            print("START YOUR Engines %s" % tag)
            awsutil.ec2start(id=instanceId)

def stop(tag):
   """ stops an instance """
   jsonData = awsutil.ec2status()
   jsonBlob = json.loads(jsonData)
   mynew = jsonBlob.get("Reservations")

   for myDict in mynew:
      ids = myDict["Instances"]
      for id in ids:
         #print (id["InstanceType"])
         #print (id["InstanceId"])
         instanceId = id["InstanceId"]
         tagName = awsutil.get_tag_name(id)

         instanceId = id["InstanceId"]
         iState = id["State"]
         if (tagName == tag):
            print ("Stopping instance ID %s " % instanceId)
            print ("The INSTANCE %s is %s " % (tagName, iState["Name"]))
            print ("KILL YOUR Engines %s" % tag)
            awsutil.ec2stop(id=instanceId)


def create_workorder(cfg):
    print(cfg['steps'])
    my_day = datetime.today().strftime('%Y-%m-%d')
    wo_name = f'ssebop-{my_day}'
    todo = cfg['todo']
    wo_file_name = f'{todo}/{wo_name}.todo'
    my_yaml = yaml.dump(cfg['steps'], default_flow_style=False)
    print(my_yaml)
    with open(wo_file_name, 'w') as file:
        file.write(my_yaml)
    return(wo_file_name) # will be name of workorder probaly with date 

def babysit_pipeline(workorder):
    print(workorder)
    print('sleeping ...')
    time.sleep(1)



config_file = '/wsefs/config/ssebop-launcher.conf'

with open(config_file, "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

print(cfg)


status = start(cfg['computeMachine'])

workorder = create_workorder(cfg)

babysit_pipeline(workorder) # blocks

# status = stop(cfg['computeMachine'])


