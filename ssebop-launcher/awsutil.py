import subprocess

def ec2status():
   print ("hello from util")
   command = 'aws ec2 describe-instances'
   process = subprocess.Popen(command,stdout=subprocess.PIPE, shell=True)
   proc_stdout = process.communicate()[0].strip()
   stupidBytesObject = proc_stdout
   outStr = (stupidBytesObject.decode("utf-8"))
   #print(outStr)
   return(outStr)

def ec2start(id):
   print ("start id %s " % id)
   command = "aws ec2 start-instances --instance-ids %s" % id
   process = subprocess.Popen(command,stdout=subprocess.PIPE, shell=True)
   proc_stdout = process.communicate()[0].strip()
   stupidBytesObject = proc_stdout
   outStr = (stupidBytesObject.decode("utf-8"))
   print(outStr)
   return(outStr)

def ec2stop(id):
   print ("stop id %s " % id)
   command = "aws ec2 stop-instances --instance-ids %s" % id
   process = subprocess.Popen(command,stdout=subprocess.PIPE, shell=True)
   proc_stdout = process.communicate()[0].strip()
   stupidBytesObject = proc_stdout
   outStr = (stupidBytesObject.decode("utf-8"))
   print(outStr)
   return(outStr)


def get_tag_name(theId):

    tagName='BOGUS1'
    if 'Tags' in theId:
             tags = theId["Tags"]
             for tg in tags:
                if tg["Key"] == "Name":
                    tagName = tg["Value"]
    else:
             tagName='BOGUS'

    return(tagName)

