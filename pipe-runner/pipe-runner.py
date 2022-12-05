import yaml
import logging
import time

print ('hello from pipe-runner')

config_file = '/wsefs/config/ssebop-launcher.conf'

with open(config_file, "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

print(cfg)


logdir = cfg['log']
filename = f'{logdir}/pipe-runner.log'
logging.basicConfig(filename=filename, format='%(asctime)s - %(message)s', level=logging.INFO)

while True:
    logging.info('pipe-runner ran and thats good! ')
    time.sleep(360)



