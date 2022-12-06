import yaml
import logging
import time
import os
import docker

def start_docker(step):
    global logging
    logging.info(f'start_docker:{step}')
    volumes = ['/wsefs:/wsefs',]
    docker_client = docker.from_env()
    cmd = f'python3 {step}.py'
    docker_image = step
    container_name = step
    #container_obj = docker_client.containers.run(docker_image, cmd, detach=True, name=container_name)
    container_obj = docker_client.containers.run(docker_image, cmd, detach=False, 
                                                 volumes=volumes, auto_remove=True, name=container_name)
    logging.info(f'{cmd} started {container_name} {docker_image} {container_obj.name}')

def dequeue_work(work_order_file):
    os.unlink(work_order_file)

def read_wo(wo_file):
    with open(wo_file, "r") as my_f:
        work_list = yaml.safe_load(my_f)
    print(work_list)
    return work_list

def do_work(wo_file):
    global logging
    logging.info(f'do_work: wo_file: {wo_file}')
    wl = read_wo(wo_file)
    logging.info(f'do_work: list {wl}')
    dequeue_work(wo_file)
    start_docker(wl[0])
    start_docker(wl[1])

def look_for_work(cfg, logging):
    todo_dir = cfg['todo']
    wo_files = os.listdir(todo_dir)
    if not wo_files:
        logging.info(f'no work returning now')
        return False
    my_work = f'{todo_dir}/{wo_files[0]}'
    logging.info(f'working on {my_work}')
    do_work(my_work)
    return True

print ('hello from pipe-runner')

config_file = '/wsefs/config/ssebop-launcher.conf'

with open(config_file, "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

print(cfg)


global logging
logdir = cfg['log']
filename = f'{logdir}/pipe-runner.log'
logging.basicConfig(filename=filename, format='%(asctime)s - %(message)s', level=logging.INFO)

while True:
    secs=40
    look_for_work(cfg, logging)
    logging.info(f'pipe-runner wait {secs} and look for work orders! ')
    time.sleep(secs)



