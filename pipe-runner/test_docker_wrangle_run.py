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
    container_obj = docker_client.containers.run(docker_image, cmd, volumes=volumes,
                                                 detach=False, auto_remove=True, name=container_name)
    logging.info(f'{cmd} started {container_name} {docker_image} {container_obj.name}')


global logging
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)



start_docker('wrangle')


