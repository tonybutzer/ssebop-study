#!/usr/bin/env python
# coding: utf-8

import os
import logging
import yaml
import time

class eta:
    def __init__(self, config_file_location):
        self._get_config(config_file_location)
        
    def _get_config(self, mconfig):

        with open(mconfig, 'r') as rfile:
            cfg = yaml.safe_load(rfile)
            print(cfg)
        try:
            # server_name = 'USGS-DDS'
            self.myname = cfg['myname']

        except ConnectionError:
            print('connection error occured')
            raise
    def perform_the_step(self):
        global logging
        # simulate
        sleepy=10
        logging.info(f'perform_the_step: simulated sleep {sleepy}');
        time.sleep(sleepy)
        logging.info('perform_the_step: COMPLETE ');
        # maybe use rio commandline


if __name__ == "__main__":

    global logging
    logdir='/wsefs/pipeline/log'
    filename = f'{logdir}/eta.log'
    logging.basicConfig(filename=filename, format='%(asctime)s - %(message)s', level=logging.INFO)

    logging.info(f'eta started')

    config = './eta.yaml'
    job = eta(config)

    job.perform_the_step()
    
            




