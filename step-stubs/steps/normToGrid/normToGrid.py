#!/usr/bin/env python
# coding: utf-8

import os
import logging
import yaml
import time

class normToGrid:
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
    def norm_them_files(self):
        global logging
        # simulate
        logging.info('norm_them_files: simulated sleep 60');
        time.sleep(60)
        logging.info('norm_them_files: COMPLETE ');
        # maybe use rio commandline


if __name__ == "__main__":

    global logging
    logdir='/wsefs/pipeline/log'
    filename = f'{logdir}/normToGrid.log'
    logging.basicConfig(filename=filename, format='%(asctime)s - %(message)s', level=logging.INFO)

    logging.info(f'normToGrid started')

    config = './normToGrid.yaml'
    norm = normToGrid(config)

    norm.norm_them_files()
    
            




