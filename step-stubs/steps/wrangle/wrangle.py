#!/usr/bin/env python
# coding: utf-8

import os
import logging
import requests
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup

import yaml
import os

import zipfile

def unzip_unlink(path_to_zip_file, directory_to_extract_to):
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)
    os.unlink(path_to_zip_file)


class Wrangle:
    def __init__(self, config_file_location):
        self._get_config(config_file_location)
        
    def _get_config(self, mconfig):

        with open(mconfig, 'r') as rfile:
            cfg = yaml.safe_load(rfile)
            #print(cfg)
        try:
            # server_name = 'USGS-DDS'
            self.username = cfg['username']
            self.password = cfg['password']
            self.site = cfg['site']
            self.url_path = cfg['url']
            self.out_loc = cfg['output_location']
            print(self.out_loc)
            self.output_name = cfg['outname']

        except ConnectionError:
            print('connection error occured')
            raise

    def get_all_links(self):
    
        soup = BeautifulSoup(requests.get(self.url_path, auth = HTTPBasicAuth(self.username, self.password)).text,
                             features="html.parser")
        schooltable = soup.find('table')
        my_links = schooltable.find_all("a", {"class": "linkUrl"})
        for link in my_links:
            yield(link['href'])
    
    def download(self, url_link):
        global logging

        if not os.path.exists(self.out_loc):
            os.makedirs(self.out_loc)  # create folder if it does not exist
        
        url =f'{self.site}{url_link}'
        logging.info(f'Downloading url ==> {url}')

        filename = url.split('/')[-1].replace(" ", "_")  # be careful with file names
        file_path = os.path.join(self.out_loc, filename)

        print(url)

        r = requests.get(url, stream=True, auth = HTTPBasicAuth(self.username, self.password))
        if r.ok:
            logging.info(f"saving to {os.path.abspath(file_path)}")
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 8):
                    if chunk:
                        f.write(chunk)
                        f.flush()
                        os.fsync(f.fileno())
        else:  # HTTP status code 4XX/5XX
            logging.error("Download failed: status code {}\n{}".format(r.status_code, r.text))
    
    def unzip_unlink(self, relatiive_path):
        global logging
        full_path = f'{self.out_loc}{relative_path}'
        logging.info(f'UNLINK:{full_path}')
        unzip_unlink(full_path, self.out_loc)
        pass




if __name__ == "__main__":

    global logging
    logdir='/wsefs/pipeline/log'
    filename = f'{logdir}/wrangle.log'
    logging.basicConfig(filename=filename, format='%(asctime)s - %(message)s', level=logging.INFO)

    logging.info(f'wrangler started')

    config = './wrangle.yaml'
    wr = Wrangle(config)
    
    for link in wr.get_all_links():
        if link.endswith('.zip'):
            print(link)
            wr.download(link)
            logging.info('wrangler COMPLETED {link}')
            just_file = link.split('/')[-1]
            # full_path = f'{out_loc}{just_file}'
            # print(full_path)
            relative_path = just_file
            wr.unzip_unlink(relative_path)
            




