import urllib3
import shutil
import requests
import os
# from urlparse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

http = urllib3.PoolManager()
url = 'https://dds.cr.usgs.gov/highvolume/eviirs/Global_LST/expedited/VIIRS/2022/comp_010/LS_eVSE_TEMP.2022.001-010.1KM.COMPRES.006.2022031224925.zip'
path = r'D:\Users\gparrish\urllib3_downloads'

# https://stackoverflow.com/questions/23013220/max-retries-exceeded-with-url-in-requests

# session = requests.Session()
# retry = Retry(connect=3, backoff_factor=0.5)
# adapter = HTTPAdapter(max_retries=retry)
# session.mount('http://', adapter)
# session.mount('https://', adapter)

# r = session.get(url, auth=('sbohms', 'EEData123456'))
# old way
r = requests.get(url, auth=('sbohms', 'EEData123456'))
# try:
#     r = requests.get(url, auth=('sbohms', 'EEData123456'))
#     if r.status_code == 200:
#         print('status is 200')
#         with open(os.path.join(path, 'test.zip'), 'wb') as out:
#             for bits in r.iter_content():
#                 out.write(bits)
# except:
#     print('connection error')
#     raise
#
# # with open(os.path.join(path, 'test.zip'), 'wb') as out:
# #     for bits in r.iter_content():
# #         out.write(bits)

if r.status_code == 200:
    print('status is 200')
    with open(os.path.join(path, 'test.zip'), 'wb') as out:
        for bits in r.iter_content():
          out.write(bits)


#
# try:
#     password_mgr = urllib3.HTTPPasswordMgrWithDefaultRealm()
#     password_mgr.add_password(None, 'https://dds.cr.usgs.gov/',
#                               'sbohms',
#                               'EEData123456')
#     handler = urllib3.HTTPBasicAuthHandler(password_mgr)
#     opener = urllib3.build_opener(handler)
#     urllib3.install_opener(opener)
#     # website = urllib3.urlopen(url)
# except:
#     raise
#
# with http.request('GET', url, preload_content=False) as r, open(path, 'wb') as out_file:
#     #shutil.copyfileobj(r.data, out_file) # this writes a zero file
#     shutil.copyfileobj(r.data, out_file)