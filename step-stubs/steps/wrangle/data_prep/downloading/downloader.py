from ftplib import FTP
import glob
import os
import sys
import subprocess
import traceback
import yaml

import urllib3
import shutil
import requests
import os
import time

# set up the pool manager.
http = urllib3.PoolManager()

# from urlparse import urlparse

def download_file(url: str, output_loc: str, outname:str, authorization: tuple, cloudtransfer=False, cloud_bucket=None, cloud_loc=None):
    """Downloads generic file that's password protected... See urllib_test.py to see kinda how it works."""

    # filename = os.path.basename(urlparse(url).path)

    r = requests.get(url, auth=authorization)

    if r.status_code == 200:
        with open(os.path.join(output_loc, f'{outname}'), 'wb') as out:
            for bits in r.iter_content():
                out.write(bits)

    else:
        print(f'status code is {r.status_code}, waiting, and trying again...')
        time.sleep(15.0)
        r = requests.get(url, auth=authorization)
        with open(os.path.join(output_loc, f'{outname}'), 'wb') as out:
            for bits in r.iter_content():
                out.write(bits)


def download_lst(self):
    '''
    Downloads LST data from:
    https://dds.cr.usgs.gov/highvolume/emodis_lst_v6/expedited/AQUA/YYYY/
        comp_JdayEnd/
        LS_eMAE_TEMP.YYYY.JdayStart-JdayEnd.1KM.COMPRES.006.YYYYxxxxxxxxx.zip
    Extracts LST and EMIS files to the temp workspace,
    Does some math on the LST file and from there
    Processes LST and EMIS and writes them to:
        D:\\NetApp\\sharedwebfs1\\<development/production>\\FEWS\\ppg\\data\\
                Global\\Dekadal\\ETa\\geotiff\\EMIS_V006\\YYYY\\emiYYDEK.tif
        D:\\NetApp\\sharedwebfs1\\<development/production>\\FEWS\\ppg\\data\\
                Global\\Dekadal\\ETa\\geotiff\\LST_V006\\YYYY\\lstYYDEK.tif
    Time:  approx 3 minutes

    returns:
        err(boolean) - True for error, else false
    '''
    url_root = ('https://' + self.dwnld_server['server'] +
                '/highvolume/emodis_lst_v6/expedited/AQUA/')

    t_1 = datetime.datetime.now()
    msg = ('{} {:%Y-%m-%d %H:%M:%S} {}'.format(constants.SEP1,
                                               t_1,
                                               constants.SEP1))
    self.logger.info(msg)
    msg = ('Downloading Input Data from DAAC {}'.format(self.heading))
    self.logger.info(msg)
    err = True
    # download LST zip file
    # LS_eMAE_TEMP.2017.001-010.1KM.COMPRES.006.2017012081000.zip
    url = os.path.join(url_root, self.year,
                       ('comp_' + self.jday_end)).replace('\\', '/')
    try:
        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, url_root,
                                  self.dwnld_server['username'],
                                  self.dwnld_server['password'])
        handler = urllib2.HTTPBasicAuthHandler(password_mgr)
        opener = urllib2.build_opener(handler)
        urllib2.install_opener(opener)
        website = urllib2.urlopen(url)

        os.chdir(self.temp_workspace)
        matches = re.findall(
            'LS_eMAE_TEMP(.*)' + '(.*).zip',
            website.read())

        rand_matches = matches[0][0][30:43]  # date part in zip file
        datafile = ('LS_eMAE_TEMP.' + self.year + '.' +
                    self.jday_start + '-' + self.jday_end +
                    '.1KM.COMPRES.006.' + rand_matches + '.zip')

        urlpath = os.path.join(url, datafile).replace('\\', '/')
        remotefile = urllib2.urlopen(urlpath)
        chunk = remotefile.readlines()  # do by line to avoid memory error
        with open(datafile, 'wb') as output:
            output.writelines(chunk)
        remotefile.close()
        msg = ('Downloaded: {}'.format(urlpath))
        self.logger.info(msg)
        # make yearly dirs if needed
        if not os.path.exists(self.lst_dir):
            os.makedirs(self.lst_dir)
        if not os.path.exists(self.emis_dir):
            os.makedirs(self.emis_dir)
        # Unzip LST/Emis files
        lst_raw, emis_raw = self.__unzip_download__(datafile)
        # rename and move the emis file to its location
        shutil.move(os.path.join(self.temp_workspace, emis_raw),
                    self.emis_path)
        # Process lst
        self.__process_lst_file__(lst_raw)

        self.logger.info(constants.SEP2)
        self.logger.info('COMPLETE')
        t_2 = datetime.datetime.now()
        t_t = t_2 - t_1
        msg = 'Processing time (HH:MM:SS): {}'.format(t_t)
        self.logger.info(msg)
        err = False
    except urllib2.URLError:
        self.logger.error(
            'Could NOT get zip file\nDownload: ...INCOMPLETE')
        raise urllib2.URLError
    except BaseException:
        msg = print_msg.get_traceback_msg(sys.exc_info())
        msg += ('\nERROR\t Download of Data from DAAC ... INCOMPLETE')
        msg += ('\n{}'.format(constants.SEP2))
        self.logger.error(msg)
    return err


def download_from_usgs_dds(remote_site, remote_directory, local_directory,
                           raw_pattern, dataset_region, dataset_type, mconfig,
                           log_file):
    '''
        DDS: Dekadal eMODIS NDVI
    '''

    with open(mconfig, 'r') as rfile:
        cfg = yaml.safe_load(rfile)
        print(cfg)
    try:
        server_name = 'USGS-DDS'
        username = cfg['username']
        password = cfg['password']
        local_file_size = 0
        local_file = None
        wget_cut_dir = len(remote_directory.split("/")) - 1
        # define URL path to the directory where the raw data is located
        remote_url = "https://" + str(remote_site)
        msg = ("INFO: Checking data availability on %s%s" % (remote_url,
                                                             remote_directory))
        # print_msg.printMsg_orig(msg, log_file)
        print(msg)

        os.chdir(local_directory)

        # Use wget tool utility to download data
        wget_command = (
            "%s -r --no-host-directories --cut-dirs=%s --no-check-certificate "
            "--user %s --password %s --accept=%s %s%s" % (
                cfg['wget_tool_exe'],
                str(wget_cut_dir),
                username, password,
                raw_pattern,
                remote_url,
                remote_directory))
        print("DEBUG: {}".format(wget_command))
        os.system(wget_command)
        local_files = glob.glob(local_directory + os.sep + raw_pattern)
        # Should be found 1 zip file
        if len(local_files) == 1:
            local_file = local_files[0]
            local_file_size = os.stat(local_file).st_size
        return local_file, local_file_size

    except BaseException:
        tb = sys.exc_info()[2]
        exctype, excvalue = sys.exc_info()[:2]
        tbinfo = traceback.format_tb(tb)[0]
        msg = ("PYTHON ERRORS:\nTraceback Info:\n" + tbinfo +
               "\nError Info:\n    " + str(sys.exc_info()[0]) + ": " +
               str(sys.exc_info()[1]) + "\n")
        print(msg)
        if local_file is not None:
            if os.path.exists(local_file):
                if os.stat(local_file).st_size == 0:
                    os.remove(local_file)
        return local_file, local_file_size



