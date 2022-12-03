import yaml
from data_prep.downloading.downloader import download_file

def download_dek(mconfig):

    with open(mconfig, 'r') as rfile:
        cfg = yaml.safe_load(rfile)
        print(cfg)
    try:
        # server_name = 'USGS-DDS'
        username = cfg['username']
        password = cfg['password']
        url = cfg['url']
        out_loc = cfg['output_location']
        output_name = cfg['outname']

        download_file(url=url, output_loc=out_loc, outname=output_name, authorization=(username, password),
                      cloudtransfer=False)
    except ConnectionError:
        print('connection error occured')
        raise

if __name__ == "__main__":

    config = r'D:\Users\gparrish\Desktop\parrish_config.yml'
    local_file = download_dek(mconfig=config)
