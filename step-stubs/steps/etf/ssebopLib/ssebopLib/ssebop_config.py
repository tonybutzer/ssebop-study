import os
import yaml
from glob import glob

class SSEBop_Config():
    """"""

    def __init__(self, config_path: str):
        with open(config_path, 'r') as rfile:
            self.config = yaml.safe_load(rfile)




if __name__ == "__main__":
    cfg = SSEBop_Config(config_path=r'W:\Projects\SSEBop_ET\GLOBAL\Version6\scrap_dsgd\ssebop_library_prototype\configs\ssebop_multi_image_config.yml')

    cfg_dict = cfg.config

    print(cfg_dict)

    # test out getting all the NDVI paths
    print(cfg_dict['ndvi']['root'])
    print(cfg_dict['ndvi']['wildcard'])

    ndvi_files = sorted(glob(cfg_dict['ndvi']['root'] + os.sep + cfg_dict['ndvi']['wildcard'], recursive=False))

    print('ndvi files \n ', ndvi_files)