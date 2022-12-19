# ===============================================================================
# Copyright 2022 Gabriel Parrish
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================
# ============= standard library imports ========================
import os
from dask.distributed import Client, LocalCluster, Lock
import numpy as np
import dask.array as da
import xarray as xr
import rioxarray as rioxa
import sys

# sys.path.append("/home/ec2-user/opt/cloud_ssebop/ssebop_v6/ssebopLib/ssebopLib")

from ssebop_config import SSEBop_Config
from raster_manager import normalize_to_std_grid
from ssebop import SSEBop
""" 
Handles a configuration for a run, and applies the correct process. Regardless 
of satellite platform.
All the pre-processing that is in-common between MODIS, VIIRS and Landsat
 ... Maybe contains the Zarr-ification functions for all sat platforms..."""


def run_viirs_ssebop_test():
    """Run VIIRS SSEBop based on config file"""
    # hardcoding some paths to files
    root = '/wsefs/pipeline/data_test_normToGrid'
    lst_raster = os.path.join(root, 'LS_eVSE_TEMP.2022.284-293.1KM.LST_TEMP.006.2022294042535.tif')
    ndvi_raster = os.path.join(root, 'GN_eVSH_NDVI.2022.284-293.1KM.VI_NDVI.001.2022310010552.tif')
    ndwi_raster = os.path.join(root, 'GN_eVSH_NDWI.2022.284-293.1KM.WI_NDWI.006.2022310173428.tif')
    ndvi_qa = os.path.join(root, 'GN_eVSH_NDVI.2022.284-293.1KM.VI_QUAL.001.2022310010552.tif')

    root2 = r's3://ws-in/ssebop/'
    dt_raster = os.path.join(root2, 'dT/dT_102.tif')
    tmax_raster = os.path.join(root2, 'air_temp/tmax_102.tif')
    global_water_raster = os.path.join(root2, 'masks/global_water_mask_inland.tif')


    # cluster = LocalCluster()
    # client = Client(cluster)

    arrs = normalize_to_std_grid(inputs=[dt_raster,tmax_raster, ndvi_raster, lst_raster, ndwi_raster, ndvi_qa, global_water_raster], temp_folder='/wsefs/pipeline/ndata', sample_file=lst_raster, resamplemethod='nearest', outdtype='float64', overwrite=True)
    
    dt_arr, tmax_arr, ndvi_arr, lst_arr, ndwi_arr,  qa_arr, global_water_arr =arrs

    # print('dt array')
    # print(dt_arr)


if __name__ == "__main__":

    run_viirs_ssebop_test()

