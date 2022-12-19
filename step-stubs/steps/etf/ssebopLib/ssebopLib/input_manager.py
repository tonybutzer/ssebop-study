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

from ssebopLib.ssebop_config import SSEBop_Config
from ssebopLib.raster_manager import RasterManager
from ssebopLib.ssebop import SSEBop
""" 
Handles a configuration for a run, and applies the correct process. Regardless 
of satellite platform.
All the pre-processing that is in-common between MODIS, VIIRS and Landsat
 ... Maybe contains the Zarr-ification functions for all sat platforms..."""

class SSEBopManager():
    """Takes a config file, handles the SSEBop run as configured."""

    def __init__(self, config_yml=None):
        if config_yml is not None:
            self.cfg = SSEBop_Config(config_path=config_yml)

    def run_viirs_ssebop(self):
        """Run VIIRS SSEBop based on config file"""
        pass

    def run_operational_viirs_ssebop(self):
        """Run VIIRS SSEBop based on config file"""
        
        #dt_raster.tif  gold_standard.tif  ndvi_qa.tif      ndwi_qa.tif      tmax_raster.tif
# global_water_raster.tif  lst_raster.tif     ndvi_raster.tif  ndwi_raster.tif
        
        # hardcoding some paths to files
        root = '/wsefs/pipeline/ndata'
        outloc = '/wsefs/pipeline/etfdata'
        lst_raster = os.path.join(root, 'lst_raster.tif')
        ndvi_raster = os.path.join(root, 'ndvi_raster.tif')
        ndwi_raster = os.path.join(root, 'ndwi_raster.tif')
        ndwi_qa = os.path.join(root, 'ndwi_qa.tif')
        dt_raster = os.path.join(root, 'dt_raster.tif')
        tmax_raster = os.path.join(root, 'tmax_raster.tif')
        global_water_raster = os.path.join(root, 'global_water_raster.tif')
        

        rm = RasterManager()
        # docs.das.org/en/latest/deploying-python.html
        cluster = LocalCluster()
        # client = Client(n_workers=2, threads_per_worker=2, memory_limit='1GB')
        client = Client(cluster)

#         dt_arr, tmax_arr, ndvi_arr, lst_arr, ndwi_arr, qa_arr, global_water_arr,
#         snow_arr_test, water_arr_test = normalize_to_std_grid(inputs=[dt_raster, tmax_raster,ndvi_raster, 
#                                       lst_raster, ndwi_raster, ndvi_qa, 
#                                       global_water_raster], temp_folder='/wsefs/pipeline/ndata', sample_file=lst_raster,
#                           resamplemethod='nearest', outdtype='float64', overwrite=True)
        
        dt_arr = rioxa.open_rasterio(dt_raster, chunks=(1000, 1000)).squeeze('band', drop=True)
        tmax_arr = rioxa.open_rasterio(tmax_raster, chunks=(1000, 1000)).squeeze('band', drop=True)
        ndvi_arr = rioxa.open_rasterio(ndvi_raster, chunks=(1000, 1000)).squeeze('band', drop=True)
        qa_arr = rioxa.open_rasterio(ndwi_qa, chunks=(1000, 1000)).squeeze('band', drop=True)
        ndwi_arr = rioxa.open_rasterio(ndwi_raster, chunks=(1000, 1000)).squeeze('band', drop=True)
        global_water_arr = rioxa.open_rasterio(global_water_raster, chunks=(1000, 1000)).squeeze('band', drop=True)
        lst_arr = rioxa.open_rasterio(lst_raster, chunks=(1000, 1000)).squeeze('band', drop=True)
        
        print('dt array')
        print(dt_arr)
        arr_shape = dt_arr.shape
        print('=' * 20)
        print('lst array')
        print(lst_arr)
        print('=' * 20)
        

        water_arr = xr.where(qa_arr == 10, x=0, y=1)
        snow_arr = xr.where(qa_arr == 4, x=0, y=1)

        print('water array')
        print(water_arr)
        print('+' * 20)

        # dealing with nodata values
        # # FIXING NODATA(s)
        # # == LST ==
        lst_arr = lst_arr.where(lst_arr > 0)
        # # == dT ==
        dt_arr = dt_arr.where(dt_arr >= 0)
        # # == tmax ==
        # # tmax has no meaningful nodata values
        # # == NDVI ==
        ndvi_arr = ndvi_arr.where(ndvi_arr != -2000)
        # == NDWI ==
        ndwi_arr = ndwi_arr.where(ndwi_arr != -2000)

        # Scaling the rasters down to the units
        ndwi_arr_scaled = ndwi_arr / 10000
        ndvi_arr_scaled = ndvi_arr / 10000
        lst_arr_scaled = lst_arr * 0.02  # https://lpdaac.usgs.gov/products/vnp21v001/

        #GELP 11_22 testing snow_arr_test and water_arr_test to see if memory error is related to how i
        # was creating the masks.
        # snow_arr_test and water_arr_test give you old rasters for testing...
        # TODO - gotta figure out how to create the mask arrays the same• as snow_arr_test...
        model = SSEBop(dt=dt_arr, tmax=tmax_arr, ndvi=ndvi_arr_scaled, lst=lst_arr_scaled, ndwi=ndwi_arr_scaled,
                       global_water_mask=global_water_arr, snow_mask=snow_arr, water_mask=water_arr,
                       temp_loc='/wsefs/pipeline/temp', ref_et=None)

        model.calculate_etf_experimental(fano_grid=5, fano_coarse_grid=100, hardcoded_sample_file=lst_raster,
                            client=client, testing=False)
        etf = model.etf

        print('output etf')
        print(etf)
        # output the file
        outlocation = os.path.join(outloc, 'etf_test.tif')
        etf.rio.to_raster(outlocation, tiled=True, lock=Lock('rio', client=client))


if __name__ == "__main__":
    sbmgr = SSEBopManager()
    # sbmgr.run_viirs_ssebop_test()
    sbmgr.run_operational_viirs_ssebop()

