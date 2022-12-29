#!/usr/bin/env python
# coding: utf-8


import rioxarray as rioxa
from glob import glob
from dask.distributed import Client, LocalCluster, Lock
import os
import s3fs


def baba_gapfill_op_simple(output_location, name, current_etf, prev_raster_list, client=None, climo_fill=None):
    """"""
    # todo - check to make sure that index < before_list and index < after_lst

    # print('name: ', name)
    # print(before_lst)

    path = current_etf
    raster_ds = rioxa.open_rasterio(path, chunks='auto').squeeze().drop(labels='band')

    before_lst = prev_raster_list
    for i in range(2):
        # open the fill raster
        try:
            fill_ras = before_lst[i]
            a_ds = rioxa.open_rasterio(fill_ras, chunks='auto').squeeze().drop(labels='band')
            #  fill raster_ds with the fill value
            raster_ds = raster_ds.fillna(value=a_ds)
        except IndexError:
            # if there isn't enough fill data
            raster_ds = raster_ds.fillna(value=0.75)
        except TypeError:
            # if the raster is the start of the series
            raster_ds = raster_ds.fillna(value=0.75)

    if climo_fill is not None:
        climo_ds = rioxa.open_rasterio(climo_fill, chunks='auto').squeeze().drop(labels='band')
        raster_ds = raster_ds.fillna(value=climo_ds)
    # even if there are two fill rasters available in the before list. still fill gaps with 0.75
    # this doesn't do anything if the raster was already gapfilled by 0.75 during the exceptions above
    raster_ds = raster_ds.fillna(value=0.75)

    raster_ds.rio.to_raster(os.path.join(output_location, name),
                            compress='LZW', tiled=True, lock=Lock('rio', client=client))


def main_fun():
    # Initialize local hardware with Dask
    cluster = LocalCluster()
    client = Client(cluster)
    print(client)
    print(cluster)


    # where the rasters are...
    raster_root = r's3://ws-out/ssebop_viirs/'
    # set up an output location
    output_path = '/wsefs/pipeline/babadata/'
    #ras_dict = timeseries_creator_op(raster_root=raster_root)



    prev_raster_list = ['/wsefs/pipeline/etfdata/etf_2022101.tif',
                    '/wsefs/pipeline/etfdata/etf_2022093.tif']
    current_etf = '/wsefs/pipeline/etfdata/etf_2022102.tif'
    name='etfca_2022102.tif'

    baba_gapfill_op_simple(output_path, name, current_etf, prev_raster_list, client=client, climo_fill=None)


if __name__ == "__main__":
    main_fun()
