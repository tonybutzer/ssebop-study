import rioxarray as rioxa
from glob import glob
from dask.distributed import Client, LocalCluster, Lock
import os
import s3fs

s3 = s3fs.S3FileSystem()

def timeseries_creator_op(raster_root, key='etf_*', suffix='.tif', recur=False):
    """Takes a
     directory full of etf timeseries and makes a dictionary of
     {key(raster_name): (subject_rasterpath, [before_lst], [after_lst])}, where before_lst and after_lst are paths of
     rasters that come temporally before and after the 'subject_rasterpath'."""

    # print(f'{raster_root}{os.sep}{key}{suffix}')
    # print(glob(f'{raster_root}{os.sep}{key}{suffix}'))
    rasters_list = sorted(s3.glob(f'{raster_root}{os.sep}{key}{suffix}'))
    print('raster list')
    print(rasters_list)

    ras_dict = {}
    for i, ras in enumerate(rasters_list):
        # get the rastername
        # rasname = s3fs.split_path(ras)[1]
        rasname = ras.split('/')[-1]
        print('raslist to split')
        print(ras.split('/'))
        # grab before and after rasters where possible
        if i > 0:
            before_rasters = rasters_list[0:i]
            # we reverse order the before rasters... for going backwards...
            before_rasters.reverse()
        if i < (len(rasters_list) - 1):
            after_rasters = rasters_list[i+1:]
        # if at the beginning or end, put in none for the last value.
        if i == 0:
            value_tuple = (ras, None, after_rasters)
        elif i == (len(rasters_list) - 1):
            value_tuple = (ras, before_rasters, None)
        else:
            value_tuple = (ras, before_rasters, after_rasters)


        ras_dict[rasname] = value_tuple

    return ras_dict


def baba_gapfill_op(output_location, name, path, before_lst, index=2, client=None, climo_fill=None):
    """"""
    # todo - check to make sure that index < before_list and index < after_lst

    print('name: ', name)
    print(before_lst)


    raster_ds = rioxa.open_rasterio(path, chunks='auto').squeeze().drop(labels='band')

    for i in range(index):
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
                            tiled=True, lock=Lock('rio', client=client))


def main():

    # Initialize local hardware with Dask
    cluster = LocalCluster()
    client = Client(cluster)
    print(client)
    print(cluster)

    # where the rasters are...
    raster_root = r's3://ws-out/ssebop_viirs/'
    # set up an output location
    output_path = r's3://ws-out/ssebop_viirs/baba_op_output'
    ras_dict = timeseries_creator_op(raster_root=raster_root)

    # print(ras_dict)
    # for k, v in ras_dict.items():
    #     print(k)
    #     print(v)
    #     print('='*20)
    # print('hey hey hey')
    print(ras_dict['etf_2017041.tif'][0])
    print(ras_dict['etf_2017041.tif'][1])
    print(ras_dict['etf_2017041.tif'][2])

    # Loop this for each dictionary item, and write out just one file...
    for k, v in ras_dict.items():
        # unpack
        path, before_lst, after_lst = v
        # name is k
        s3path = f's3://{path}'
        baba_gapfill_op(output_path, k, s3path, before_lst, index=2, client=client, climo_fill=None)

if __name__ == "__main__":
    # uncomment if you want to recreate the baba cumulative dekads
    main()
