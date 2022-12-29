import rioxarray as rioxa
from glob import glob
from dask.distributed import Client, LocalCluster, Lock
import os
from ssebop_v6.ssebop.raster_manager import normalize_to_std_grid_dask

def main():
    # instantiate the client as the do-er
    # docs.das.org/en/latest/deploying-python.html
    cluster = LocalCluster()
    # client = Client(n_workers=2, threads_per_worker=2, memory_limit='1GB')
    client = Client(cluster)
    print(client)
    print(cluster)

    etf_location = '/wsefs/pipeline/babadata/etfca_2022102.tif'
    refet_location = '/wsefs/pipeline/ndata/etr_raster.tif'

    output_location = '/wsefs/pipeline/etadata/eta_2022102.tif'

#     etf_files = sorted(glob(os.path.join(etf_location, 'etf_*.tif'), recursive=False))
#     refet_files = sorted(glob(os.path.join(refet_location, 'etr_*.tif'), recursive=False))

#     for etf, refet in zip(etf_files, refet_files):

    # etf_name = os.path.split(etf)[1]
    # etf_name_tif = etf_name.split('_')[1]
    # out_suffix = etf_name_tif[:7]
    # print(out_suffix)
    
    etf = etf_location

    # open the fill raster
    etf_ds = rioxa.open_rasterio(etf, chunks='auto').squeeze().drop(labels='band')

    # the refet needs to get virtualwarped
    # refet_ds = rioxa.open_rasterio(refet, chunks='auto').squeeze().drop(labels='band')
    # refet_ds_lst = normalize_to_std_grid_dask(inputs=[refet], temp_folder=None, nodatas=[None], sample_file=etf, resamplemethod='nearest')
    # refet_ds = refet_ds_lst[0]
    # # print both things
    # print(etf_ds)
    # print(refet_ds)

# RefET appears to be scaled by 10
    eta = etf_ds * refet_ds

    print(eta)

    eta.rio.to_raster(output_location,compress='LZW', tiled=True, lock=Lock('rio', client=client))




if __name__ == "__main__":
    main()