import os
from affine import Affine
import numpy as np
import rasterio.mask
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio import shutil as rio_shutil
from rasterio.vrt import WarpedVRT
from timeit import default_timer as t_now
import rioxarray as riox
import dask.array as da
import sys
import time
import concurrent.futures as cf
from concurrent.futures import wait
import dask
import boto3

# consider tiling!
# https://rasterio.readthedocs.io/en/latest/topics/image_options.html
# can this help?
# https://stackstac.readthedocs.io/en/latest/index.html

def s3_push_delete_local(local_file, bucket, bucket_filepath):
    s3 = boto3.client('s3')
    with open(local_file, "rb") as f:
        if 'vsis3' in bucket:
            bucket = bucket.split('/')[-1]
            print(bucket, bucket_filepath)
        s3.upload_fileobj(f, bucket, bucket_filepath)
    os.remove(local_file)


def resample_daskVRT(filepath: str, scaling_value: int, resample_alg='average', nodata_val=None):
    if resample_alg == 'average':
        rs = Resampling.average

    elif resample_alg == 'sum':
        rs = Resampling.sum

    elif resample_alg == 'bilinear':
        rs = Resampling.bilinear

    elif resample_alg == 'nearest':
        rs = Resampling.nearest

    # open file to get needed info from file
    with rasterio.open(filepath) as src:
        # print('blockshapes\n', src.block_shapes)
        # print('metadata \n', src.meta)
        meta = src.meta
        # https://www.perrygeo.com/python-affine-transforms.html
        transform = meta['transform']
        # print(transform, '\n')
        xres = transform[0]
        yres = transform[4]
        new_xres = xres * scaling_value
        new_yres = yres * scaling_value
        height = meta['height']
        width = meta['width']
        new_height = round(height / scaling_value)
        new_width = round(width / scaling_value)

        new_transform = Affine(a=new_xres, b=transform[1], c=transform[2],
                               d=transform[3], e=new_yres, f=transform[5])
        # print(new_transform)

    # open file again to resample
    with rasterio.open(filepath) as src:
        if nodata_val is not None:
            src.meta['nodata'] = nodata_val

        # can you apply a mask somehow right here before reading into memory?
        # with WarpedVRT(src) as vrt1:
        # with WarpedVRT(vrt1, ....)

        with WarpedVRT(src,
                       transform=new_transform,
                       height=new_height,
                       width=new_width,
                       resampling=rs) as vrt:

            # # # outputting data to file or reading in directly
            # data = vrt.read()

            # outwarp = os.path.join(root, f'ndvi_warp{scaling_value}.tif')
            # rio_shutil.copy(vrt, outwarp, driver='GTiff')

            # instead do like this example: https://gist.github.com/rmg55/875a2b79ee695007a78ae615f1c916b2
            data = riox.open_rasterio(vrt, chunks=True).squeeze('band', drop=True)
            # handling of nodata values
            # https://corteva.github.io/rioxarray/stable/rioxarray.html#rioxarray.raster_array.RasterArray.write_nodata
            if nodata_val is not None:
                # NaN mask where values where nodata
                data = data.where(data != nodata_val)
                data.rio.write_nodata(nodata_val, encoded=True, inplace=True)
            return data


def coarsen_dask_arr(arr: da, scaling_value: int, resample_alg='average', mask=None):
    """
    big help with implementation from Rich Signell.

    :param arr: Dask array opened as: rxr.open_rasterio(path).squeeze('band', drop=True)
    :param scaling_value: a whole interger. In the SSEBop context 5, 25 or 100.
    :param resample_alg: 'average', 'mean' or 'sum' accepted.
    :return: dask array that has been resampled based on scaling factor.
    """

    with dask.config.set(**{'array.slicing.split_large_chunks': False}):
        # To avoid creating the large chunks, set the option
        #     >>> with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        #     ...     array.reshape(shape)Explictly passing ``limit`` to ``reshape`` will also silence this warning
        #     >>> array.reshape(shape, limit='128 MiB')
        #   dsub = arr.coarsen(x=scaling_value, y=scaling_value, boundary='pad').mean()

        if resample_alg == 'average':
            # https://docs.xarray.dev/en/v2022.06.0/generated/xarray.core.rolling.DataArrayCoarsen.
            # construct.html#xarray.core.rolling.DataArrayCoarsen.construct
            dsub = arr.coarsen(x=scaling_value, y=scaling_value, boundary='pad').mean()
        elif resample_alg == 'mean':
            dsub = arr.coarsen(x=scaling_value, y=scaling_value, boundary='pad').mean()
        elif resample_alg == 'sum':
            # is this correct?
            dsub = arr.coarsen(x=scaling_value, y=scaling_value, boundary='pad').sum()
        elif resample_alg == 'median':
            dsub = arr.coarsen(x=scaling_value, y=scaling_value, boundary='pad').median()
        else:
            print('WARNING')
            print(f"{resample_alg}, is not supported, options are: 'average', 'mean' or 'sum'")
            raise TypeError
        return dsub


def normalize_to_std_grid(inputs, temp_folder, sample_file=None,
                          resamplemethod='nearest', outdtype='float64', overwrite=True):
    """
    Uses rasterio virtual raster to standardize grids of different crs, resolution, boundaries based on  a shapefile geometry feature
    :param inputs: a list of (daily) raster input files for SSEBop.
    :param outloc: output locations 'temp' for the virtual files
    :return: list of numpy arrays
    """
    outputs = []

    with rasterio.open(sample_file) as src:

        out_meta = src.meta
        crs = out_meta['crs']
        transform = out_meta['transform']
        left = transform[2]
        top = transform[5]
        cols = out_meta['width']
        rows = out_meta['height']
        xres = transform[0]
        yres = transform[4]
        # return out_meta

    if resamplemethod == 'nearest':
        rs = Resampling.nearest
    elif resamplemethod == 'average':
        rs = Resampling.average
    else:
        print('only nearest-neighbor and average resampling is supported at this time')
        sys.exit(0)

    for i, warpfile in enumerate(inputs):
        print('warpfile', warpfile, i)
        # print(f'warping {warpfile}\n with nodata value: {nodatas[i]}')
        # TODO:  Source dataset should be opened in read-only mode. Use of datasets opened in modes other than
        #  'r' will be disallowed in a future version.
        with rasterio.open(warpfile, 'r') as src:
            # create the virtual raster based on the standard rasterio
            # attributes from the sample tiff and shapefile feature.
            # update with suitable nodata values.
            with WarpedVRT(src, resampling=rs,
                           crs=crs,
                           transform=transform,
                           height=rows,
                           width=cols,
                           dtype=outdtype) as vrt:
                # save the file as an enumerated tiff. reopen outside this loop with the outputs list
                outwarp = os.path.join(temp_folder, 'temp_{}.tif'.format(i))
                if not overwrite:
                    if not os.path.exists(outwarp):
                        rio_shutil.copy(vrt, outwarp, driver='GTiff')
                else:
                    rio_shutil.copy(vrt, outwarp, driver='GTiff')
                outputs.append(outwarp)
    return outputs


def normalize_to_std_grid_dask(inputs, temp_folder, nodatas=[], sample_file=None,
                               resamplemethod='nearest', outdtype='float64', overwrite=True, s3_string=None):
    """
        Uses rasterio virtual raster to standardize grids of different crs, resolution, boundaries based on  a shapefile geometry feature
        :param inputs: a list of (daily) raster input files for SSEBop.
        :param outloc: output locations 'temp' for the virtual files
        :return: list of numpy arrays
        """
    outputs = []
    if s3_string is None:
        with rasterio.open(f's3://{sample_file}') as src:

            out_meta = src.meta
            crs = out_meta['crs']
            transform = out_meta['transform']
            left = transform[2]
            top = transform[5]
            cols = out_meta['width']
            rows = out_meta['height']
            xres = transform[0]
            yres = transform[4]
            # return out_meta

        if resamplemethod == 'nearest':
            rs = Resampling.nearest
        elif resamplemethod == 'average':
            rs = Resampling.average
        else:
            print('only nearest-neighbor and average resampling is supported at this time')
            sys.exit(0)

        for i, warpfile in enumerate(inputs):
            print('warpfile', warpfile, i)
            print(f'warping {warpfile}\n with nodata value: {nodatas[i]}')
            # TODO:  Source dataset should be opened in read-only mode. Use of datasets opened in modes other than
            #  'r' will be disallowed in a future version.
            with rasterio.open(warpfile, 'r') as src:
                # create the virtual raster based on the standard rasterio
                # attributes from the sample tiff and shapefile feature.
                # update with suitable nodata values.
                nodata_val = nodatas[i]
                # src.nodata = nodata_val
                with WarpedVRT(src, resampling=rs,
                               crs=crs,
                               transform=transform,
                               height=rows,
                               width=cols,
                               dtype=outdtype) as vrt:
                    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
                        # To avoid creating the large chunks, set the option
                        #     >>> with dask.config.set(**{'array.slicing.split_large_chunks': True}):
                        #     ...     array.reshape(shape)Explictly passing ``limit`` to ``reshape`` will also silence this warning
                        #     >>> array.reshape(shape, limit='128 MiB')
                        #   dsub = arr.coarsen(x=scaling_value, y=scaling_value, boundary='pad').mean()
                        data = riox.open_rasterio(vrt, chunks=(1000, 1000)).squeeze('band', drop=True)
                        outputs.append(data)
        return outputs

    else:
        with rasterio.open(f's3://{sample_file}') as src:

            out_meta = src.meta
            crs = out_meta['crs']
            transform = out_meta['transform']
            left = transform[2]
            top = transform[5]
            cols = out_meta['width']
            rows = out_meta['height']
            xres = transform[0]
            yres = transform[4]
            # return out_meta

        if resamplemethod == 'nearest':
            rs = Resampling.nearest
        elif resamplemethod == 'average':
            rs = Resampling.average
        else:
            print('only nearest-neighbor and average resampling is supported at this time')
            sys.exit(0)

        for i, warpfile in enumerate(inputs):
            print('warpfile', warpfile, i)
            print(f'warping {warpfile}\n with nodata value: {nodatas[i]}')
            # TODO:  Source dataset should be opened in read-only mode. Use of datasets opened in modes other than
            #  'r' will be disallowed in a future version.
            with rasterio.open(f's3://{warpfile}', 'r') as src:
                # create the virtual raster based on the standard rasterio
                # attributes from the sample tiff and shapefile feature.
                # update with suitable nodata values.
                nodata_val = nodatas[i]
                # src.nodata = nodata_val
                with WarpedVRT(src, resampling=rs,
                               crs=crs,
                               transform=transform,
                               height=rows,
                               width=cols,
                               dtype=outdtype) as vrt:
                    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
                        # To avoid creating the large chunks, set the option
                        #     >>> with dask.config.set(**{'array.slicing.split_large_chunks': True}):
                        #     ...     array.reshape(shape)Explictly passing ``limit`` to ``reshape`` will also silence this warning
                        #     >>> array.reshape(shape, limit='128 MiB')
                        #   dsub = arr.coarsen(x=scaling_value, y=scaling_value, boundary='pad').mean()
                        data = riox.open_rasterio(vrt, chunks=(1000, 1000)).squeeze('band', drop=True)
                        outputs.append(data)
        return outputs


def warp_based_on_sample_dask(input_raster: str, nodata=None, sample_file=None,
                              resamplemethod='average', outdtype='float32'):
    with rasterio.open(f's3://{sample_file}') as src:

        out_meta = src.meta
        crs = out_meta['crs']
        transform = out_meta['transform']
        left = transform[2]
        top = transform[5]
        cols = out_meta['width']
        rows = out_meta['height']
        xres = transform[0]
        yres = transform[4]
        # return out_meta

    if resamplemethod == 'nearest':
        rs = Resampling.nearest
    elif resamplemethod == 'average':
        rs = Resampling.average
    elif resamplemethod == 'bilinear':
        rs = Resampling.bilinear
    else:
        print('only nearest-neighbor and average resampling is supported at this time')
        sys.exit(0)

    with rasterio.open(input_raster, 'r') as src:
        # create the virtual raster based on the standard rasterio
        # attributes from the sample tiff and shapefile feature.
        # update with suitable nodata values.
        nodata_val = nodata
        # src.nodata = nodata_val
        with WarpedVRT(src, resampling=rs,
                       crs=crs,
                       transform=transform,
                       height=rows,
                       width=cols,
                       dtype=outdtype) as vrt:
            with dask.config.set(**{'array.slicing.split_large_chunks': True}):
                # To avoid creating the large chunks, set the option
                #     >>> with dask.config.set(**{'array.slicing.split_large_chunks': True}):
                #     ...     array.reshape(shape)Explictly passing ``limit`` to ``reshape`` will also silence this warning
                #     >>> array.reshape(shape, limit='128 MiB')
                #   dsub = arr.coarsen(x=scaling_value, y=scaling_value, boundary='pad').mean()
                data = riox.open_rasterio(vrt, chunks=(1000, 1000)).squeeze('band', drop=True)

    return data


def warp_based_on_sample(input_raster: str, temp_folder: str, outname: str, nodata=None, sample_file=None,
                         resamplemethod='average', outdtype='float32'):
    with rasterio.open(f's3://{sample_file}') as src:

        out_meta = src.meta
        crs = out_meta['crs']
        transform = out_meta['transform']
        left = transform[2]
        top = transform[5]
        cols = out_meta['width']
        rows = out_meta['height']
        xres = transform[0]
        yres = transform[4]
        # return out_meta

    if resamplemethod == 'nearest':
        rs = Resampling.nearest
    elif resamplemethod == 'average':
        rs = Resampling.average
    elif resamplemethod == 'bilinear':
        rs = Resampling.bilinear
    else:
        print('only nearest-neighbor and average resampling is supported at this time')
        sys.exit(0)

    with rasterio.open(input_raster, 'r') as src:
        # create the virtual raster based on the standard rasterio
        # attributes from the sample tiff and shapefile feature.
        # update with suitable nodata values.
        nodata_val = nodata
        # src.nodata = nodata_val
        with WarpedVRT(src, resampling=rs,
                       crs=crs,
                       transform=transform,
                       height=rows,
                       width=cols,
                       dtype=outdtype) as vrt:
            # save the file as an enumerated tiff. reopen outside this loop with the outputs list
            outwarp = os.path.join(temp_folder, outname)
            rio_shutil.copy(vrt, outwarp, driver='GTiff')

    return outwarp

class RasterManager():
    """Manages the Rasters for SSEBop"""

    def __init__(self, cfg=None):
        self.config = cfg

    def _s3_push_delete_local(self, local_file, bucket, bucket_filepath):
            s3 = boto3.client('s3')
            with open(local_file, "rb") as f:
                if 'vsis3' in bucket:
                    bucket = bucket.split('/')[-1]
                    print(bucket, bucket_filepath)
                s3.upload_fileobj(f, bucket, bucket_filepath)
            os.remove(local_file)

    def _resample_daskVRT(self, filepath: str, scaling_value: int, resample_alg='average', nodata_val=None):

        if resample_alg == 'average':
            rs = Resampling.average

        elif resample_alg == 'sum':
            rs = Resampling.sum

        elif resample_alg == 'bilinear':
            rs = Resampling.bilinear

        elif resample_alg == 'nearest':
            rs = Resampling.nearest

        # open file to get needed info from file
        with rasterio.open(filepath) as src:
            # print('blockshapes\n', src.block_shapes)
            # print('metadata \n', src.meta)
            meta = src.meta
            # https://www.perrygeo.com/python-affine-transforms.html
            transform = meta['transform']
            # print(transform, '\n')
            xres = transform[0]
            yres = transform[4]
            new_xres = xres * scaling_value
            new_yres = yres * scaling_value
            height = meta['height']
            width = meta['width']
            new_height = round(height/scaling_value)
            new_width = round(width/scaling_value)

            new_transform = Affine(a=new_xres, b=transform[1], c=transform[2],
                                   d=transform[3], e=new_yres, f=transform[5])
            # print(new_transform)

        # open file again to resample
        with rasterio.open(filepath) as src:
            if nodata_val is not None:
                src.meta['nodata'] = nodata_val

            # can you apply a mask somehow right here before reading into memory?
            # with WarpedVRT(src) as vrt1:
            # with WarpedVRT(vrt1, ....)

            with WarpedVRT(src,
                           transform=new_transform,
                           height=new_height,
                           width=new_width,
                           resampling=rs) as vrt:

                # # # outputting data to file or reading in directly
                # data = vrt.read()

                # outwarp = os.path.join(root, f'ndvi_warp{scaling_value}.tif')
                # rio_shutil.copy(vrt, outwarp, driver='GTiff')

                # instead do like this example: https://gist.github.com/rmg55/875a2b79ee695007a78ae615f1c916b2
                data = riox.open_rasterio(vrt, chunks=True).squeeze('band', drop=True)
                # handling of nodata values
                # https://corteva.github.io/rioxarray/stable/rioxarray.html#rioxarray.raster_array.RasterArray.write_nodata
                if nodata_val is not None:
                    # NaN mask where values where nodata
                    data = data.where(data != nodata_val)
                    data.rio.write_nodata(nodata_val, encoded=True, inplace=True)
                return data

    def _coarsen_dask_arr(self, arr: da, scaling_value: int, resample_alg='average', mask=None):
        """
        big help with implementation from Rich Signell.

        :param arr: Dask array opened as: rxr.open_rasterio(path).squeeze('band', drop=True)
        :param scaling_value: a whole interger. In the SSEBop context 5, 25 or 100.
        :param resample_alg: 'average', 'mean' or 'sum' accepted.
        :return: dask array that has been resampled based on scaling factor.
        """

        with dask.config.set(**{'array.slicing.split_large_chunks': False}):
            # To avoid creating the large chunks, set the option
            #     >>> with dask.config.set(**{'array.slicing.split_large_chunks': True}):
            #     ...     array.reshape(shape)Explictly passing ``limit`` to ``reshape`` will also silence this warning
            #     >>> array.reshape(shape, limit='128 MiB')
            #   dsub = arr.coarsen(x=scaling_value, y=scaling_value, boundary='pad').mean()

            if resample_alg == 'average':
                #https://docs.xarray.dev/en/v2022.06.0/generated/xarray.core.rolling.DataArrayCoarsen.
                #construct.html#xarray.core.rolling.DataArrayCoarsen.construct
                dsub = arr.coarsen(x=scaling_value, y=scaling_value, boundary='pad').mean()
            elif resample_alg == 'mean':
                dsub = arr.coarsen(x=scaling_value, y=scaling_value, boundary='pad').mean()
            elif resample_alg == 'sum':
                # is this correct?
                dsub = arr.coarsen(x=scaling_value, y=scaling_value, boundary='pad').sum()
            elif resample_alg == 'median':
                dsub = arr.coarsen(x=scaling_value, y=scaling_value, boundary='pad').median()
            else:
                print('WARNING')
                print(f"{resample_alg}, is not supported, options are: 'average', 'mean' or 'sum'")
                raise TypeError
            return dsub

    def _normalize_to_std_grid(self, inputs, temp_folder, nodatas=[], sample_file=None,
                              resamplemethod='nearest', outdtype='float64', overwrite=True):
        """
        Uses rasterio virtual raster to standardize grids of different crs, resolution, boundaries based on  a shapefile geometry feature
        :param inputs: a list of (daily) raster input files for SSEBop.
        :param outloc: output locations 'temp' for the virtual files
        :return: list of numpy arrays
        """
        outputs = []

        with rasterio.open(sample_file) as src:

            out_meta = src.meta
            crs = out_meta['crs']
            transform = out_meta['transform']
            left = transform[2]
            top = transform[5]
            cols = out_meta['width']
            rows = out_meta['height']
            xres = transform[0]
            yres = transform[4]
            # return out_meta

        if resamplemethod == 'nearest':
            rs = Resampling.nearest
        elif resamplemethod == 'average':
            rs = Resampling.average
        else:
            print('only nearest-neighbor and average resampling is supported at this time')
            sys.exit(0)

        for i, warpfile in enumerate(inputs):
            print('warpfile', warpfile, i)
            print(f'warping {warpfile}\n with nodata value: {nodatas[i]}')
            # TODO:  Source dataset should be opened in read-only mode. Use of datasets opened in modes other than
            #  'r' will be disallowed in a future version.
            with rasterio.open(warpfile, 'r') as src:
                # create the virtual raster based on the standard rasterio
                # attributes from the sample tiff and shapefile feature.
                # update with suitable nodata values.
                nodata_val = nodatas[i]
                src.nodata = nodata_val
                with WarpedVRT(src, resampling=rs,
                               crs=crs,
                               transform=transform,
                               height=rows,
                               width=cols,
                               dtype=outdtype) as vrt:
                    # save the file as an enumerated tiff. reopen outside this loop with the outputs list
                    outwarp = os.path.join(temp_folder, 'temp_{}.tif'.format(i))
                    if not overwrite:
                        if not os.path.exists(outwarp):
                            rio_shutil.copy(vrt, outwarp, driver='GTiff')
                    else:
                        rio_shutil.copy(vrt, outwarp, driver='GTiff')
                    outputs.append(outwarp)
        return outputs

    def _normalize_to_std_grid_dask(self, inputs, sample_file=None,
                              resamplemethod='nearest', outdtype='float64', tiled=True, blockx=1000, blocky=1000, s3_string=False):
        """
            Uses rasterio virtual raster to standardize grids of different crs, resolution, boundaries based on  a shapefile geometry feature
            :param inputs: a list of (daily) raster input files for SSEBop.
            :param outloc: output locations 'temp' for the virtual files
            :return: list of numpy arrays
            """
        outputs = []
        if s3_string:
            with rasterio.open(f's3://{sample_file}') as src:

                out_meta = src.meta
                crs = out_meta['crs']
                transform = out_meta['transform']
                cols = out_meta['width']
                rows = out_meta['height']
                # left = transform[2]
                # top = transform[5]
                # xres = transform[0]
                # yres = transform[4]
                # return out_meta

            if resamplemethod == 'nearest':
                rs = Resampling.nearest
            elif resamplemethod == 'average':
                rs = Resampling.average
            else:
                print('only nearest-neighbor and average resampling is supported at this time')
                sys.exit(0)

            for i, warpfile in enumerate(inputs):
                print('warpfile', warpfile, i)
                with rasterio.open(f's3://{warpfile}', 'r') as src:
                    # create the virtual raster based on the standard rasterio
                    # attributes from the sample tiff and shapefile feature.
                    # # update with suitable nodata values. DEADBEEF
                    # nodata_val = nodatas[i]
                    # # src.nodata = nodata_val
                    with WarpedVRT(src, resampling=rs,
                                   crs=crs,
                                   transform=transform,
                                   height=rows,
                                   width=cols,
                                   dtype=outdtype) as vrt:
                        with dask.config.set(**{'array.slicing.split_large_chunks': True}):
                            # To avoid creating the large chunks, set the option
                            #     >>> with dask.config.set(**{'array.slicing.split_large_chunks': True}):
                            #     ...     array.reshape(shape)Explictly passing ``limit`` to ``reshape`` will also silence this warning
                            #     >>> array.reshape(shape, limit='128 MiB')
                            #   dsub = arr.coarsen(x=scaling_value, y=scaling_value, boundary='pad').mean()
                            data = riox.open_rasterio(vrt, chunks=(1000, 1000)).squeeze('band', drop=True)
                            outputs.append(data)
            return outputs

        else:
            with rasterio.open(sample_file) as src:

                out_meta = src.meta
                crs = out_meta['crs']
                transform = out_meta['transform']
                left = transform[2]
                top = transform[5]
                cols = out_meta['width']
                rows = out_meta['height']
                xres = transform[0]
                yres = transform[4]
                # return out_meta

            if resamplemethod == 'nearest':
                rs = Resampling.nearest
            elif resamplemethod == 'average':
                rs = Resampling.average
            else:
                print('only nearest-neighbor and average resampling is supported at this time')
                sys.exit(0)

            for i, warpfile in enumerate(inputs):
                print('warpfile', warpfile, i)
                with rasterio.open(warpfile, 'r') as src:
                    # create the virtual raster based on the standard rasterio
                    # attributes from the sample tiff and shapefile feature.
                    with WarpedVRT(src, resampling=rs,
                                   crs=crs,
                                   transform=transform,
                                   height=rows,
                                   width=cols,
                                   dtype=outdtype) as vrt:
                        # To avoid creating the large chunks, set the option
                        with dask.config.set(**{'array.slicing.split_large_chunks': True}):

                            data = riox.open_rasterio(vrt, chunks=(1000, 1000)).squeeze('band', drop=True)
                            outputs.append(data)
            return outputs

    def _warp_based_on_sample_dask(self, input_raster: str, nodata=None, sample_file=None,
                             resamplemethod='average', outdtype='float32'):

        with rasterio.open(f's3://{sample_file}') as src:

            out_meta = src.meta
            crs = out_meta['crs']
            transform = out_meta['transform']
            left = transform[2]
            top = transform[5]
            cols = out_meta['width']
            rows = out_meta['height']
            xres = transform[0]
            yres = transform[4]
            # return out_meta

        if resamplemethod == 'nearest':
            rs = Resampling.nearest
        elif resamplemethod == 'average':
            rs = Resampling.average
        elif resamplemethod == 'bilinear':
            rs = Resampling.bilinear
        else:
            print('only nearest-neighbor and average resampling is supported at this time')
            sys.exit(0)


        with rasterio.open(input_raster, 'r') as src:
            # create the virtual raster based on the standard rasterio
            # attributes from the sample tiff and shapefile feature.
            # update with suitable nodata values.
            nodata_val = nodata
            # src.nodata = nodata_val
            with WarpedVRT(src, resampling=rs,
                           crs=crs,
                           transform=transform,
                           height=rows,
                           width=cols,
                           dtype=outdtype) as vrt:
                 with dask.config.set(**{'array.slicing.split_large_chunks': True}):
                            # To avoid creating the large chunks, set the option
                            #     >>> with dask.config.set(**{'array.slicing.split_large_chunks': True}):
                            #     ...     array.reshape(shape)Explictly passing ``limit`` to ``reshape`` will also silence this warning
                            #     >>> array.reshape(shape, limit='128 MiB')
                            #   dsub = arr.coarsen(x=scaling_value, y=scaling_value, boundary='pad').mean()
                            data = riox.open_rasterio(vrt, chunks=(1000, 1000)).squeeze('band', drop=True)

        return data

    def _warp_based_on_sample(self, input_raster: str, temp_folder: str, outname: str, nodata=None, sample_file=None,
                             resamplemethod='average', outdtype='float32'):
        with rasterio.open(sample_file) as src:

            out_meta = src.meta
            crs = out_meta['crs']
            transform = out_meta['transform']
            left = transform[2]
            top = transform[5]
            cols = out_meta['width']
            rows = out_meta['height']
            xres = transform[0]
            yres = transform[4]
            # return out_meta

        if resamplemethod == 'nearest':
            rs = Resampling.nearest
        elif resamplemethod == 'average':
            rs = Resampling.average
        elif resamplemethod == 'bilinear':
            rs = Resampling.bilinear
        else:
            print('only nearest-neighbor and average resampling is supported at this time')
            sys.exit(0)

        with rasterio.open(input_raster, 'r') as src:
            # create the virtual raster based on the standard rasterio
            # attributes from the sample tiff and shapefile feature.
            # update with suitable nodata values.
            nodata_val = nodata
            # src.nodata = nodata_val
            with WarpedVRT(src, resampling=rs,
                           crs=crs,
                           transform=transform,
                           height=rows,
                           width=cols,
                           dtype=outdtype) as vrt:
                # save the file as an enumerated tiff. reopen outside this loop with the outputs list
                outwarp = os.path.join(temp_folder, outname)
                rio_shutil.copy(vrt, outwarp, driver='GTiff')

        return outwarp

if __name__ == "__main__":
    rm = RasterManager()

    lst = r'W:\Data\Temperature\Global\eVIIRS\2013\2013011.1_km_VIIRS_LST.tif'
    tmax = r'W:\Data\Temperature\Global\CHELSA\dekad_climatology\tmax_011.tif'
    ndvi = r'W:\Data\NDVI\Global\VIIRS\V001\2013_orig\2013001.1_km_16_days_NDVI.tif'

    lst, tmax, ndvi = rm._normalize_to_std_grid_dask(inputs=[lst, tmax, ndvi], sample_file=lst)

    print('='*20, 'lst', '='*20)
    print(lst)
    print('=' * 20, 'lst', '=' * 20)

    print('=' * 20, 'tmax', '=' * 20)
    print(tmax)
    print('=' * 20, 'tmax', '=' * 20)

    print('=' * 20, 'ndvi', '=' * 20)
    print(ndvi)
    print('=' * 20, 'ndvi', '=' * 20)
