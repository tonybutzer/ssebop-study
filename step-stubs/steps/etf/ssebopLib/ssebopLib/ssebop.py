import os
import numpy as np
# numpy type hinting: https://stackoverflow.com/questions/52839427/numpy-type-hints-in-python-pep-484
import xarray as xr
import rioxarray as rxr
from dask.distributed import Lock
from .raster_manager import RasterManager

class SSEBop():
    """This is the core SSEBop module"""

    # TODO - figure out how to make the temp_loc not necessary
    def __init__(self, dt, tmax, ndvi, lst, ndwi, global_water_mask, snow_mask,
                 water_mask, temp_loc: str, ref_et=None):
        """
        Implementation of the SSEBop model:

        Senay, G. B. (2018). "Satellite Psychrometric Formulation of the Operational Simplified Surface Energy Balance
        (SSEBop) Model for Quantifying and Mapping Evapotranspiration."
        Applied Engineering in Agriculture 34(3): 555-566

        :param dt: numpy array of dt; dT = (Rn*r_ah)/(C_p*rho_a) eq. A.5
        :param tmax: Max daily Air temperature
        :param ndvi: Normalized Difference Vegetation Index
        :param lst: Remotely Sensed Land Surface Temperature.
        :param refet: Gridded Reference ET
        :param cfactor: cfactor lst correction
        :param kwargs: dict, optional
            scalarmodel: {True, False}
            y : {}
            z : {}
        """

        # set etf and eta to false for the object because when instantiated, you haven't calculated it yet...
        self.temp = temp_loc
        self.etf = None
        self.eta = None
        # set refet if given, if not it will be None.
        self.ref_et = ref_et
        # set the standard and required inputs to
        self.dt = dt
        self.ndvi = ndvi
        self.tmax = tmax
        self.lst = lst
        self.ndwi= ndwi
        self.water_mask = water_mask
        self.global_water_mask = global_water_mask
        self.snow_mask = snow_mask

        print('heres the snow mask!!')
        print(snow_mask)

        # self.fano_fine_grid = fano_fine
        # self.fano_coarse_grid = fano_coarse
        # TODO - find a way to do this programatically based on the FANO resolution(s)
        self.trimmed_sizes = (14500, 37200)

        # initialize raster manager object
        self.rm = RasterManager()

        # self.calculate_etf(fano_grid=fano_fine, fano_coarse_grid=fano_coarse,
        #                    temp=temp_loc, hardcoded_sample_file=hardcoded_sample_file,
        #                    client=client, testing=test_mode)


    def calculate_etf(self, fano_grid=5, fano_coarse_grid=100, hardcoded_sample_file=None,
                      client=None, testing=False, high_mem=False):
        """
        Calculates ET fraction with FANO method from specified fine and coarse grid sizes.
        For more on the FANO method, see Senay et al. 2022 B.
        :rtype: object
        :param fano_grid: integer value of small FANO grid
        :param fano_coarse_grid: interger value of coarse FANO grid.
        :param temp: str temporary file location.
        :param hardcoded_sample_file: A file that is the right projection, resolution and extent for resampling to.
        :param client: Dask Client
        :param testing: If in testing mode, "True" intermediate rasters will be output.
        :param high_mem: if you have a high memory computer, some things will
        happen in-memory rather than being written out.
        """

        print('doing etf')

        # trim so that coarsen will work! have to be evenly divisible by the coarse and fine grid sizes.
        lst_ds = self.lst[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        ndvi_ds = self.ndvi[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        dt_ds = self.dt[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        tmax_ds = self.tmax[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        water_mask_ds = self.water_mask[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        snow_mask_ds = self.snow_mask[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        ndwi_ds = self.ndwi[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        global_water_mask = self.global_water_mask[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        # temp folder
        temp = self.temp

        # ===== FANO  WATER MASK ====

        # 1) make a binary mask of water (NDVIarr[NDVIarr < 0] == 1 else -> 0) using 1 km NDVI
        ndvi_waterbool = ndvi_ds < 0  # where water: True, where land: False...
        # set water pixels to one
        # https://docs.dask.org/en/latest/generated/dask.array.where.html
        # https://xarray.pydata.org/en/stable/generated/xarray.where.html
        # set land pixels to zero
        ndvi_watermask = xr.where(ndvi_waterbool, x=1,
                                  y=0)  # Where True, yield x, otherwise yield y (0 for land, 1 for water)
        ndvi_landmask = xr.where(~ndvi_waterbool, x=1, y=0)  # 1 for land, 0 for water
        # testing
        if testing:
            ndvi_landmask.rio.to_raster(os.path.join(temp, 'ndvi_landmask.tif'),
                                        tiled=True, lock=Lock('rio', client=client))

        #  make a mask from NDWI also
        ndwi_water_bool = ndwi_ds > 0.12  # # where water: True, where land: false
        # set Land pixels to zero
        ndwi_water_mask = xr.where(ndwi_water_bool, x=1, y=0)  # 1 for water 0 for land
        ndwi_land_mask = xr.where(~ndwi_water_bool, x=1, y=0)  # 0 for water, 1 for land
        # # testing
        if testing:
            ndwi_land_mask.rio.to_raster(os.path.join(temp, 'ndwi_landmask.tif'),
                                         tiled=True, lock=Lock('rio', client=client))

        # QA water mask
        qa_water_mask_bool = water_mask_ds == 1
        qa_land_mask = xr.where(qa_water_mask_bool, x=1, y=0)  # 0 for water, 1 for land
        qa_water_mask = xr.where(~qa_water_mask_bool, x=1, y=0)  # 1 for water, 0 for land
        # QA snow/ice mask
        snow_mask_bool = snow_mask_ds == 1
        qa_snowfree_mask = xr.where(snow_mask_bool, x=1, y=0)  # 1 for NO SNOW and NO ICE, 0 for snow/ice
        # testing
        if testing:
            qa_snowfree_mask.rio.to_raster(os.path.join(temp, 'qa_snowfree_mask.tif'),
                                           tiled=True, lock=Lock('rio', client=client))

        # Combine the different water makss.
        positive_land = qa_land_mask * ndwi_land_mask * ndvi_landmask
        # # # testing
        # positive_land.rio.to_raster(os.path.join(temp, 'all_clear_land.tif'),
        #                              tiled=True, lock=Lock('rio', client=client))
        positive_water = qa_water_mask + ndwi_water_mask + ndvi_watermask  # if you multiply, the values will cancel out incorrectly if they disagree. we want any one to trigger water id
        positive_water = xr.where(positive_water >= 1, x=1, y=0)
        # # # testing
        if testing:
            positive_water.rio.to_raster(os.path.join(temp, 'positive_water.tif'),
                                         tiled=True, lock=Lock('rio', client=client))

        # ====================================================================================================================
        # # make the land mask out of the inverse of the positive water mask, then apply the qa_snow_ (which, looks good)

        snow_free_water = positive_water * qa_snowfree_mask
        snow_free_land = positive_land * qa_snowfree_mask

        # # # 2) reduce binary mask with 'sum' to 25km resolution. This is the 'bad pixels count'
        # # use Rich's algorithm to handle the dask array # todo -test to confirm
        water_sum_25 = self.rm._coarsen_dask_arr(snow_free_water, scaling_value=fano_grid, resample_alg='sum')
        # # testing
        if testing:
            water_sum_25.rio.to_raster(os.path.join(temp, 'water_sum.tif'),
                                       tiled=True, lock=Lock('rio', client=client))

        # 3) reduce antiwatermask with 'sum' to 5km resolution. This is the 'good pixels count'
        land_sum_25 = self.rm._coarsen_dask_arr(snow_free_land, scaling_value=fano_grid, resample_alg='sum')
        # # testing
        if testing:
            land_sum_25.rio.to_raster(os.path.join(temp, 'land_sum.tif'),
                                      tiled=True, lock=Lock('rio', client=client))

        # 4) divide both 5km rasters and X100 for percent bad/total depending on the denominator
        total_pixels = land_sum_25 + water_sum_25  # is this right?
        # using this layer above a certian threshold the values will be replaced with 100km tcorr...
        water_percent = (water_sum_25 / total_pixels) * 100
        # # testing
        if testing:
            water_percent.rio.to_raster(os.path.join(temp, 'water_percent.tif'),
                                        tiled=True, lock=Lock('rio', client=client))

        # the watermask is the 1km resolution watermask, in this case, since we only use NDVI
        # we consider the ndvi_watermask = watermask.
        # NOTE: "antiwatermask" is the one we want bc antiwatermask has land=1, water=0
        # if we add more criteria later on, the water mask will combine ndvi_antiwatermask and "x_watermask"
        watermask = snow_free_land  # land is 1, water is 0

        # MASKING
        # https://docs.xarray.dev/en/stable/user-guide/indexing.html?highlight=masking#masking-with-where
        ndvi_ds_masked = ndvi_ds.where(watermask == 1)
        lst_ds_masked = lst_ds.where(watermask == 1)

        # ===== FANO PREP ====

        # the below resampling is done by...
        # ...using rasterio virtualwarp with rioxarray and Dask
        # squeeze().drop() is built into the resample_daskVRT functions so all good.
        # lst and NDVI masked - we don't want water in the FANO expression
        # @ 25km
        lst_ds25_masked = self.rm._coarsen_dask_arr(lst_ds_masked, fano_grid)
        # # # testing
        if testing:
            lst_ds25_masked.rio.to_raster(os.path.join(temp, 'lst_ds25_masked.tif'),
                                          tiled=True, lock=Lock('rio', client=client))
        ndvi_ds25_masked = self.rm._coarsen_dask_arr(ndvi_ds_masked, fano_grid)
        # @ 100 km
        lst_ds100_masked = self.rm._coarsen_dask_arr(lst_ds_masked, fano_coarse_grid)
        # # # testing
        if testing:
            lst_ds100_masked.rio.to_raster(os.path.join(temp, 'lst_ds100_masked.tif'),
                                           tiled=True, lock=Lock('rio', client=client))
        ndvi_ds100_masked = self.rm._coarsen_dask_arr(ndvi_ds_masked, fano_coarse_grid)

        # lst unmasked - we use this at the very end to assign
        # regular NON FANO 25km average LST to water areas i.e. NDVI<0.
        # @ 25km only
        lst_ds25_unmasked = self.rm._coarsen_dask_arr(lst_ds, fano_grid)
        # # # testing
        if testing:
            lst_ds25_unmasked.rio.to_raster(os.path.join(temp, 'lst_ds25_unmasked.tif'),
                                            tiled=True, lock=Lock('rio', client=client))
        ndvi_ds25_unmasked = self.rm._coarsen_dask_arr(ndvi_ds, fano_grid)

        # get dt at 25km and 100km. Don't need masking. (Mask=None by default.)
        dt_ds25 = self.rm._coarsen_dask_arr(dt_ds, fano_grid)
        dt_ds100 = self.rm._coarsen_dask_arr(dt_ds, fano_coarse_grid)
        # get tmax at 25km only, no masking here either
        tmax_ds25 = self.rm._coarsen_dask_arr(tmax_ds, fano_grid)
        # # testing
        if testing:
            tmax_ds25.rio.to_raster(os.path.join(temp, 'tmax_ds25.tif'),
                                    tiled=True, lock=Lock('rio', client=client))

        # ========================= Calculate FANO Tcold at 25km and 100km =========================
        fano_constant = 1.25
        # make the threshold into a xrDask dataset
        high_ndvi_threshold = 0.9
        # water threshold beyond which, a pixel is too wet to be used for c-factor...(%)
        water_threshold = 75

        tcold_FANO_25 = (lst_ds25_masked - (fano_constant * dt_ds25 *
                                            (high_ndvi_threshold - ndvi_ds25_masked)))
        tcold_FANO_100 = (lst_ds100_masked - (fano_constant * dt_ds100
                                              * (high_ndvi_threshold - ndvi_ds100_masked)))
        # Testing
        if testing:
            tcold_FANO_25.rio.to_raster(os.path.join(temp, 'tcold_fano_25.tif'),
                                        tiled=True, lock=Lock('rio', client=client))
        if testing:
            tcold_FANO_100.rio.to_raster(os.path.join(temp, 'tcold_fano_100.tif'),
                                         tiled=True, lock=Lock('rio', client=client))

        # GELP 11_22 Commented out bc we had a memory error here...
        # smooth the very coarse 100km fano, that way it doesn't create huge artifacts...is it a good idea?!
        smoothing = False
        # by default we don't do it
        if smoothing:
            tcold_FANO_100.load()
            tcold_FANO_100_smooth = tcold_FANO_100.interp(y=dt_ds100['y'], x=dt_ds100['x'], method='linear')
            tcold_FANO_100_smooth.load()
            tcold_FANO_100_25 = tcold_FANO_100_smooth.interp(y=tcold_FANO_25['y'], x=tcold_FANO_25['x'],
                                                             method='nearest')
        else:
            # Prep step: get tcold FANO 100 at 25km to easily do masking
            # https://gis.stackexchange.com/questions/339463/using-xarray-to-resample-and-merge-two-datasets
            # must load first
            tcold_FANO_100.load()
            tcold_FANO_100_25 = tcold_FANO_100.interp(y=tcold_FANO_25['y'], x=tcold_FANO_25['x'], method='nearest')
        # # testing
        if testing:
            tcold_FANO_100_25.rio.to_raster(os.path.join(temp, 'tcold_fano_100_25.tif'),
                                            tiled=True, lock=Lock('rio', client=client))

        # ====== MERGING LAYERS of FANO Based on Conditions =====
        # using .fillna() from xarray
        # https://xarray.pydata.org/en/stable/generated/xarray.DataArray.fillna.html
        # 1) lst_avg_25 where ndvi_25_masked > 0.9
        # use masked LST bc at this stage we are still avoiding water...
        lst_ds25_high_ndvi = lst_ds25_masked.where(ndvi_ds25_masked >= high_ndvi_threshold)
        # # testing
        # if not os.path.exists(os.path.join(temp, 'lst_ds25_high_ndvi.tif')):
        #     lst_ds25_high_ndvi.rio.to_raster(os.path.join(temp, 'lst_ds25_high_ndvi.tif'),
        #                             tiled=True, lock=Lock('rio', client=client))

        # 2) Mask tcold FANO at 25km based on the water percentage...
        tcold_FANO_25_wetregion_mask = tcold_FANO_25.where(water_percent < water_threshold)
        # # testing
        if testing:
            tcold_FANO_25_wetregion_mask.rio.to_raster(os.path.join(temp, 'tcold_fano_25_wetregion.tif'),
                                                       tiled=True, lock=Lock('rio', client=client))
        # ====================
        # --- META steps 1 and 2 ---
        # This step combines lst (non-FANO) from high ndvi areas, with FANO-LST from ...
        # ...areas that ARE NOT wet.
        coldfano_and_highndvi = lst_ds25_high_ndvi.fillna(tcold_FANO_25_wetregion_mask)
        # # testing
        if testing:
            coldfano_and_highndvi.rio.to_raster(os.path.join(temp, 'coldfano_and_highndvi.tif'),
                                                tiled=True, lock=Lock('rio', client=client))

        # --- steps 3 and 4 ---
        # combining 100km FANO with 25km unmasked LST to fill gaps...
        # ... The unmaksed gap-filling LST is areas that are cold, supposedly bc of water...
        # TODO GS and GELP - Do we need to make sure that lst_ds25_unmasked is used wherever waterperecet > 10% and not just as a backup for 100km FANO cold?
        cold_fano_100_and_rawlst = tcold_FANO_100_25.fillna(lst_ds25_unmasked)
        # testing
        if testing:
            cold_fano_100_and_rawlst.rio.to_raster(os.path.join(temp, 'cold_fano_100_and_rawlst.tif'),
                                                   tiled=True, lock=Lock('rio', client=client))
        # !!! combine both! Get Tc !!!
        tcold = coldfano_and_highndvi.fillna(cold_fano_100_and_rawlst)
        # testing
        if testing:
            tcold.rio.to_raster(os.path.join(temp, 'tcold.tif'),
                                tiled=True, lock=Lock('rio', client=client))

        # Finally we arrive at a c factor!!!!
        cfactor = tcold / tmax_ds25

        if high_mem:
            # it is not particularly efficient.
            # Even with 64 gigs of VDI memory getting memory warnings. Maybe not optimized here...
            # # THIS STEP is suffering from a memory error so we change to VRT... You can execute the .interp() if...
            # # ...you use a 'nearest' neighbor method instead of 'linear'.
            # # does rechunking help?
            # cfactor = cfactor.chunk(chunks={'x':1492, 'y': 6})
            # bilinearly smooth the c factor down to 1km
            # https://gis.stackexchange.com/questions/339463/using-xarray-to-resample-and-merge-two-datasets
            # must load - luckily this is not a big file :-)
            cfactor.load()
            cfactor_bilinear_ds = cfactor.interp(y=tmax_ds['y'], x=tmax_ds['x'], method='linear')

        else:
            print('writing cfactor \n', cfactor)
            cfactor.rio.to_raster(os.path.join(temp, 'cfactor.tif'),
                                  tiled=True, lock=Lock('rio', client=client))
            cfactor_path = os.path.join(temp, 'cfactor.tif')
            print('cfactor written !\n', )

            # # todo- This should work but doesn't... tcold1km comes out all warped and effed up.
            # cfactor_bilinear_ds = resample_daskVRT(cfactor_path, scaling_value=(1/fano_grid), resample_alg='bilinear', nodata_val=None)

            cfactor_bilinear_path = self.rm._warp_based_on_sample(cfactor_path, temp_folder=temp,
                                                         outname='cfactor_bilinear.tif',
                                                         sample_file=hardcoded_sample_file,
                                                         resamplemethod='bilinear')

            # cfactor_bilinear_path = os.path.join(temp, 'cfactor_bilinear.tif')
            cfactor_bilinear_ds = rxr.open_rasterio(cfactor_bilinear_path, masked=True, chunks=True).squeeze().drop(
                labels='band')
            cfactor_bilinear_ds = cfactor_bilinear_ds[:14500, :37200]
            print('the resampled dataset\n', cfactor_bilinear_ds)

        # # todo - try https://stackoverflow.com/questions/69584244/wrong-raster-format-when-multiplying-rasters-using-rioxarray
        # # are the rasters aligned?
        # print('checking for alignment')
        # try:
        #     xr.align(cfactor_bilinear_ds, tmax_ds, join='exact')
        #     print('they were aligned to begin with!')
        # except ValueError:
        #     print('we got a value error, they are not aligned, now do an alighnment')
        #     cfactor_bilinear_ds, tmax_ds = xr.align(cfactor_bilinear_ds, tmax_ds, join='inner')
        #     # Testing if the align worked...
        #     try:
        #         xr.align(cfactor_bilinear_ds, tmax_ds, join='exact')
        #         print('the align worked!')
        #     except ValueError:
        #         print('we still got a value error. We appearently havent changed the rasters.')

        # testing
        if testing:
            cfactor_bilinear_ds.rio.to_raster(os.path.join(temp, 'cfactor_bilinear_ds.tif'),
                                              tiled=True, lock=Lock('rio', client=client))
            tmax_ds.rio.to_raster(os.path.join(temp, 'tmax_ds.tif'),
                                  tiled=True, lock=Lock('rio', client=client))

        tcold_1km = cfactor_bilinear_ds * tmax_ds
        # testing
        if testing:
            tcold_1km.rio.to_raster(os.path.join(temp, 'tcold_1km.tif'),
                                    tiled=True, lock=Lock('rio', client=client))

        # # === etf calculation w non bilinear ===
        # # 1 - Create T cold
        # tcold_1km = cfactor * tmax_ds
        # # testing
        # tcold_1km.rio.to_raster(os.path.join(temp, 'tcold_1km.tif'),
        #                         tiled=True, lock=Lock('rio', client=client))

        # MAYBE?!
        # 2 - Create T hot with gray-sky dT raster (OLD FORMULATION)
        # 3 - Albedo correction on LST ?
        # 4 - Emissivity correction on albedo-corrected LST ?

        # 5 - Calculate ETf

        # get rid of snow # TODO - SNOW
        lst_ds = lst_ds.where(qa_snowfree_mask == 1)
        # # testing
        # lst_ds.rio.to_raster(os.path.join(temp, 'lst_ds_snowmasked.tif'),
        #                         tiled=True, lock=Lock('rio', client=client))

        print('ET fraction calculation')
        # # OLD formulation
        # etf_raw = (t_hot - lst_ds) / dt_ds
        # NEW formulation from recent publication.
        etf_raw = 1 - ((lst_ds - tcold_1km) / dt_ds)
        # testing
        if testing:
            etf_raw.rio.to_raster(os.path.join(temp, 'etf_raw.tif'),
                                  tiled=True, lock=Lock('rio', client=client))

        # use the global water mask to make sure that water bodies (always water for all time)...
        # ...are given 0.85 ETf in the end.
        etf_raw = xr.where(global_water_mask == 1, 0.85, etf_raw)
        # give fake values of 1 ETf to SNOW
        etf_raw = xr.where(qa_snowfree_mask == 0, 1.0, etf_raw)
        # get rid of values below 0 and above 1.2, for they are erroneous.
        etf_1 = xr.where(etf_raw < 0, 0, etf_raw)
        etf_final = xr.where(etf_raw > 1.0, 1.0, etf_1)

        self.etf = etf_final
        # etf_final.rio.to_raster(os.path.join(output_location, etf_name),
        #                         tiled=True, lock=Lock('rio', client=client))

    def calculate_etf_experimental(self, fano_grid=5, fano_coarse_grid=100, hardcoded_sample_file=None,
                      client=None, testing=False, high_mem=False):
        """
        This version has NDWI masking disabled

        Calculates ET fraction with FANO method from specified fine and coarse grid sizes.
        For more on the FANO method, see Senay et al. 2022 B.
        :rtype: object
        :param fano_grid: integer value of small FANO grid
        :param fano_coarse_grid: interger value of coarse FANO grid.
        :param temp: str temporary file location.
        :param hardcoded_sample_file: A file that is the right projection, resolution and extent for resampling to.
        :param client: Dask Client
        :param testing: If in testing mode, "True" intermediate rasters will be output.
        :param high_mem: if you have a high memory computer, some things will
        happen in-memory rather than being written out.
        """

        print('doing etf')

        # trim so that coarsen will work! have to be evenly divisible by the coarse and fine grid sizes.
        lst_ds = self.lst[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        ndvi_ds = self.ndvi[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        dt_ds = self.dt[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        tmax_ds = self.tmax[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        water_mask_ds = self.water_mask[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        snow_mask_ds = self.snow_mask[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        ndwi_ds = self.ndwi[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        global_water_mask = self.global_water_mask[:self.trimmed_sizes[0], :self.trimmed_sizes[1]]
        # temp folder
        temp = self.temp

        # ===== FANO  WATER MASK ====

        # 1) make a binary mask of water (NDVIarr[NDVIarr < 0] == 1 else -> 0) using 1 km NDVI
        ndvi_waterbool = ndvi_ds < 0  # where water: True, where land: False...
        # set water pixels to one
        # https://docs.dask.org/en/latest/generated/dask.array.where.html
        # https://xarray.pydata.org/en/stable/generated/xarray.where.html
        # set land pixels to zero
        ndvi_watermask = xr.where(ndvi_waterbool, x=1,
                                  y=0)  # Where True, yield x, otherwise yield y (0 for land, 1 for water)
        ndvi_landmask = xr.where(~ndvi_waterbool, x=1, y=0)  # 1 for land, 0 for water
        # testing
        if testing:
            ndvi_landmask.rio.to_raster(os.path.join(temp, 'ndvi_landmask.tif'),
                                        tiled=True, lock=Lock('rio', client=client))

        #  make a mask from NDWI also
        ndwi_water_bool = ndwi_ds > 0.12  # # where water: True, where land: false
        # set Land pixels to zero
        ndwi_water_mask = xr.where(ndwi_water_bool, x=1, y=0)  # 1 for water 0 for land
        ndwi_land_mask = xr.where(~ndwi_water_bool, x=1, y=0)  # 0 for water, 1 for land
        # # testing
        if testing:
            ndwi_land_mask.rio.to_raster(os.path.join(temp, 'ndwi_landmask.tif'),
                                         tiled=True, lock=Lock('rio', client=client))

        # QA water mask
        qa_water_mask_bool = water_mask_ds == 1
        qa_land_mask = xr.where(qa_water_mask_bool, x=1, y=0)  # 0 for water, 1 for land
        qa_water_mask = xr.where(~qa_water_mask_bool, x=1, y=0)  # 1 for water, 0 for land
        # QA snow/ice mask
        snow_mask_bool = snow_mask_ds == 1
        qa_snowfree_mask = xr.where(snow_mask_bool, x=1, y=0)  # 1 for NO SNOW and NO ICE, 0 for snow/ice
        # testing
        if testing:
            qa_snowfree_mask.rio.to_raster(os.path.join(temp, 'qa_snowfree_mask.tif'),
                                           tiled=True, lock=Lock('rio', client=client))

        # Combine the different water makss.
        positive_land = qa_land_mask * ndvi_landmask
        # # # testing
        # positive_land.rio.to_raster(os.path.join(temp, 'all_clear_land.tif'),
        #                              tiled=True, lock=Lock('rio', client=client))
        positive_water = qa_water_mask + ndvi_watermask  # if you multiply, the values will cancel out incorrectly if they disagree. we want any one to trigger water id
        positive_water = xr.where(positive_water >= 1, x=1, y=0)
        # # # testing
        if testing:
            positive_water.rio.to_raster(os.path.join(temp, 'positive_water.tif'),
                                         tiled=True, lock=Lock('rio', client=client))

        # ====================================================================================================================
        # # make the land mask out of the inverse of the positive water mask, then apply the qa_snow_ (which, looks good)

        snow_free_water = positive_water * qa_snowfree_mask
        snow_free_land = positive_land * qa_snowfree_mask

        # # # 2) reduce binary mask with 'sum' to 25km resolution. This is the 'bad pixels count'
        # # use Rich's algorithm to handle the dask array # todo -test to confirm
        water_sum_25 = self.rm._coarsen_dask_arr(snow_free_water, scaling_value=fano_grid, resample_alg='sum')
        # # testing
        if testing:
            water_sum_25.rio.to_raster(os.path.join(temp, 'water_sum.tif'),
                                       tiled=True, lock=Lock('rio', client=client))

        # 3) reduce antiwatermask with 'sum' to 5km resolution. This is the 'good pixels count'
        land_sum_25 = self.rm._coarsen_dask_arr(snow_free_land, scaling_value=fano_grid, resample_alg='sum')
        # # testing
        if testing:
            land_sum_25.rio.to_raster(os.path.join(temp, 'land_sum.tif'),
                                      tiled=True, lock=Lock('rio', client=client))

        # 4) divide both 5km rasters and X100 for percent bad/total depending on the denominator
        total_pixels = land_sum_25 + water_sum_25  # is this right?
        # using this layer above a certian threshold the values will be replaced with 100km tcorr...
        water_percent = (water_sum_25 / total_pixels) * 100
        # # testing
        if testing:
            water_percent.rio.to_raster(os.path.join(temp, 'water_percent.tif'),
                                        tiled=True, lock=Lock('rio', client=client))

        # the watermask is the 1km resolution watermask, in this case, since we only use NDVI
        # we consider the ndvi_watermask = watermask.
        # NOTE: "antiwatermask" is the one we want bc antiwatermask has land=1, water=0
        # if we add more criteria later on, the water mask will combine ndvi_antiwatermask and "x_watermask"
        watermask = snow_free_land  # land is 1, water is 0

        # MASKING
        # https://docs.xarray.dev/en/stable/user-guide/indexing.html?highlight=masking#masking-with-where
        ndvi_ds_masked = ndvi_ds.where(watermask == 1)
        lst_ds_masked = lst_ds.where(watermask == 1)

        # ===== FANO PREP ====

        # the below resampling is done by...
        # ...using rasterio virtualwarp with rioxarray and Dask
        # squeeze().drop() is built into the resample_daskVRT functions so all good.
        # lst and NDVI masked - we don't want water in the FANO expression
        # @ 25km
        lst_ds25_masked = self.rm._coarsen_dask_arr(lst_ds_masked, fano_grid)
        # # # testing
        if testing:
            lst_ds25_masked.rio.to_raster(os.path.join(temp, 'lst_ds25_masked.tif'),
                                          tiled=True, lock=Lock('rio', client=client))
        ndvi_ds25_masked = self.rm._coarsen_dask_arr(ndvi_ds_masked, fano_grid)
        # @ 100 km
        lst_ds100_masked = self.rm._coarsen_dask_arr(lst_ds_masked, fano_coarse_grid)
        # # # testing
        if testing:
            lst_ds100_masked.rio.to_raster(os.path.join(temp, 'lst_ds100_masked.tif'),
                                           tiled=True, lock=Lock('rio', client=client))
        ndvi_ds100_masked = self.rm._coarsen_dask_arr(ndvi_ds_masked, fano_coarse_grid)

        # lst unmasked - we use this at the very end to assign
        # regular NON FANO 25km average LST to water areas i.e. NDVI<0.
        # @ 25km only
        lst_ds25_unmasked = self.rm._coarsen_dask_arr(lst_ds, fano_grid)
        # # # testing
        if testing:
            lst_ds25_unmasked.rio.to_raster(os.path.join(temp, 'lst_ds25_unmasked.tif'),
                                            tiled=True, lock=Lock('rio', client=client))
        ndvi_ds25_unmasked = self.rm._coarsen_dask_arr(ndvi_ds, fano_grid)

        # get dt at 25km and 100km. Don't need masking. (Mask=None by default.)
        dt_ds25 = self.rm._coarsen_dask_arr(dt_ds, fano_grid)
        dt_ds100 = self.rm._coarsen_dask_arr(dt_ds, fano_coarse_grid)
        # get tmax at 25km only, no masking here either
        tmax_ds25 = self.rm._coarsen_dask_arr(tmax_ds, fano_grid)
        # # testing
        if testing:
            tmax_ds25.rio.to_raster(os.path.join(temp, 'tmax_ds25.tif'),
                                    tiled=True, lock=Lock('rio', client=client))

        # ========================= Calculate FANO Tcold at 25km and 100km =========================
        fano_constant = 1.25
        # make the threshold into a xrDask dataset
        high_ndvi_threshold = 0.9
        # water threshold beyond which, a pixel is too wet to be used for c-factor...(%)
        water_threshold = 75

        tcold_FANO_25 = (lst_ds25_masked - (fano_constant * dt_ds25 *
                                            (high_ndvi_threshold - ndvi_ds25_masked)))
        tcold_FANO_100 = (lst_ds100_masked - (fano_constant * dt_ds100
                                              * (high_ndvi_threshold - ndvi_ds100_masked)))
        # Testing
        if testing:
            tcold_FANO_25.rio.to_raster(os.path.join(temp, 'tcold_fano_25.tif'),
                                        tiled=True, lock=Lock('rio', client=client))
        if testing:
            tcold_FANO_100.rio.to_raster(os.path.join(temp, 'tcold_fano_100.tif'),
                                         tiled=True, lock=Lock('rio', client=client))

        # GELP 11_22 Commented out bc we had a memory error here...
        # smooth the very coarse 100km fano, that way it doesn't create huge artifacts...is it a good idea?!
        smoothing = False
        # by default we don't do it
        if smoothing:
            tcold_FANO_100.load()
            tcold_FANO_100_smooth = tcold_FANO_100.interp(y=dt_ds100['y'], x=dt_ds100['x'], method='linear')
            tcold_FANO_100_smooth.load()
            tcold_FANO_100_25 = tcold_FANO_100_smooth.interp(y=tcold_FANO_25['y'], x=tcold_FANO_25['x'],
                                                             method='nearest')
        else:
            # Prep step: get tcold FANO 100 at 25km to easily do masking
            # https://gis.stackexchange.com/questions/339463/using-xarray-to-resample-and-merge-two-datasets
            # must load first
            tcold_FANO_100.load()
            tcold_FANO_100_25 = tcold_FANO_100.interp(y=tcold_FANO_25['y'], x=tcold_FANO_25['x'], method='nearest')
        # # testing
        if testing:
            tcold_FANO_100_25.rio.to_raster(os.path.join(temp, 'tcold_fano_100_25.tif'),
                                            tiled=True, lock=Lock('rio', client=client))

        # ====== MERGING LAYERS of FANO Based on Conditions =====
        # using .fillna() from xarray
        # https://xarray.pydata.org/en/stable/generated/xarray.DataArray.fillna.html
        # 1) lst_avg_25 where ndvi_25_masked > 0.9
        # use masked LST bc at this stage we are still avoiding water...
        lst_ds25_high_ndvi = lst_ds25_masked.where(ndvi_ds25_masked >= high_ndvi_threshold)
        # # testing
        # if not os.path.exists(os.path.join(temp, 'lst_ds25_high_ndvi.tif')):
        #     lst_ds25_high_ndvi.rio.to_raster(os.path.join(temp, 'lst_ds25_high_ndvi.tif'),
        #                             tiled=True, lock=Lock('rio', client=client))

        # 2) Mask tcold FANO at 25km based on the water percentage...
        tcold_FANO_25_wetregion_mask = tcold_FANO_25.where(water_percent < water_threshold)
        # # testing
        if testing:
            tcold_FANO_25_wetregion_mask.rio.to_raster(os.path.join(temp, 'tcold_fano_25_wetregion.tif'),
                                                       tiled=True, lock=Lock('rio', client=client))
        # ====================
        # --- META steps 1 and 2 ---
        # This step combines lst (non-FANO) from high ndvi areas, with FANO-LST from ...
        # ...areas that ARE NOT wet.
        coldfano_and_highndvi = lst_ds25_high_ndvi.fillna(tcold_FANO_25_wetregion_mask)
        # # testing
        if testing:
            coldfano_and_highndvi.rio.to_raster(os.path.join(temp, 'coldfano_and_highndvi.tif'),
                                                tiled=True, lock=Lock('rio', client=client))

        # --- steps 3 and 4 ---
        # combining 100km FANO with 25km unmasked LST to fill gaps...
        # ... The unmaksed gap-filling LST is areas that are cold, supposedly bc of water...
        # TODO GS and GELP - Do we need to make sure that lst_ds25_unmasked is used wherever waterperecet > 10% and not just as a backup for 100km FANO cold?
        cold_fano_100_and_rawlst = tcold_FANO_100_25.fillna(lst_ds25_unmasked)
        # testing
        if testing:
            cold_fano_100_and_rawlst.rio.to_raster(os.path.join(temp, 'cold_fano_100_and_rawlst.tif'),
                                                   tiled=True, lock=Lock('rio', client=client))
        # !!! combine both! Get Tc !!!
        tcold = coldfano_and_highndvi.fillna(cold_fano_100_and_rawlst)
        # testing
        if testing:
            tcold.rio.to_raster(os.path.join(temp, 'tcold.tif'),
                                tiled=True, lock=Lock('rio', client=client))

        # Finally we arrive at a c factor!!!!
        cfactor = tcold / tmax_ds25

        if high_mem:
            # it is not particularly efficient.
            # Even with 64 gigs of VDI memory getting memory warnings. Maybe not optimized here...
            # # THIS STEP is suffering from a memory error so we change to VRT... You can execute the .interp() if...
            # # ...you use a 'nearest' neighbor method instead of 'linear'.
            # # does rechunking help?
            # cfactor = cfactor.chunk(chunks={'x':1492, 'y': 6})
            # bilinearly smooth the c factor down to 1km
            # https://gis.stackexchange.com/questions/339463/using-xarray-to-resample-and-merge-two-datasets
            # must load - luckily this is not a big file :-)
            cfactor.load()
            cfactor_bilinear_ds = cfactor.interp(y=tmax_ds['y'], x=tmax_ds['x'], method='linear')

        else:
            print('writing cfactor \n', cfactor)
            cfactor.rio.to_raster(os.path.join(temp, 'cfactor.tif'),
                                  tiled=True, lock=Lock('rio', client=client))
            cfactor_path = os.path.join(temp, 'cfactor.tif')
            print('cfactor written !\n', )

            # # todo- This should work but doesn't... tcold1km comes out all warped and effed up.
            # cfactor_bilinear_ds = resample_daskVRT(cfactor_path, scaling_value=(1/fano_grid), resample_alg='bilinear', nodata_val=None)

            cfactor_bilinear_path = self.rm._warp_based_on_sample(cfactor_path, temp_folder=temp,
                                                         outname='cfactor_bilinear.tif',
                                                         sample_file=hardcoded_sample_file,
                                                         resamplemethod='bilinear')

            # cfactor_bilinear_path = os.path.join(temp, 'cfactor_bilinear.tif')
            cfactor_bilinear_ds = rxr.open_rasterio(cfactor_bilinear_path, masked=True, chunks=True).squeeze().drop(
                labels='band')
            cfactor_bilinear_ds = cfactor_bilinear_ds[:14500, :37200]
            print('the resampled dataset\n', cfactor_bilinear_ds)

        # # todo - try https://stackoverflow.com/questions/69584244/wrong-raster-format-when-multiplying-rasters-using-rioxarray
        # # are the rasters aligned?
        # print('checking for alignment')
        # try:
        #     xr.align(cfactor_bilinear_ds, tmax_ds, join='exact')
        #     print('they were aligned to begin with!')
        # except ValueError:
        #     print('we got a value error, they are not aligned, now do an alighnment')
        #     cfactor_bilinear_ds, tmax_ds = xr.align(cfactor_bilinear_ds, tmax_ds, join='inner')
        #     # Testing if the align worked...
        #     try:
        #         xr.align(cfactor_bilinear_ds, tmax_ds, join='exact')
        #         print('the align worked!')
        #     except ValueError:
        #         print('we still got a value error. We appearently havent changed the rasters.')

        # testing
        if testing:
            cfactor_bilinear_ds.rio.to_raster(os.path.join(temp, 'cfactor_bilinear_ds.tif'),
                                              tiled=True, lock=Lock('rio', client=client))
            tmax_ds.rio.to_raster(os.path.join(temp, 'tmax_ds.tif'),
                                  tiled=True, lock=Lock('rio', client=client))

        tcold_1km = cfactor_bilinear_ds * tmax_ds
        # testing
        if testing:
            tcold_1km.rio.to_raster(os.path.join(temp, 'tcold_1km.tif'),
                                    tiled=True, lock=Lock('rio', client=client))

        # # === etf calculation w non bilinear ===
        # # 1 - Create T cold
        # tcold_1km = cfactor * tmax_ds
        # # testing
        # tcold_1km.rio.to_raster(os.path.join(temp, 'tcold_1km.tif'),
        #                         tiled=True, lock=Lock('rio', client=client))

        # MAYBE?!
        # 2 - Create T hot with gray-sky dT raster (OLD FORMULATION)
        # 3 - Albedo correction on LST ?
        # 4 - Emissivity correction on albedo-corrected LST ?

        # 5 - Calculate ETf

        # get rid of snow # TODO - SNOW
        lst_ds = lst_ds.where(qa_snowfree_mask == 1)
        # # testing
        # lst_ds.rio.to_raster(os.path.join(temp, 'lst_ds_snowmasked.tif'),
        #                         tiled=True, lock=Lock('rio', client=client))

        print('ET fraction calculation')
        # # OLD formulation
        # etf_raw = (t_hot - lst_ds) / dt_ds
        # NEW formulation from recent publication.
        etf_raw = 1 - ((lst_ds - tcold_1km) / dt_ds)
        # testing
        if testing:
            etf_raw.rio.to_raster(os.path.join(temp, 'etf_raw.tif'),
                                  tiled=True, lock=Lock('rio', client=client))

        # use the global water mask to make sure that water bodies (always water for all time)...
        # ...are given 0.85 ETf in the end.
        etf_raw = xr.where(global_water_mask == 1, 0.85, etf_raw)
        # give fake values of 1 ETf to SNOW
        etf_raw = xr.where(qa_snowfree_mask == 0, 1.0, etf_raw)
        # get rid of values below 0 and above 1.2, for they are erroneous.
        etf_1 = xr.where(etf_raw < 0, 0, etf_raw)
        etf_final = xr.where(etf_raw > 1.0, 1.0, etf_1)

        self.etf = etf_final
        # etf_final.rio.to_raster(os.path.join(output_location, etf_name),
        #                         tiled=True, lock=Lock('rio', client=client))


    def set_refet(self, ref_et):
        """Refet array"""
        if self.ref_et is None:
            print(f'setting RefET from {self.ref_et} to arr of size {ref_et.shape}')
        else:
            print('changing/overwriting ref_et attribute')
        self.ref_et = ref_et

    # another class handles this?
    def calculate_eta(self):
        """
        Senay 2013 eq 1
        :return:
        """

        self.eta = self.ref_et * self.etf
