

class FANO():
    """Handles produces FANO-related outputs for SSEBop algorithm."""

    def __init__(self, ):
        print('bubuhbubububu')


def ssebop_etf(lst: str, ndvi: str, dt: str, tmax: str, water_mask: str,
               snow_mask: str, ndwi_layer: str, client, output_location,
               hardcoded_sample_file=r'W:\Data\NDVI\Global\VIIRS\NDVI\dekadal\2018\2018011.1_km_VIIRS_NDVI.tif',
               viirs_resolution=1, fano_grid=25, fano_coarse_grid=100, high_mem=False, testing=False):

    etf_year = lst.split('.')[0][-4:]
    etf_dekad = tmax.split('.')[0][-3:]

    # make a temp file: (will only execute the first time if it's not already extant...)
    temp = os.path.join(output_location, 'temp'+etf_dekad)
    if not os.path.exists(temp):
        os.mkdir(temp)

    etf_name = f'etf_{etf_year}{etf_dekad}.tif'

    # standardize the grid.
    lst_ds, ndvi_ds, dt_ds, tmax_ds, water_mask_ds, snow_mask_ds, ndwi_ds = normalize_to_std_grid_dask([lst, ndvi, dt, tmax, water_mask,
                                                     snow_mask, ndwi_layer],
                                                temp_folder=temp,
                                                nodatas=[None, None, None, None, None, None, None],
                                                sample_file=hardcoded_sample_file,
                                                resamplemethod='nearest',
                                                outdtype='float32')

    # print('shapes: ')
    # print(lst_ds.shape, ndvi_ds.shape, dt_ds.shape, tmax_ds.shape, water_mask_ds.shape, snow_mask_ds.shape, ndwi_ds.shape)

    # TODO - find a way to do this programatically based on the FANO resolution
    lst_ds = lst_ds[:14500, :37200]
    ndvi_ds = ndvi_ds[:14500, :37200]
    dt_ds = dt_ds[:14500, :37200]
    tmax_ds = tmax_ds[:14500, :37200]
    water_mask_ds = water_mask_ds[:14500, :37200]
    snow_mask_ds = snow_mask_ds[:14500, :37200]
    ndwi_ds = ndwi_ds[:14500, :37200]


    # # try to avoid making large chunks (This creates an index error when i try and .interp()
    # https://docs.dask.org/en/stable/array-slicing.html
    # dask.config.set({"array.slicing.split_large_chunks": True})

    # # FIXING NODATA(s)
    # # == LST ==
    # lst_ds = rxr.open_rasterio(lst, masked=True, chunks=True).squeeze().drop(labels='band')
    lst_ds = lst_ds.where(lst_ds > 0)

    # # == dT ==
    # dt_ds = rxr.open_rasterio(dt, masked=True, chunks=True).squeeze().drop(labels='band')
    dt_ds = dt_ds.where(dt_ds >= 0)

    # # == tmax ==
    # # tmax has no meaningful nodata values
    # tmax_ds = rxr.open_rasterio(tmax, masked=True, chunks=True).squeeze().drop(labels='band')

    # # == NDVI ==
    # ndvi_ds = rxr.open_rasterio(ndvi, masked=True, chunks=True).squeeze().drop(labels='band')
    # # TODO - Find out about what is going on in the north in January where NDVI has been filled with int(-500)
    ndvi_ds = ndvi_ds.where(ndvi_ds != -500)
    ndvi_ds = ndvi_ds / 10000
    ndvi_ds = ndvi_ds.where(ndvi_ds >= -100)

    # == NDWI ==
    # ndwi_ds = rxr.open_rasterio(ndwi_layer, masked=True, chunks=True).squeeze().drop(labels='band')
    # NDWI is good to go, NaNs are encoded i think.

    # # == 3 masks ==
    # # No nodatas for the masks
    # water_mask_ds = rxr.open_rasterio(water_mask, masked=True, chunks=True).squeeze().drop(labels='band')
    # snow_mask_ds = rxr.open_rasterio(snow_mask, masked=True, chunks=True).squeeze().drop(labels='band')

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
    ndwi_water_bool = ndwi_ds > 0.12  #  # where water: True, where land: false
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
    water_sum_25 = coarsen_dask_arr(snow_free_water, scaling_value=fano_grid, resample_alg='sum')
    # # testing
    if testing:
        water_sum_25.rio.to_raster(os.path.join(temp, 'water_sum.tif'),
                                   tiled=True, lock=Lock('rio', client=client))

    # 3) reduce antiwatermask with 'sum' to 5km resolution. This is the 'good pixels count'
    land_sum_25 = coarsen_dask_arr(snow_free_land, scaling_value=fano_grid, resample_alg='sum')
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
    watermask = snow_free_land # land is 1, water is 0

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
    lst_ds25_masked = coarsen_dask_arr(lst_ds_masked, fano_grid)
    # # # testing
    if testing:
        lst_ds25_masked.rio.to_raster(os.path.join(temp, 'lst_ds25_masked.tif'),
                                tiled=True, lock=Lock('rio', client=client))
    ndvi_ds25_masked = coarsen_dask_arr(ndvi_ds_masked, fano_grid)
    # @ 100 km
    lst_ds100_masked = coarsen_dask_arr(lst_ds_masked, fano_coarse_grid)
    # # # testing
    if testing:
        lst_ds100_masked.rio.to_raster(os.path.join(temp, 'lst_ds100_masked.tif'),
                                      tiled=True, lock=Lock('rio', client=client))
    ndvi_ds100_masked = coarsen_dask_arr(ndvi_ds_masked, fano_coarse_grid)

    # lst unmasked - we use this at the very end to assign
    # regular NON FANO 25km average LST to water areas i.e. NDVI<0.
    # @ 25km only
    lst_ds25_unmasked = coarsen_dask_arr(lst_ds, fano_grid)
    # # # testing
    if testing:
        lst_ds25_unmasked.rio.to_raster(os.path.join(temp, 'lst_ds25_unmasked.tif'),
                                tiled=True, lock=Lock('rio', client=client))
    ndvi_ds25_unmasked = coarsen_dask_arr(ndvi_ds, fano_grid)


    # get dt at 25km and 100km. Don't need masking. (Mask=None by default.)
    dt_ds25 = coarsen_dask_arr(dt_ds, fano_grid)
    dt_ds100 = coarsen_dask_arr(dt_ds, fano_coarse_grid)
    # get tmax at 25km only, no masking here either
    tmax_ds25 = coarsen_dask_arr(tmax_ds, fano_grid)
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



    # smooth the very coarse 100km fano, that way it doesn't create huge artifacts...is it a good idea?!
    smoothing=False
    # by default we don't do it
    if smoothing:
        tcold_FANO_100.load()
        tcold_FANO_100_smooth = tcold_FANO_100.interp(y=dt_ds100['y'], x=dt_ds100['x'], method='linear')
        tcold_FANO_100_smooth.load()
        tcold_FANO_100_25 = tcold_FANO_100_smooth.interp(y=tcold_FANO_25['y'], x=tcold_FANO_25['x'], method='nearest')
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
        # Writing the file out is necessary for VRT method
        if not os.path.exists(os.path.join(temp, 'cfactor.tif')):
            print('writing cfactor \n', cfactor)
            cfactor.rio.to_raster(os.path.join(temp, 'cfactor.tif'),
                                  tiled=True, lock=Lock('rio', client=client))
        cfactor_path = os.path.join(temp, 'cfactor.tif')
        print('cfactor written !\n',)

        # # todo- This should work but doesn't... tcold1km comes out all warped and effed up.
        # cfactor_bilinear_ds = resample_daskVRT(cfactor_path, scaling_value=(1/fano_grid), resample_alg='bilinear', nodata_val=None)

        if not os.path.exists(os.path.join(temp, 'cfactor_bilinear.tif')):
            cfactor_bilinear_path = warp_based_on_sample(cfactor_path, temp_folder=temp, outname='cfactor_bilinear.tif',
                                                         sample_file=hardcoded_sample_file, resamplemethod='bilinear')

        cfactor_bilinear_path = os.path.join(temp, 'cfactor_bilinear.tif')
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

    # get rid of snow
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

    # get rid of values below 0 and above 1.2, for they are erroneous.
    etf_1 = xr.where(etf_raw < 0, 0, etf_raw)
    etf_final = xr.where(etf_raw > 1.2, 1.2, etf_1)


    etf_final.rio.to_raster(os.path.join(output_location, etf_name),
                      tiled=True, lock=Lock('rio', client=client))