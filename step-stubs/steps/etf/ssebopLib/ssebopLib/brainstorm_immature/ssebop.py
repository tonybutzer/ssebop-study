import os
import numpy as np
# numpy type hinting: https://stackoverflow.com/questions/52839427/numpy-type-hints-in-python-pep-484

class SSEBop():
    """This is the core SSEBop module"""

    # TODO - SSEBop will need to be initialized with a pre-made xarray dataset...
    def __init__(self, dt: str, tmax: str, ndvi: str, lst: str,
                 refet: str, cfactor: str, refet_coeff=1.00):
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

        self.dt = dt
        self.refet = refet
        self.ndvi = ndvi
        self.tmax = tmax
        self.lst = lst
        self.cfactor = cfactor
        self.refet_coeff = refet_coeff

    def calculate_etf(self):
        """
        Senay 2013 eq 2
        :return:
        """

        self.calculate_tcold()
        self.calculate_thot()

        self.etf = (self.thot - self.lst) / self.dt

    def calculate_thot(self):
        """
        Senay 2013 eq 19
        :return:
        """
        self.thot = self.tcold + self.dt

    def calculate_tcold(self):
        """
        senay 2013 eq 3
        :return:
        """
        self.tcold = self.tmax * self.cfactor

    # another class handles this?
    def calculate_eta(self, zarr_file, product):
        """
        Senay 2013 eq 1
        :return:
        """

        self.eta = (self.refet_coeff * self.refet) * self.etf

    # TODO - FANO method for the c-factor.

    # ==========================GEE FANO ===================

    def tcorr_FANO(self):
        """Compute the scene wide Tcorr for the current image adjusting tcorr
            temps based on NDVI thresholds to simulate true cold cfactor

        FANO: Forcing And Normalizing Operation

        Returns
        -------
        ee.Image of Tcorr values

        """

        # setting up geotransforms
        # geotransorm for 'fine' 5km grid
        coarse_transform = [5000, 0, 15, 0, -5000, 15]
        # geotransform for 'coarse' 100 km grid. Units in meters.
        coarse_transform100 = [100000, 0, 15, 0, -100000, 15]
        # Setting up constants for the linear transformation of Surface Temp.
        # based on the slope of dT vs NDVI: empirical constant from lansat todo - same for MODIS?!?!
        dt_coeff = 0.125
        # thresholds for getting wet soil or rice paddies out of the dT funciton
        # subtext - we assume that NDVI is the only thing that is responsible for dT moving around.
        # low ndvi hot, high ndvi cold, low ndvi cold (like wet mud) is bad.
        ndwi_threshold = -0.15
        high_ndvi_threshold = 0.9
        # we throw out ENTIRELY if there is more than 10% of water above the threshold within a
        # 5km pixel. Just cant trust it.
        # if you throw out the 5km pixel you revert to 100km.
        water_pct = 10
        # max pixels argument for .reduceResolution() Just GEE crap
        m_pixels = 65535


        # getting the rasters we need.
        lst = ee.Image(self.lst)
        # clamp ndvi
        ndvi = ee.Image(self.ndvi).clamp(-1.0, 1.0)
        tmax = ee.Image(self.tmax)
        dt = ee.Image(self.dt)
        ndwi = ee.Image(self.ndwi)
        # watermask comes from the quality band as well.
        # We combine it with NDWI to filter. MODIS will b diff.
        qa_watermask = ee.Image(self.qa_water_mask)

        # setting NDVI to negative values where Landsat QA Pixel detects water.
        ndvi = ndvi.where(qa_watermask.eq(1).And(ndvi.gt(0)), ndvi.multiply(-1))

        watermask = ndwi.lt(ndwi_threshold)
        # combining NDWI mask with QA Pixel watermask.
        watermask = watermask.multiply(qa_watermask.eq(0))
        # returns qa_watermask layer masked by combined watermask to get a count of valid pixels
        watermask_for_coarse = qa_watermask.updateMask(watermask)

        # this is filtering out 'watery' zones.
        watermask_coarse_count = watermask_for_coarse\
            .reproject(self.crs, self.transform)\
            .reduceResolution(ee.Reducer.count(), True, m_pixels)\
            .reproject(self.crs, coarse_transform)\
            .updateMask(1).select([0], ['count'])
        total_pixels_count = ndvi\
            .reproject(self.crs, self.transform)\
            .reduceResolution(ee.Reducer.count(), True, m_pixels)\
            .reproject(self.crs, coarse_transform)\
            .updateMask(1).select([0], ['count'])

        # Doing a layering mosaic check to fill any remaining Null watermask coarse pixels with valid mask data.
        #   This can happen if the reduceResolution count contained exclusively water pixels from 30 meters.
        watermask_coarse_count = ee.Image([watermask_coarse_count, total_pixels_count.multiply(0).add(1)])\
            .reduce(ee.Reducer.firstNonNull())

        percentage_bad = watermask_coarse_count.divide(total_pixels_count)
        pct_value = (1 - (water_pct / 100))
        wet_region_mask_5km = percentage_bad.lte(pct_value)

        ndvi_avg_masked = ndvi\
            .updateMask(watermask)\
            .reproject(self.crs, self.transform)\
            .reduceResolution(ee.Reducer.mean(), True, m_pixels)\
            .reproject(self.crs, coarse_transform)
        ndvi_avg_masked100 = ndvi\
            .updateMask(watermask)\
            .reproject(self.crs, self.transform)\
            .reduceResolution(ee.Reducer.mean(), True, m_pixels)\
            .reproject(self.crs, coarse_transform100)
        ndvi_avg_unmasked = ndvi\
            .reproject(self.crs, self.transform)\
            .reduceResolution(ee.Reducer.mean(), True, m_pixels)\
            .reproject(self.crs, coarse_transform)\
            .updateMask(1)
        lst_avg_masked = lst\
            .updateMask(watermask)\
            .reproject(self.crs, self.transform)\
            .reduceResolution(ee.Reducer.mean(), True, m_pixels)\
            .reproject(self.crs, coarse_transform)
        lst_avg_masked100 = lst\
            .updateMask(watermask)\
            .reproject(self.crs, self.transform)\
            .reduceResolution(ee.Reducer.mean(), True, m_pixels)\
            .reproject(self.crs, coarse_transform100)
        lst_avg_unmasked = lst\
            .reproject(self.crs, self.transform)\
            .reduceResolution(ee.Reducer.mean(), True, m_pixels)\
            .reproject(self.crs, coarse_transform)\
            .updateMask(1)

        # Here we don't need the reproject.reduce.reproject sandwich bc these are coarse data-sets
        dt_avg = dt.reproject(self.crs, coarse_transform)
        dt_avg100 = dt.reproject(self.crs, coarse_transform100).updateMask(1)
        tmax_avg = tmax.reproject(self.crs, coarse_transform)

        # FANO expression as a function of dT, calculated at the coarse resolution(s)
        Tc_warm = lst_avg_masked.expression(
            f'(lst - (dt_coeff * dt * (ndvi_threshold - ndvi) * 10))',
            {'dt_coeff': dt_coeff, 'ndvi_threshold': high_ndvi_threshold,
             'ndvi': ndvi_avg_masked, 'dt': dt_avg, 'lst': lst_avg_masked})

        Tc_warm100 = lst_avg_masked100.expression(
            '(lst - (dt_coeff * dt * (ndvi_threshold - ndvi) * 10))',
            {'dt_coeff': dt_coeff, 'ndvi_threshold': high_ndvi_threshold,
              'ndvi': ndvi_avg_masked100, 'dt': dt_avg100, 'lst': lst_avg_masked100})

        # In places where NDVI is really high, use the masked original lst at those places.
        # In places where NDVI is really low (water) use the unmasked original lst.
        # Everywhere else, use the FANO adjusted Tc_warm, ignoring masked water pixels.
        # In places where there is too much land covered by water 10% or greater,
        #   use a FANO adjusted Tc_warm from a coarser resolution (100km) that ignored masked water pixels.

        # here you're substituting FANO lst (cold everywhere) for the regular LST.
        Tc_cold = lst_avg_unmasked\
            .where((ndvi_avg_masked.gte(0).And(ndvi_avg_masked.lte(high_ndvi_threshold))), Tc_warm)\
            .where(ndvi_avg_masked.gt(high_ndvi_threshold), lst_avg_masked)\
            .where(wet_region_mask_5km, Tc_warm100)\
            .where(ndvi_avg_unmasked.lt(0), lst_avg_unmasked)

        c_factor = Tc_cold.divide(tmax_avg)

        # bilinearly smooth the gridded c factor
        c_factor_bilinear = c_factor.resample('bilinear')

        return c_factor_bilinear.rename(['tcorr'])\
            .set({'system:index': self._index,
                  'system:time_start': self._time_start,
                  'tmax_source': tmax.get('tmax_source'),
                  'tmax_version': tmax.get('tmax_version')})
