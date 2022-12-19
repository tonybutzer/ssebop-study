import os
import geopandas as geopd
import rasterio
from rasterio.mask import mask
from shapely.geometry import Point, Polygon, MultiPolygon, MultiPoint
from pyproj import CRS
import numpy as np
from rasterio import logging
# # https://gis.stackexchange.com/questions/184728/suppressing-rasterio-verbosity
# log = logging.getLogger()
# log.setLevel(logging.FATAL)


class SSEBop():
    """A 1 dimensional implementation of the SSEBop module"""

    # TODO - SSEBop will need to be initialized with a pre-made xarray dataset...
    def __init__(self, dt: float, tmax: float, ndvi: float, lst: float, tcold_coarse: float, refet_coeff=1.00, cfactor=None, refet=None):
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
        :param tcold_coarse: COLD lst temperature - can be FANO or otherwise determined
        :param kwargs: dict, optional
            scalarmodel: {True, False}
            y : {}
            z : {}
        """



        self.dt = dt
        self.ndvi = ndvi
        self.tmax = tmax
        self.lst = lst
        self.refet_coeff = refet_coeff

        # we want either a cfactor to be able to get put in, or for a tcold_coarse
        # to be put in. If both are entered, the cfactor overrides the tcold_coarse
        if tcold_coarse is None and cfactor is None:
            print('if you set tcold_coarse to none, you have to specify a cfactor')
            raise Exception
        if tcold_coarse is not None and cfactor is not None:
            print("WARNING: the cfactor you entered will override the tcold_coarse for purposes of ETf")
        self.tcold_coarse = tcold_coarse
        if cfactor is not None:
            self.cfactor = cfactor
        else:
            self.cfactor = self.calculate_cfactor()

        # SSEBop mainly calculates ETf
        self.etf = self.calculate_etf()

        # if refet is provided calculate ETr and ETa
        if refet is not None:
            # adjust the ETr by the coefficient, if other than 1.0
            self.etr = refet * refet_coeff
            # calculate ETa
            self.eta = self.etf * self.etr

    def calculate_cfactor(self):
        """"""

        cfactor = self.tcold_coarse/self.tmax
        return cfactor

    def calculate_etf(self):
        """
        Senay 2018
        :return:
        """

        tcold = self.calculate_tcold()
        # # OLD formulation
        # etf = (t_hot - lst_ds) / dt_ds
        # NEW formulation from recent publication SENAY 2018
        etf = 1 - ((self.lst - tcold) / self.dt)
        return etf

    def calculate_tcold(self):
        """
        senay 2013 eq 3
        :return:
        """
        tcold = self.tmax * self.cfactor
        return tcold

    def calculate_eta(self, zarr_file, product):
        """
        Senay 2013 eq 1
        :return:
        """

        self.eta = (self.refet_coeff * self.refet) * self.etf


class FANO_lst():

    fano_constant = 1.25
    high_ndvi_threshold = 0.9
    lst = None
    dt = None
    ndvi = None

    def __init__(self, fano_constant: float, high_ndvi_threshold: float,
                 lst: float, dt: float, ndvi: float):
        self.fano_constant = fano_constant
        self.high_ndvi_threshold = high_ndvi_threshold
        self.lst = lst
        self.dt = dt
        self.ndvi = ndvi

        self.fano_lst = self._calc_fano()

    def _calc_fano(self):

        fano_lst = (self.lst- (self.fano_constant * self.dt *
                                            (self.high_ndvi_threshold - self.ndvi)))
        return fano_lst


class PointSample():

    """A little class that can pick a location to sample based on a coordinate
      and a raster with the same coordinate system.... What makes it cool is that It will be able to sample a radius
      of a certain size around the point"""

    point = None
    geo_df = None
    raster = None

    # TODO - Sampling class that can get average lst and Tmax for 100km/25km/10km, etc radius around a shapefile point.
    # todo - need a way to specify what zonal stat the zonal_sampler will do. Avg? Sum?
    # Watch out for NDVI and LST mis-registration...
    # keep the sampled temps to make histograms...

    def __init__(self, x_coords: float, y_coords: float, epsg=str, raster=str):
        self.point = Point(x_coords, y_coords)
        self.geo_df = geopd.GeoDataFrame(index=[0], crs=CRS.from_epsg(epsg), geometry=[self.point])
        self.raster = raster
        # # you can also make geodataframes like so:
        # # https://geopandas.org/en/stable/gallery/create_geopandas_from_pandas.html#From-WKT-format
        # TODO = if the epsg code of the raster and the point don't match... then we need to change something...

    def sample_zone(self, square=True, size=0.225, scaling=None, testing=True):
        """
        You can sample an area around the point of a specified size

        # ex: https://gis.stackexchange.com/questions/367496/plot-a-circle-with-a-given-radius-around-points-on-map-using-python
        # ex2: https://geopandas.org/en/stable/getting_started/introduction.html#Buffer
        # how far across is 25km?
        # https://www.usna.edu/Users/oceano/pguth/md_help/html/approx_equivalents.htm
        """

        # resolution of 1 makes it square default is 16 which makes it circular
        # https://shapely.readthedocs.io/en/latest/manual.html#object.buffer
        if square:
            self.geo_df['buffered'] = self.geo_df.buffer(size, resolution=1)
        else:
            self.geo_df['buffered'] = self.geo_df.buffer(size, resolution=16)
        # ...now we have a polygon in a column labeled 'buffered'.

        # try sampling the raster...
        with rasterio.open(self.raster) as src:
            out_img, out_transform = mask(src, self.geo_df.buffered, all_touched=True, crop=True)

            # convert to float avoids --> ValueError: cannot convert float NaN to integer
            out_img_float = out_img.astype('float32')
            # filter out invalid values
            out_img_float[out_img_float < 0] = np.nan
            # scale if applicable
            if scaling is not None:
                # np.multiply(out_img, scaling, out=out_img, casting='unsafe')
                out_img_float *= 0.0001
            zonal_mean = np.nanmean(out_img_float)
            return zonal_mean

    def sample_point(self, scaling=None):
        """For sampling only the pixel the coordinates touch."""

        # try sampling the raster...
        with rasterio.open(self.raster) as src:
            # This way doesn't work see:
            # https://gis.stackexchange.com/questions/345428/reading-raster-values-in-points-gives-back-a-generator-object-instead-of-actual
            # point_sample = src.sample([(self.point.x, self.point.y)])
            # return point_sample
            # instead ->
            values = [sample[0] for sample in src.sample([(self.point.x, self.point.y)])]
            value = float(values[0])
            if scaling is not None:
                value *= scaling
            return value

