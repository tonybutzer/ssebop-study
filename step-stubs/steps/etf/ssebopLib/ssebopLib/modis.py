import os
from ssebop_v6.ssebop.raster_manager import RasterManager
from ssebop_v6.ssebop.ssebop import SSEBop
from ssebop_v6.ssebop.input_manager import InDataManager

"""
 __ __  ___  ___  _  ___   ___  ___  ___  ___           
|  \  \| . || . \| |/ __> / __>/ __>| __>| . > ___  ___ 
|     || | || | || |\__ \ \__ \\__ \| _> | . \/ . \| . \
|_|_|_|`___'|___/|_|<___/ <___/<___/|___>|___/\___/|  _/
                                                   |_|  
                                                   """
"""This class handles all things MODIS related to SSEBop, model prep"""

# TODO - this class will take pre-prepared GLOBAL MODIS images (AND associated Met Data) and make a ZARR file
#(SOMEDAY): Have a mode to be able to use: https://registry.opendata.aws/modis-astraea/


class Modis():

    image_is_aqua = False
    resolution = False
    time_period = None

    _origin = (0,0)

    def __init__(self, config):
        # do setup stuff
        print('modis is activated!'
              '1!!')

    def calculate_tcorr(self):
        """"""
        pass

    def calculate_gridded_cfactor(self):
        """"""

    def get_etf(self):

        model = SSEBop(configthingythatdoesntexistyet)

    def get_eta(self):

        pass

    def preprocess_timeseries(self, outlocation):
        """This function creates a nice Zarr file from disorganized inputs."""
        pass


    # example of properties that can protect attributes that are internal to the class.
    # Setters are like sepparate methods that keep users from inputing bad attributes to the function -LEON FOKS
    @property
    def origin(self):
        return self._origin
    @origin.setter
    def origin(self, centerpoint:tuple):
        assert isinstance(centerpoint, tuple), TypeError('the centerpoint should be a tuple')
        self._origin = centerpoint
