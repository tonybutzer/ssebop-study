# ===============================================================================
# Copyright 2019 Gabriel Parrish
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
import os
from ssebop_v6.ssebop.raster_manager import RasterManager
from ssebop_v6.ssebop.ssebop import SSEBop


"""
 _    ___  _ _  ___  ___  ___  ___   ___  ___  ___  ___
| |  | . || \ || . \/ __>| . ||_ _| / __>/ __>| __>| . > ___  ___
| |_ |   ||   || | |\__ \|   | | |  \__ \\__ \| _> | . \/ . \| . \
|___||_|_||_\_||___/<___/|_|_| |_|  <___/<___/|___>|___/\___/|  _/
                                                             |_|

Since this code has been developed after the advent of Collection 2 Landsat data release (Col. 2)
All we need is to kick out the surface reflectance lst, ndvi and ndwi which is all that is needed by SSEBop

"""


class Landsat():

    def __init__(self, config_pathslash_):

        # do setup stuff
        print('Landsat is activated!'
              '1!!')

    def calculate_tcorr(self):
        """"""
        pass

    def calculate_gridded_cfactor(self):
        """"""

    def get_etf(self):

        model = SSEBop()

