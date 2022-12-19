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
import numpy as np
from ssebop_v6.ssebopLib.ssebopLib.ssebop import SSEBop as ssebop

cfactor = np.array([[0.9981, 0.9981],
                [0.9981, 0.9981],
                [0.99810, 0.9981]])
tmax = np.array([[305, 306],
                [308, 296],
                [298, 290]])
lst = np.array([[310, 308],
                [312, 298],
                [299, 290]])
ndvi = np.array([[0.4, 0.2],
                [0.1, 0.8],
                [0.7, 0.9]])
dt = np.array([[18.0, 14.0],
                [11.0, 20.0],
                [15.0, 11.0]])
refet = np.array([[8.0, 8.0],
                [8.0, 8.0],
                [8.0, 8.0]])

goodetf = np.array([[0.69002778, 0.81561429],
                     [0.58316364, 0.87188],
                     [0.89558667, 0.94990909]], dtype='float64')

def test_calculate_etf():
    # check that the input arrays multiply in expected ways
    model = ssebop(dt=dt, tmax=tmax, ndvi=ndvi, lst=lst, refet=refet, cfactor=cfactor)
    model.calculate_etf()

    # this doesn't work bc goodetf is a rounded off version of what model.etf produces in the example...
    # assert np.array_equal(model.etf, goodetf, equal_nan=False)

    # this however, is the way to do it...
    assert np.allclose(model.etf, goodetf, rtol=1e-04)

if __name__ == "__main__":
    test_calculate_etf()