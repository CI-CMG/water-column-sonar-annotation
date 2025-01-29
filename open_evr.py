import urllib.request
import shutil
import xarray as xr
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import echoregions as er


# https://colab.research.google.com/drive/1-I56QOIftj9sewlbyzTzncdRt54Fh51d?usp=sharing#scrollTo=mM49CCneMgBx
def open_evr_file():
    bucket_name = 'noaa-wcsd-zarr-pds'
    ship_name = "Henry_B._Bigelow"
    cruise_name = "HB0707"
    sensor_name = "EK60"
    # https://echoregions.readthedocs.io/en/latest/Regions2D_functionality.html

    # TEST_DATA_PATH = 'https://raw.githubusercontent.com/OSOceanAcoustics/echoregions/contains_transect_zip/echoregions/test_data'
    TEST_DATA_PATH = 'https://drive.google.com/file/d/1CYrC0y3CYg5jTj0MXICuXtOddb6Aq-bn/view?usp=sharing'
    urllib.request.urlretrieve(f"{TEST_DATA_PATH}/transect.evr", "transect.evr")
    regions2d = er.read_evr('transect.evr')
    regions2d_df = regions2d.data
    regions2d_df.head(3)


"""
BT
20081013 0945237040 0.097133 20081013 0945237040 499.942520 20081013 1005300480 499.942520 20081013 1005300480 0.097133 1 
251_175_000
"""

if __name__ == '__main__':
    open_evr_file()
