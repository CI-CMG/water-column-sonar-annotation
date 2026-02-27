import numpy as np

from water_column_sonar_annotation.cruise import CruiseManager


#######################################################
def setup_module():
    print("setup")


def teardown_module():
    print("teardown")


# @pytest.fixture
# def process_cruise_path(test_path):
#     return test_path["DATA_TEST_PATH"]


#######################################################
# def test_get_cruise_bottom_nan(process_cruise_path, tmp_path):
#     cruise_manager = CruiseManager()
#     cruise = cruise_manager.get_cruise()
#     # count of non-nan values np.count_nonzero(~np.isnan(cruise.bottom.values)) / cruise.Sv.shape[1]
#     assert len(cruise.Sv.shape) == 3


def test_get_cruise():
    cruise_manager = CruiseManager()
    cruise = cruise_manager.get_cruise()
    assert len(cruise.Sv.shape) == 3


def test_get_coordinates():
    """This only gets the depth over the interval, need to calculate the 'altitude'"""
    cruise_manager = CruiseManager()
    lat, lon = cruise_manager.get_coordinates(
        start_time="2019-10-16T16:20:00",
        end_time="2019-10-16T16:30:00",
    )
    assert np.isclose(lat, 41.48177)
    assert np.isclose(lon, -68.50478)


def test_get_depth():
    """This only gets the depth over the interval, need to calculate the 'altitude'"""
    cruise_manager = CruiseManager()
    depth_value = cruise_manager.get_depth(
        start_time="2019-10-16T16:20:00",
        end_time="2019-10-16T16:50:00",
    )
    assert np.isclose(depth_value, 96.36)  # 96.356674


def test_get_depth_index():
    # Converts depth float value to nearest index
    cruise_manager = CruiseManager()
    depth_index_value = cruise_manager.get_depth_index(23.4)
    assert depth_index_value == 117


def test_get_time_index():
    # Converts timestamp to nearest index from cruise
    cruise_manager = CruiseManager()
    time_in_nanoseconds = np.datetime64("2019-09-25 22:47:24.213000").astype(int)
    depth_index_value = cruise_manager.get_time_index(time_in_nanoseconds)
    assert depth_index_value == 1282150
    time_in_nanoseconds2 = np.datetime64("2019-09-25 22:47:34.000000").astype(int)
    depth_index_value2 = cruise_manager.get_time_index(time_in_nanoseconds2)
    assert depth_index_value2 == 1282160


def test_get_altitude():
    """This gets the distance from EVR to the bottom"""
    cruise_manager = CruiseManager()
    altitude_value = cruise_manager.get_altitude(
        start_time="2019-10-16T16:20:00",
        end_time="2019-10-16T16:50:00",
        bbox_max=80.0,
    )
    # bottom is at 96.356674
    # setting the bbox at 80.
    assert np.isclose(altitude_value, 16.36)  # 96.356674


def test_get_altitude_nan_bottom():
    """This gets the distance from EVR to the bottom when there are nan values"""
    cruise_manager = CruiseManager()
    altitude_value = cruise_manager.get_altitude(
        start_time="2019-09-26T10:02:39.03",
        end_time="2019-09-26T10:02:40.00",
        bbox_max=10.0,
    )
    assert np.isclose(altitude_value, 0.0)


# get_gps
