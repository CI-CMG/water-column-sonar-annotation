import numpy as np
import pytest

from water_column_sonar_annotation.geospatial import GeospatialManager


#######################################################
def setup_module():
    print("setup")


def teardown_module():
    print("teardown")


@pytest.fixture
def process_check_distance_from_coastline(test_path):
    return test_path["DATA_TEST_PATH"]


#######################################################
def test_check_distance_from_coastline(process_check_distance_from_coastline, tmp_path):
    geospatial_manager = GeospatialManager()
    distance = geospatial_manager.check_distance_from_coastline(
        shapefile_path=process_check_distance_from_coastline
    )
    assert np.isclose(distance, 1236212.37356)


def test_get_local_time():
    geospatial_manager = GeospatialManager()
    local_time = geospatial_manager.get_local_time(
        iso_time="2026-01-26T20:35:00Z",
        latitude=51.508742,
        longitude=-30.410156,
    )
    assert local_time == "2026-01-26T18:35:00-02:00"


def test_get_local_hour_of_day():
    geospatial_manager = GeospatialManager()
    local_hour_of_day = geospatial_manager.get_local_hour_of_day(
        iso_time="2026-01-26T20:35:00Z",
        latitude=51.508742,
        longitude=-30.410156,
    )
    assert local_hour_of_day == 18


def test_get_month_of_year():
    geospatial_manager = GeospatialManager()
    local_hour_of_day = geospatial_manager.get_month_of_year(
        iso_time="2026-01-26T20:35:00Z",
        latitude=51.508742,
        longitude=-30.410156,
    )
    assert local_hour_of_day == 1
