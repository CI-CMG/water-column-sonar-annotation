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
