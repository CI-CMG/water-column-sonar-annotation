import pytest

from water_column_sonar_annotation.cruise import CruiseManager


#######################################################
def setup_module():
    print("setup")


def teardown_module():
    print("teardown")


@pytest.fixture
def process_cruise_path(test_path):
    return test_path["DATA_TEST_PATH"]


#######################################################
def test_get_cruise(process_cruise_path, tmp_path):
    cruise_manager = CruiseManager()
    cruise = cruise_manager.get_cruise()
    assert len(cruise.Sv.shape) == 3
