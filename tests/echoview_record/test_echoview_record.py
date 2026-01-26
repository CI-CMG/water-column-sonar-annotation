import pytest

from water_column_sonar_annotation.echoview_record import EchoviewRecordManager


#######################################################
def setup_module():
    print("setup")


def teardown_module():
    print("teardown")


@pytest.fixture
def process_evr_file_path(test_path):
    return test_path["DATA_TEST_PATH"]


#######################################################
@pytest.mark.skip(reason="todo implement this")
def test_process_evr_file(process_evr_file_path, tmp_path):
    echoview_record_manager = EchoviewRecordManager()
    echoview_record_manager.process_point_data()
    assert True
