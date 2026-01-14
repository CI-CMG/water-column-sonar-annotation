import pytest


#######################################################
def setup_module():
    print("setup")


def teardown_module():
    print("teardown")


@pytest.fixture
def process_evr_test_path(test_path):
    return test_path["DATA_TEST_PATH"]


#######################################################
def test_process_evr_files(process_evr_test_path, tmp_path):
    print(f"test path: {process_evr_test_path}")
    pass
