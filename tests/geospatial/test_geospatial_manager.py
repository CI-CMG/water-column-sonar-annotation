from water_column_sonar_annotation.geospatial import GeospatialManager


#######################################################
def setup_module():
    print("setup")


def teardown_module():
    print("teardown")


def test_check_distance():
    geospatial_manager = GeospatialManager()
    distance = geospatial_manager.check_distance()
    assert distance > 0.0
