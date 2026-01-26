import numpy as np

from water_column_sonar_annotation.astronomical import AstronomicalManager


#######################################################
def setup_module():
    print("setup")


def teardown_module():
    print("teardown")


def test_get_solar_azimuth():
    astronomical_manager = AstronomicalManager()
    azimuth_noon = astronomical_manager.get_solar_azimuth(
        iso_time="2026-01-26T19:00:00Z",  # noon
        latitude=39.9674884,
        longitude=-105.2532602,
    )
    assert np.isclose(azimuth_noon, 31.3815)

    azimuth_sunset = astronomical_manager.get_solar_azimuth(
        iso_time="2026-01-26T00:00:00Z",  # sunset
        latitude=39.9674884,
        longitude=-105.2532602,
    )
    assert np.isclose(azimuth_sunset, 1.2527)
