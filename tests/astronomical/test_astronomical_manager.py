import numpy as np

from water_column_sonar_annotation.astronomical import AstronomicalManager


#######################################################
def setup_module():
    print("setup")


def teardown_module():
    print("teardown")


def test_get_solar_azimuth():
    astronomical_manager = AstronomicalManager()
    # https://www.suncalc.org/#/39.9812,-105.2495,13/2026.01.26/11:52/1/3
    azimuth_noon = astronomical_manager.get_solar_azimuth(
        iso_time="2026-01-26T19:00:00Z",  # noon
        latitude=39.9674884,  # Boulder
        longitude=-105.2532602,
    )
    assert np.isclose(azimuth_noon, 31.3815)

    azimuth_sunset = astronomical_manager.get_solar_azimuth(
        iso_time="2026-01-26T00:00:00Z",  # sunset
        latitude=39.9674884,
        longitude=-105.2532602,
    )
    assert np.isclose(azimuth_sunset, 1.2527)


def test_is_daylight_at_noon():
    astronomical_manager = AstronomicalManager()
    is_daylight = astronomical_manager.is_daylight(
        iso_time="2026-01-27T19:00:00Z",  # noon
        latitude=39.9674884,  # Boulder
        longitude=-105.2532602,
    )
    assert is_daylight


def test_is_daylight_at_midnight():
    astronomical_manager = AstronomicalManager()
    is_daylight = astronomical_manager.is_daylight(
        iso_time="2026-01-28T07:00:00Z",  # noon
        latitude=39.9674884,  # Boulder
        longitude=-105.2532602,
    )
    assert not is_daylight


def test_is_daylight_at_sunset_before_and_after():
    astronomical_manager = AstronomicalManager()
    is_daylight = astronomical_manager.is_daylight(
        # sunset is at 5:13pm on jan 27th, per https://psl.noaa.gov/boulder/boulder.sunset.html
        iso_time="2026-01-28T00:13:00Z",  # sunset @ 5:13pm, nautical sunset @6:16pm
        latitude=39.9674884,  # Boulder
        longitude=-105.2532602,
    )
    assert is_daylight

    astronomical_manager = AstronomicalManager()
    is_daylight_before_nautical_sunset = astronomical_manager.is_daylight(
        iso_time="2026-01-28T01:16:00Z",  # sunset @5:13pm, nautical sunset @6:16pm
        latitude=39.9674884,  # Boulder
        longitude=-105.2532602,
    )
    assert is_daylight_before_nautical_sunset

    is_daylight_after_nautical_sunset = astronomical_manager.is_daylight(
        iso_time="2026-01-28T01:17:00Z",  # sunset @5:13pm, nautical sunset @6:16pm
        latitude=39.9674884,  # Boulder
        longitude=-105.2532602,
    )
    assert not is_daylight_after_nautical_sunset


def test_is_daylight_at_sunrise_before_and_after():
    astronomical_manager = AstronomicalManager()
    is_daylight_at_sunrise = astronomical_manager.is_daylight(
        iso_time="2026-01-27T14:13:00Z",  # sunrise @7:13am, nautical sunrise @6:12am
        latitude=39.9674884,  # Boulder
        longitude=-105.2532602,
    )
    assert is_daylight_at_sunrise

    is_daylight_before_nautical_sunrise = astronomical_manager.is_daylight(
        iso_time="2026-01-27T13:12:00Z",  # sunrise @7:13am, nautical sunrise @6:12am
        latitude=39.9674884,  # Boulder
        longitude=-105.2532602,
    )
    assert not is_daylight_before_nautical_sunrise

    is_daylight_after_nautical_sunrise = astronomical_manager.is_daylight(
        iso_time="2026-01-27T13:13:00Z",  # sunrise @7:13am, nautical sunrise @6:12am
        latitude=39.9674884,  # Boulder
        longitude=-105.2532602,
    )
    assert is_daylight_after_nautical_sunrise
