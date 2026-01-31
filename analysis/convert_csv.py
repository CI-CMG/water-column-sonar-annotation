from dateutil import tz
from datetime import datetime
from timezonefinder import TimezoneFinder

import pandas as pd

"""
One off of processing the converting files for carrie
"""


def phase_day(
    solar_azimuth: float,
    iso_time: str,
    latitude: float,
    longitude: float,
) -> int:
    """
    Returns whether the time/gps references a Nautical Daylight time
    Going to need to verify the az is correctly computed
    { 'night': 4, 'dawn': 1, 'day': 2, 'dusk': 3 }
    """
    print(iso_time)
    obj = TimezoneFinder()
    calculated_timezone = obj.timezone_at(lng=longitude, lat=latitude)
    from_zone = tz.gettz("UTC")
    # print('b')
    to_zone = tz.gettz(calculated_timezone)
    utc = datetime.fromisoformat(iso_time)
    # print('c')
    utc = utc.replace(tzinfo=from_zone)
    local_time = utc.astimezone(to_zone)
    # print('d')

    if solar_azimuth < -12.0:
        return 4  # night
    if solar_azimuth >= 0.0:
        return 2  # day
    if local_time.hour < 12:
        return 1  # dawn
    return 3  # dusk


def process_data():
    df = pd.read_csv("Henry_B._Bigelow_HB1906_annotations.csv")
    print(df.columns)
    phase_of_day = []
    for i in range(df.shape[0]):
        xx = phase_day(
            solar_azimuth=df.iloc[i].get("solar_altitude"),
            iso_time=df.iloc[i].get("time_start"),
            latitude=df.iloc[i].get("latitude"),
            longitude=df.iloc[i].get("longitude"),
        )
        phase_of_day.append(xx)
        print(xx)
    print(phase_of_day)


if __name__ == "__main__":
    try:
        process_data()
    except Exception as e:
        print(e)
