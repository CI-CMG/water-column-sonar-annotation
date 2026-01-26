import pandas as pd
import pvlib


class AstronomicalManager:
    #######################################################
    def __init__(
        self,
    ):
        self.DECIMAL_PRECISION = 6

    def get_solar_azimuth(
        self,
        # iso_time: str = "2026-01-26T18:42:00Z",
        iso_time: str = "2026-01-26T00:06:00Z",
        latitude: float = 39.9674884,  # boulder gps coordinates
        longitude: float = -105.2532602,
    ):
        """
        Good reference for calculating: https://www.suncalc.org/#/39.9812,-105.2495,13/2026.01.26/11:52/1/3
        utc time now: '2026-01-25T18:42:00Z' # 11:43am
            7:14 am↑ (sunrise)
                (Timestamp('2026-01-25 18:42:00+0000', tz='UTC'), Timestamp('2026-01-25 14:15:07.145030400+0000', tz='UTC')) # good
            5:13 pm↑ (sunset)
                (Timestamp('2026-01-25 18:42:00+0000', tz='UTC'), Timestamp('2026-01-26 00:10:51.244243200+0000', tz='UTC')) # good'ish
            solar altitude should be: 31.26°, azimuth should be: 174.01°
        :param iso_time: ISO8601 timestamp in string format
        :param latitude: Latitude in decimal value
        :param longitude: Longitude in decimal value
        :return: Solar Altitude
        """
        #
        # TODO: pvlib.location.lookup_altitude
        #
        solar_position = pvlib.solarposition.get_solarposition(
            time=pd.DatetimeIndex([iso_time]),
            latitude=latitude,
            longitude=longitude,
        )
        # 'elevation' is analogous to 'altitude'
        elevation = solar_position.elevation.iloc[0]
        # sunrise_sunset = pvlib.solarposition.sun_rise_set_transit_spa(
        #     times=pd.DatetimeIndex([iso_time]),
        #     latitude=latitude,
        #     longitude=longitude,
        # )
        # Note: sunrise & sunset can be consolidated into altitude
        return elevation


# if __name__ == "__main__":
#     astronomical_manager = AstronomicalManager()
#     azimuth = astronomical_manager.get_solar_azimuth()
#     print(azimuth)
